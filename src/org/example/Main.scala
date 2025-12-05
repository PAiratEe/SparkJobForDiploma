package org.example

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write

object Main {

  private val spark = SparkSession.builder()
    .appName("S3 to ClickHouse Loader")
    .master("k8s://https://192.168.49.2:8443")
    .config("spark.executor.memory", "2g")
    .config("spark.driver.memory", "2g")
    .config("spark.dynamicAllocation.enabled", "false")
    .config("spark.executor.instances", "2")
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    val env = sys.env
    println("ENV VARS: " + env.mkString(", "))

    val year = env.getOrElse("YEAR", throw new NoSuchElementException("YEAR env not found"))
    val month = env.getOrElse("MONTH", throw new NoSuchElementException("MONTH env not found")).reverse.padTo(2, '0').reverse
    val day = env.getOrElse("DAY", throw new NoSuchElementException("DAY env not found")).reverse.padTo(2, '0').reverse

    println(s"📅 Running Spark job for date: $year-$month-$day")

    val s3Bucket = "s3a://airbyte-bucket/api/raw"
    val clickhouseUrl = "jdbc:clickhouse://clickhouse.default.svc.cluster.local:8123"
    val clickhouseUser = "default"
    val clickhousePassword = "dCkUgJH3JI"
    val clickhouseTable = env.getOrElse("TABLE", throw new NoSuchElementException("TABLE env not found"))

    val path = env.getOrElse("DATA_PATH", throw new NoSuchElementException("PATH env not found"))
    val input = s"$s3Bucket/$path/$year/$month/$day/*.json"

    val jsonDF = spark.read.json(input)

    println("✅ Исходная схема:")
    jsonDF.printSchema()

    val df = if (jsonDF.columns.contains("_airbyte_data"))
      jsonDF.selectExpr("_airbyte_data.*")
    else
      jsonDF

    val normalized = normalizeForJdbc(df)

    println("✅ Расплющенная схема:")
    normalized.printSchema()

    val validatedDF = validateWithGE(normalized, clickhouseTable)

    val processedPath = s"s3a://airbyte-bucket/api/processed/$path/$year/$month/$day/"

    validatedDF.write
      .mode("append")
      .parquet(processedPath)

    validatedDF.write
      .mode("append")
      .format("jdbc")
      .option("url", clickhouseUrl)
      .option("dbtable", clickhouseTable)
      .option("user", clickhouseUser)
      .option("password", clickhousePassword)
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .save()

    println(s"✅ Data successfully written to ClickHouse table [$clickhouseTable] and S3 [$processedPath]")

    spark.stop()
  }

  private def validateWithGE(df: DataFrame, table: String): DataFrame = {

    import spark.implicits._
    implicit val formats: Formats = DefaultFormats

    val geRestUrl = sys.env.getOrElse("GE_REST_URL", "http://ge-server.default.svc.cluster.local:5000/validate")

    val rows = df.toJSON.collect().toSeq
    val parsedRows = rows.map(parse(_).extract[Map[String, Any]])

    val client = HttpClients.createDefault()
    val post = new HttpPost(geRestUrl)
    post.setHeader("Content-type", "application/json")

    val requestBody = write(Map(
      "suite_name" -> s"${table}_suite",
      "data" -> parsedRows
    ))

    post.setEntity(new StringEntity(requestBody, ContentType.APPLICATION_JSON))

    val response = client.execute(post)
    val responseString = EntityUtils.toString(response.getEntity)
    response.close()
    client.close()

    val jsonResp = parse(responseString).extract[Map[String, Any]]
    val validMask: Seq[Boolean] = jsonResp.get("result") match {
      case Some(list: List[Boolean]) => list
      case _ =>
        println(s"⚠️ Can't parse GE response: $responseString")
        Seq.fill(rows.size)(false)
    }

    val indexedDF = df.withColumn("idx", monotonically_increasing_id())

    val maskDF = validMask.zipWithIndex.toSeq.toDF("ge_valid", "idx")
      .select($"idx".cast("long"), $"ge_valid".cast("boolean"))

    val validRows = indexedDF
      .join(maskDF, Seq("idx"))
      .filter($"ge_valid" === true)
      .drop("ge_valid", "idx")

    println(s"✅ GE validation finished: ${validMask.count(_ == true)} passed, ${validMask.count(_ == false)} failed")

    validRows
  }

  private def normalizeForJdbc(df: DataFrame, prefix: String = ""): DataFrame = {
    var currentDF = df

    df.schema.fields.foreach { field =>
      val colName = if (prefix.nonEmpty) s"${prefix}__${field.name}" else field.name

      field.dataType match {

        case structType: StructType =>
          structType.fields.foreach { subField =>
            currentDF = currentDF.withColumn(s"${colName}__${subField.name}", col(s"${field.name}.${subField.name}"))
          }
          currentDF = currentDF.drop(field.name)

        case ArrayType(structType: StructType, _) =>
          currentDF = currentDF.withColumn(field.name, explode_outer(col(field.name)))
          structType.fields.foreach { subField =>
            currentDF = currentDF.withColumn(s"${colName}__${subField.name}", col(s"${field.name}.${subField.name}"))
          }
          currentDF = currentDF.drop(field.name)

        case _: org.apache.spark.sql.types.BooleanType =>
          currentDF = currentDF.withColumn(colName, col(field.name).cast("int"))

        case _: org.apache.spark.sql.types.LongType =>
          currentDF = currentDF.withColumn(colName, col(field.name).cast("long"))

        case _: org.apache.spark.sql.types.IntegerType =>
          currentDF = currentDF.withColumn(colName, col(field.name).cast("long"))

        case _: org.apache.spark.sql.types.DoubleType =>
          currentDF = currentDF.withColumn(colName, col(field.name).cast("double"))

        case _: org.apache.spark.sql.types.FloatType =>
          currentDF = currentDF.withColumn(colName, col(field.name).cast("double"))

        case _: ArrayType | _: MapType =>
          currentDF = currentDF.withColumn(colName, to_json(col(field.name)))

        case _: StringType =>
          val tryTs = to_timestamp(col(field.name),
            "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS][XXX][X]"
          )

          currentDF = currentDF.withColumn(
            colName,
            when(tryTs.isNotNull, tryTs)
              .otherwise(col(field.name))
          )

        case _ =>
      }
    }

    currentDF
  }

}

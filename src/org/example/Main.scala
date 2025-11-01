package org.example

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, MapType, StructType}
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

    val year = env.getOrElse("YEAR", throw new NoSuchElementException("YEAR env not found")).toInt
    val month = env.getOrElse("MONTH", throw new NoSuchElementException("MONTH env not found")).toInt
    val day = env.getOrElse("DAY", throw new NoSuchElementException("DAY env not found")).toInt

    println(s"📅 Running Spark job for date: $year-$month-$day")

    val s3Bucket = "s3a://airbyte-bucket/api/raw"
    val clickhouseUrl = "jdbc:clickhouse://clickhouse.default.svc.cluster.local:8123"
    val clickhouseUser = "default"
    val clickhousePassword = "dCkUgJH3JI"
    val clickhouseTable = env.getOrElse("TABLE", throw new NoSuchElementException("TABLE env not found"))

    val path = env.getOrElse("PATH", throw new NoSuchElementException("PATH env not found"))
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

    //val validatedDF = validateWithGE(normalized)

    val processedPath = s"s3a://airbyte-bucket/api/processed/$path/$year/$month/$day/"

    normalized.printSchema()

    normalized.write
      .mode("append")
      .parquet(processedPath)

    normalized.write
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

  private def validateWithGE(df: DataFrame): DataFrame = {

    import spark.implicits._
    implicit val formats: Formats = DefaultFormats

    val geRestUrl = sys.env.getOrElse("GE_REST_URL", "http://great-expectations:5000/validate")

    val rows = df.toJSON.collect().toSeq
    val parsedRows = rows.map(parse(_).extract[Map[String, Any]])

    val client = HttpClients.createDefault()
    val post = new HttpPost(geRestUrl)
    post.setHeader("Content-type", "application/json")

    val requestBody = write(Map(
      "suite_name" -> "spark_clickhouse_suite",
      "data" -> parsedRows
    ))

    post.setEntity(new StringEntity(requestBody, "UTF-8"))

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

    val validated = df.withColumn("ge_valid", typedLit(validMask))
    val validRows = validated.filter($"ge_valid" === true).drop("ge_valid")
    val invalidCount = validMask.count(!_)

    println(s"✅ GE validation finished: ${validRows.count()} passed, $invalidCount failed")

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

        case _: ArrayType | _: MapType =>
          currentDF = currentDF.withColumn(colName, to_json(col(field.name))).drop(field.name)

        case _ =>
      }
    }

    currentDF
  }

}

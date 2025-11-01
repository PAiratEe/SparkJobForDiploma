package org.example

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, MapType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s._
import org.json4s.jackson.JsonMethods._

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

    val year = env.getOrElse("YEAR", throw new NoSuchElementException("YEAR env not found")).toInt
    val month = env.getOrElse("MONTH", throw new NoSuchElementException("MONTH env not found")).toInt
    val day = env.getOrElse("DAY", throw new NoSuchElementException("DAY env not found")).toInt

    println(s"📅 Running Spark job for date: $year-$month-$day")

    val s3Bucket = "s3a://airbyte-bucket/api/raw"
    val clickhouseUrl = "jdbc:clickhouse://clickhouse.default.svc.cluster.local:8123"
    val clickhouseUser = "default"
    val clickhousePassword = "dCkUgJH3JI"
    val clickhouseTable = "wikipedia_pageviews_top"

    val input = s"$s3Bucket/wikipedia_pageviews/top/$year/$month/$day/*.json"

    val jsonDF = spark.read.json(input)

    println("✅ Исходная схема:")
    jsonDF.printSchema()

    val df = if (jsonDF.columns.contains("_airbyte_data"))
      jsonDF.selectExpr("_airbyte_data.*")
    else
      jsonDF

    val normalized = normalizeForJdbc(df)

    println("✅ Расплющенная схема:")

    val processedPath = s"s3a://airbyte-bucket/api/processed/wikipedia_pageviews/top/$year/$month/$day/"

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

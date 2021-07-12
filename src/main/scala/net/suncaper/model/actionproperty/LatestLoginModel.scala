package net.suncaper.model.actionproperty


import org.apache.spark.sql.functions.{count, row_number, _}
import org.apache.spark.sql.types.{DataType, DateType, LongType, TimestampType}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}

object LatestLoginModel {
  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_users"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"lastLoginTime":{"cf":"cf", "col":"lastLoginTime", "type":"string"}
         |}
         |}""".stripMargin

    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()

    import spark.implicits._

    val source: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val fireTsDF = source
      //      .withColumn("year",col("lastLoginTime")/60/60/24/365+1970)
      .withColumn("day",col("lastLoginTime")/60/60/24-16060)
      .drop("lastLoginTime")

    val result = fireTsDF.select('id,
      when(col("day")>="662" && col("day")<"668", "7天内")
        .when(col("day")>="668" && col("day")<"669", "1天内")
        .when(col("day")>="655" && col("day")<"662", "14天内")
        .when(col("day")>="639" && col("day")<"655", "30天内")
        .otherwise("超过30天未登录")
        .as("LatestLogin")
    )


    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user_profile"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"LatestLogin":{"cf":"cf", "col":"LatestLogin", "type":"string"}
         |}
         |}""".stripMargin

    result.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

}
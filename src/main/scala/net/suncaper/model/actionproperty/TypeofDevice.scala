package net.suncaper.model.actionproperty

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.SparkSession


object TypeofDevice {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HBaseTest")
      .master("local") //设定spark 程序运行模式。local在本地运行
      .getOrCreate()

    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_logs"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"global_user_id":{"cf":"cf", "col":"global_user_id", "type":"string"},
         |"user_agent":{"cf":"cf", "col":"user_agent", "type":"string"}
         |}
         |}""".stripMargin

    import spark.implicits._

    val readDF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()


    //     转换
    val result = readDF.select('global_user_id,
      when('user_agent like "%Windows%", "Windows")
        .when('user_agent like "%Mac OS%", "Mac")
        .when('user_agent like "%Android%", "Android")
        .when('user_agent like "%CPU iPhone OS%", "IOS")
        .otherwise("Linux")
        .as("device")
    )

      .withColumn("id",col("global_user_id").cast(LongType))
      .drop("user_agent","global_user_id")

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user_profile"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"device":{"cf":"cf", "col":"device", "type":"string"}
         |}
         |}""".stripMargin

    result.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()


    spark.stop()
  }
}


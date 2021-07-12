package net.suncaper.model.marketproperty

import java.util.Date

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, row_number}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object PurchaseFrequencyModel {

  /*val time = (new Date().getTime/1000/60/60/24-182)*1000*60*60*24

  spark.read
    .option(HBaseTableCatalog.tableCatalog, catalog)
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .load()
    .select('id,
      when('payTime.cast(LongType)>time,"x")
        .otherwise("y")
        as("x"))
    .where('x === "x")
    //      .withColumn("date", 'payTime)
    .show(false)*/

  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_orders"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |    "memberId":{"cf":"cf", "col":"memberId", "type":"string"},
         |    "payTime":{"cf":"cf", "col":"paymentCode", "type":"string"}
         |  }
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
      .withColumn("test",substring('memberId,-3,3).cast(LongType))

    source.show(20, false)

    val temp = source.groupBy('test)
      .count()
      .sort(asc("count"))
      .withColumn("id", 'test).drop( "memberId","test","payTime")

//    temp.show(1000, false)

    val result = temp.select('id,
      when('count >= "400","高")
        .when('count >= "200" && 'count < "400","中")
        .when('count >= "0" && 'count < "200","低")
        .otherwise("未知")
      as("purchaseFrequency")
    )

    result.sort(asc("id")).show(1000, false)

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user_profile"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"purchaseFrequency":{"cf":"cf", "col":"purchaseFrequency", "type":"string"}
         |}
         |}""".stripMargin

    result.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    spark.stop()
  }

}

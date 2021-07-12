package net.suncaper.model.actionproperty

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

import org.apache.spark.sql.expressions.Window

object LoginFrequency {
  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_logs"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"global_user_id":{"cf":"cf", "col":"global_user_id", "type":"string"},
         |"loc_url":{"cf":"cf", "col":"loc_url", "type":"string"}
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

    val result = source
      .withColumn("id",col("global_user_id").cast(LongType)).drop("global_user_id")
      .filter('loc_url like "%login%"  )
      .groupBy("id")
      .agg(count('id) as "count")
      //      .show(1000,false)
      .agg(sum('count)/950 as 'average)

    val s= result.select("average")
      .collect()
      .map(_(0))
      .toList
      .toArray

    println(s(0))

    val result2=source
      .withColumn("id",col("global_user_id").cast(LongType)).drop("global_user_id")
      .filter('loc_url like "%login%"  ).groupBy("id")
      .agg(count('id) as "count")

    val result3 = result2.select('id,
      when(col("count")===0 , "无")
        .when(col("count")*1.8<s(0) && col("count") >0 , "较少")
        .when(col("count")*1.8>=s(0) && col("count")/1.2<s(0), "一般")
        .when(col("count")/1.2>=s(0) , "经常")
        .as("LoginFrequency")
    )

    result3.show(1000,false)

        def catalogWrite =
          s"""{
             |"table":{"namespace":"default", "name":"user_profile"},
             |"rowkey":"id",
             |"columns":{
             |"id":{"cf":"rowkey", "col":"id", "type":"long"},
             |"LoginFrequency":{"cf":"cf", "col":"LoginFrequency", "type":"string"}
             |}
             |}""".stripMargin

        result3.write
          .option(HBaseTableCatalog.tableCatalog, catalogWrite)
          .option(HBaseTableCatalog.newTable, "5")
          .format("org.apache.spark.sql.execution.datasources.hbase")
          .save()
  }

}

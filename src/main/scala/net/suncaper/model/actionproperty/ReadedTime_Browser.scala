package net.suncaper.model.actionproperty


import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, desc, row_number, to_timestamp, when, year}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

//浏览时段
object ReadedTime_Browser {
  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_logs"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"global_user_id":{"cf":"cf", "col":"global_user_id", "type":"string"},
         |"log_time":{"cf":"cf", "col":"log_time", "type":"string"}
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
      .withColumn("hour",substring('log_time,12,2))



    val result = source.select('global_user_id,
      when('hour>="01" && 'hour<="07", "1点-7点")
        .when('hour>="08" && 'hour<="12", "8点-12点")
        .when('hour>="13" && 'hour<="17", "13点-17点")
        .when('hour>="18" && 'hour<="21", "18点-21点")
        .when('hour>="22" && 'hour<="23", "22点-24点")
        .otherwise("22点-24点")
        .as("ReadedTime")
    )

      .groupBy('global_user_id,'ReadedTime)
      .agg(count('ReadedTime) as "count")
      .withColumn("row_num", row_number() over Window.partitionBy('global_user_id).orderBy('count.desc))
      .where('row_num === 1)
      .withColumn("id",col("global_user_id").cast(LongType))
      .withColumnRenamed("ReadedTime", "ReadedTime_Browser")
      .drop("count", "row_num","ReadedTime","log_time","global_user_id")

    result.show(1000, false)

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user_profile"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"ReadedTime_Browser":{"cf":"cf", "col":"ReadedTime_Browser", "type":"string"}
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


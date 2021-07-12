package net.suncaper.model.actionproperty

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count, row_number, substring, when}
import org.apache.spark.sql.types.LongType
import java.sql.Timestamp
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.functions._
//浏览时长
object WatchTime_Browser {
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


    val timestamp_diff = udf((startTime: Timestamp, endTime: Timestamp) => {
      (startTime.getTime()/1000-0)
    })

    val source: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val test =source.select('global_user_id,'log_time)
      .withColumn("lo_time" ,timestamp_diff(col("log_time"),col("log_time"))).drop("log_time")

    val result = test
      .groupBy('global_user_id)
      .agg((max('lo_time)-min('lo_time))/60 as 'Watch).drop("lo_time")

    val result1 = result.select('global_user_id,
      when('Watch < 1,"1分钟内")
        .when('Watch.cast(IntegerType) between (2,5),"1-5分钟")
        .when('Watch >= 5,"5分钟以上")
        .as('WatchTime)
    )

      .withColumn("id",col("global_user_id").cast(LongType))
      .orderBy('id)
      .drop("global_user_id")
    result1.show(1000,false)


def catalogWrite =
  s"""{
     |"table":{"namespace":"default", "name":"user_profile"},
     |"rowkey":"id",
     |"columns":{
     |"id":{"cf":"rowkey", "col":"id", "type":"long"},
     |"WatchTime":{"cf":"cf", "col":"WatchTime", "type":"string"}
     |}
     |}""".stripMargin

    result1.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()


    spark.stop()
  }


}


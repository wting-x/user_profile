package net.suncaper.model.marketproperty

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, row_number}
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object AdvisoryServiceFrequencyModel {

  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_orders"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |    "memberId":{"cf":"cf", "col":"memberId", "type":"string"},
         |    "orderAmount":{"cf":"cf", "col":"orderAmount", "type":"string"},
         |    "smManualTime":{"cf":"cf", "col":"smManualTime", "type":"string"}
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

    val result = source.groupBy('test)
      .agg(count('orderAmount) as "count", count(when('smManualTime > 0,true)) as "ManualTimeCount")
      .withColumn("id", 'test)
      .withColumn("AdvisoryServiceFrequency",substring(('ManualTimeCount/'count).cast(StringType),0,5))

    //result.show(950, false)

    val classify = result.select('id,
      when('AdvisoryServiceFrequency >="0.3","高")
        .when('AdvisoryServiceFrequency<"0.3" && 'AdvisoryServiceFrequency >="0.1","中")
        .when('AdvisoryServiceFrequency<"0.1" ,"低")
        .otherwise("未知")
        .as("AdvisoryServiceFrequency"))

    classify.orderBy('id).show(950,false)



    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user_profile"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"AdvisoryServiceFrequency":{"cf":"cf", "col":"AdvisoryServiceFrequency", "type":"string"}
         |}
         |}""".stripMargin

    classify.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    spark.stop()
  }
}


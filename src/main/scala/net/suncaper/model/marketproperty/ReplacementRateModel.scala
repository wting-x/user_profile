package net.suncaper.model.marketproperty

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.{count, substring, when}
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ReplacementRateModel {

  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_orders"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |    "memberId":{"cf":"cf", "col":"memberId", "type":"string"},
         |    "orderStatus":{"cf":"cf", "col":"orderStatus", "type":"string"}
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

    //source.show(200, false)

    val result = source.groupBy('test)
      .agg(count(when('orderStatus === "1",true)) as("replacement"),count('orderStatus) as "count")
      .withColumn("id", 'test)
      .withColumn("replacementRate",substring(('replacement/'count).cast(StringType),0,5))
      .drop("test","count","countfalse","replacement")



    val classcify = result.select('id,
      when('replacementRate >="0.0001" && 'replacementRate <"0.3", "中")
        .when('replacementRate < "0.0001", "低")
        .when('replacementRate >="0.3", "高")
        .otherwise("未知")
        .as("replacementRate")
    )

    classcify.sort('id).show(950, false)

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user_profile"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"replacementRate":{"cf":"cf", "col":"replacementRate", "type":"string"}
         |}
         |}""".stripMargin

    classcify.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    spark.stop()
  }

}


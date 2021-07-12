package net.suncaper.model.marketproperty

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, row_number}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object MaxSingleComsumptionModel {

  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_orders"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |    "memberId":{"cf":"cf", "col":"memberId", "type":"string"},
         |    "orderAmount":{"cf":"cf", "col":"orderAmount", "type":"string"}
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
      .agg(max('orderAmount.cast(LongType)) as "Max")
      .withColumn("row_num", row_number() over Window.partitionBy('test).orderBy('Max.desc))
      .where('row_num === 1)
      .withColumn("id", 'test).drop("row_num","orderAmount","test")

    result.show(950, false)

    val classify = result.select('id,
      when('Max >="50000",">50000")
        .when('Max<"49999" && 'Max >="30000","30000-49999")
        .when('Max<"29999" && 'Max >="10000","10000-29999")
        .when('Max<"9999" ,"1-9999")
        .otherwise("未知")
        .as("MaxSingleComsumption"))

    classify.orderBy('id).show(950,false)



    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user_profile"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"MaxSingleComsumption":{"cf":"cf", "col":"MaxSingleComsumption", "type":"string"}
         |}
         |}""".stripMargin

    classify.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    spark.stop()
  }
}


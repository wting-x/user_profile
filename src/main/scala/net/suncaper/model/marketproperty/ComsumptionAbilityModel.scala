package net.suncaper.model.marketproperty

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, row_number}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ComsumptionAbilityModel {

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
      .agg(sum('orderAmount.cast(LongType)) as "Sum")
      .withColumn("row_num", row_number() over Window.partitionBy('test).orderBy('Sum.desc))
      .where('row_num === 1)
      .withColumn("id", 'test).drop("row_num","orderAmount","test")

    result.show(950, false)

    val classify = result.select('id,
      when('Sum >="1000000","超高")
    .when('Sum<"1000000" && 'Sum >="800000","高")
      .when('Sum<"800000" && 'Sum >="600000","中上")
      .when('Sum<"6000000" && 'Sum >="400000","中")
      .when('Sum<"4000000" && 'Sum >="200000","中下")
      .when('Sum<"2000000" && 'Sum >="100000","低")
      .when('Sum<"100000" ,"很低")
        .otherwise("未知")
        .as("ComsumptionAbility"))

    classify.orderBy('id).show(950,false)



    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user_profile"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"ComsumptionAbility":{"cf":"cf", "col":"ComsumptionAbility", "type":"string"}
         |}
         |}""".stripMargin

    classify.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    spark.stop()
  }
}

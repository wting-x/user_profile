package net.suncaper.model.userproperty

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.{count, substring, when}
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object userPriceSensitivity {
  def main(args: Array[String]): Unit = {
    def catalog1 =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_orders"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |    "memberId":{"cf":"cf", "col":"memberId", "type":"string"},
         |    "orderSn":{"cf":"cf", "col":"orderSn", "type":"string"}
         |  }
         |}""".stripMargin

    def catalog2 =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_goods"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |    "productAmount":{"cf":"cf", "col":"productAmount", "type":"string"},
         |    "price":{"cf":"cf", "col":"price", "type":"string"},
         |    "number":{"cf":"cf", "col":"number", "type":"string"},
         |    "shippingFee":{"cf":"cf", "col":"shippingFee", "type":"string"},
         |    "cOrderSn":{"cf":"cf", "col":"cOrderSn", "type":"string"}
         |  }
         |}""".stripMargin

    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()

    import spark.implicits._

    val source1: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog1)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
      .withColumn("test",substring('memberId,-3,3).cast(LongType))

    //source1.show(100, false)

    val source2: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog2)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    //source2.show(20, false)


    val result = source1.join(source2,source1.col("orderSn") === source2.col("cOrderSn"))
      .drop("id","orderSn","cOrderSn","memberId")
    //result.show(100,false)


    val save = result.groupBy("test")
      .agg(count('productAmount) as "orderCount", sum('productAmount.cast(LongType)) as "Total",sum('price.cast(LongType)*'number.cast(LongType)+'shippingFee.cast(LongType)) as "orderValueTotal")
      .withColumn("discountValueTotal",'orderValueTotal-'Total)
      .withColumn("id",'test)
      .drop("test")
    //save.show(100,false)

    val save1 = result.groupBy("test")
      .agg(count(when(('price.cast(LongType)*'number.cast(LongType)+'shippingFee.cast(LongType))-'productAmount.cast(LongType) > 0,true))as "discountOrderCount")
    //save1.show(100,false)

    val result1 = save.join(save1,save.col("id") === save1.col("test"))
    .select("test","orderCount","orderValueTotal", "discountValueTotal","discountOrderCount")
      .withColumn("discountOrderRate",'discountOrderCount/'orderCount)
      .withColumn("avgDiscoutValueRate",('discountValueTotal/'discountOrderCount)/('orderValueTotal/'orderCount))
      .withColumn("discoutValueRate",'discountValueTotal/'orderValueTotal)
      .withColumn("PSM",'discountOrderRate+'avgDiscoutValueRate+'discoutValueRate)


    result1.show(100,false)


    val classify = result1.select('test,
      when('PSM >=1 , "极度敏感")
        .when('PSM>=0.6 && 'PSM<1, "比较敏感")
        .when('PSM>=0.3 && 'PSM<0.6, "一般敏感")
        .when('PSM>=0 && 'PSM<0.3, "不太敏感")
        .when('PSM<0, "极度不敏感")
        .otherwise("未知")
        .as("PriceSensitivity")

    )
    //classify.show(1000,false)

    val classify1 = classify.select("test", "PriceSensitivity")
      .withColumn("id", 'test)
        .drop("test")

    classify1.show(10,false)


    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user_profile"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"PriceSensitivity":{"cf":"cf", "col":"PriceSensitivity", "type":"string"}
         |}
         |}""".stripMargin

    classify1.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    spark.stop()
    }
}

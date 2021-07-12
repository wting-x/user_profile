package net.suncaper.model.marketproperty

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType}

object CouponModel {

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
         |    "activityPrice":{"cf":"cf", "col":"activityPrice", "type":"string"},
         |    "couponAmount":{"cf":"cf", "col":"couponAmount", "type":"string"},
         |    "orderPromotionAmount":{"cf":"cf", "col":"orderPromotionAmount", "type":"string"},
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

    source1.show(100, false)

    val source2: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog2)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    source2.show(20, false)


    val result = source1.join(source2,source1.col("orderSn") === source2.col("cOrderSn"))
      .drop("id","memberId")
    result.show(100,false)

    /*活动券
    activityPrice != 0
    折扣券
    couponAmount != 0
    下单立减
    orderPromotionAmount != 0*/

    val save = result.groupBy("test")
      .agg(count(when('activityPrice =!= "0.00",true) ).cast(StringType) as "activity",
        count(when('couponAmount =!= "0.00",true)).cast(StringType) as "coupon",
        count(when('orderPromotionAmount =!= "0.00",true)).cast(StringType) as "promotion")
      .withColumn("id",'test).drop("test")

    save.show(100,false)

    /*val classify = save.select('id,
      when('activity > 0 && 'coupon > 0 &&'promotion > 0, "有券必买")
        .otherwise("null")
        .as("Coupon"),
      when('activity > 0, "活动券").otherwise("null").as("activityCoupon"),
      when('coupon > 0,"折扣券").otherwise("null").as("discountCoupon"),
      when('promotion > 0,"下单立减").otherwise("null").as("promotionCoupon")
    )

    classify.show(950,false)*/

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user_profile"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"activity":{"cf":"cf", "col":"activity", "type":"string"},
         |"coupon":{"cf":"cf", "col":"coupon", "type":"string"},
         |"promotion":{"cf":"cf", "col":"promotion", "type":"string"}
         |}
         |}""".stripMargin

    save.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    spark.stop()

  }
}
package net.suncaper.model.marketproperty

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

object SaveMoneyModel {

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

    source1.show(100, false)

    val source2: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog2)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    source2.show(20, false)


    val result = source1.join(source2,source1.col("orderSn") === source2.col("cOrderSn"))
    .drop("id","orderSn","cOrderSn","memberId")
    result.show(100,false)

//省钱能手=productAmount/(price*number+shippingFee)

    val save = result.groupBy("test")
      .agg(sum('productAmount.cast(LongType)) as "Total",sum('price.cast(LongType)*'number.cast(LongType)+'shippingFee.cast(LongType)) as "Sum")
      .withColumn("discount",'Total/'Sum)
      .withColumn("id",'test)

    //save.show(100,false)

    val classify = save.select('id,
      when('discount >=0.8 && 'discount<=1, "8折-9折")
        .when('discount>=0.5 && 'discount<0.8, "5折-7折")
        .when('discount>=0.3 && 'discount<0.5, "3折-4折")
        .otherwise("未知")
        .as("discount")
    )
    classify.show(100,false)

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user_profile"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"discount":{"cf":"cf", "col":"discount", "type":"string"}
         |}
         |}""".stripMargin

    classify.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    spark.stop()

  }
}
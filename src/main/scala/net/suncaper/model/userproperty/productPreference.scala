package net.suncaper.model.userproperty

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.{substring, when}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}

object productPreference {
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
         |    "cOrderSn":{"cf":"cf", "col":"cOrderSn", "type":"string"},
         |    "productType":{"cf":"cf", "col":"productType", "type":"string"},
         |    "cateId":{"cf":"cf", "col":"cateId", "type":"string"},
         |    "brandId":{"cf":"cf", "col":"brandId", "type":"string"}
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

    source1.show(20, false)

    val source2: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog2)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    source2.show(20, false)


    val result = source1.join(source2,source1.col("orderSn") === source2.col("cOrderSn"))
      //.select("test","orderSn","productName")
    result.show(100,false)

    /*result.createOrReplaceTempView("temp_table")
    val result2 = result.sqlContext.sql("select test,concat_ws(',',collect_set(productName)) as purchaseGoods from temp_table group by test")
      .withColumn("id", 'test)
      .drop("test")

    result2.show(100,false)*/



}
  }

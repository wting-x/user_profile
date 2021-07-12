package net.suncaper.model.actionproperty

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, LongType}

object ViewGoods {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HBaseTest")
      .master("local") //设定spark 程序运行模式。local在本地运行
      .getOrCreate()

    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_logs"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"global_user_id":{"cf":"cf", "col":"global_user_id", "type":"string"},
         |"loc_url":{"cf":"cf", "col":"loc_url", "type":"string"}
         |}
         |}""".stripMargin
    def catalog1 =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_goods"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"productName":{"cf":"cf", "col":"productName", "type":"string"},
         |"productId":{"cf":"cf", "col":"productId", "type":"string"}
         |}
         |}""".stripMargin

    import spark.implicits._
    val url2ProductId = udf(getProductId _)

    val readDF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val readDF_goods: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog1)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    //     转换
    val result = readDF.select(
      'global_user_id.as("id").cast(DataTypes.LongType),
      url2ProductId('loc_url).as("p_Id").cast(DataTypes.IntegerType)
    ).filter('p_Id.isNotNull)
    //      .show(200, false)

    val result_goods=readDF_goods.select(
      'productId.cast(DataTypes.IntegerType),'productName)
    //      .show(200, false)

    val result3=result.join(result_goods, result.col("p_Id") === result_goods.col("productId"))
      .select('id,'productName)

    result3.createOrReplaceTempView("temp_table")
    val result2 = result3.sqlContext.sql("select id,concat_ws(',',collect_set(productName)) as ViewGoods from temp_table group by id")

    result2.show(100,false)

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user_profile"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"ViewGoods":{"cf":"cf", "col":"ViewGoods", "type":"string"}
         |}
         |}""".stripMargin

    result2.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    spark.stop()

  }
  def getProductId(url: String) = {
    var productId: String = null
    if (url.contains("/product/") && url.contains(".html")) {
      val start: Int = url.indexOf("/product/")
      val end: Int = url.indexOf(".html")
      if (end > start) {
        productId = url.substring(start + 9, end)
      }
    }
    productId
  }
}

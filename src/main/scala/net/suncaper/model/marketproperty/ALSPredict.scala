package net.suncaper.model.marketproperty

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.{count, substring, when}

object ALSPredict {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()

    import spark.implicits._

    /*def logsCatalog =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_logs"},
         |  "rowkey":"id",
         |   "columns":{
         |     "id":{"cf":"rowkey", "col":"id", "type":"Long"},
         |     "global_user_id":{"cf":"cf", "col":"global_user_id", "type":"string"},
         |     "loc_url":{"cf":"cf", "col":"loc_url", "type":"string"}
         |   }
         |}""".stripMargin

    val url2ProductId = udf(getProductId _)

    val logsDF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, logsCatalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val ratingDF = logsDF.select('global_user_id.as("userId").cast(IntegerType),
      url2ProductId('loc_url).as("productId").cast(IntegerType)).filter('productId.isNotNull)
      .groupBy('userId, 'productId)
      .agg(count('productId).as('rating))*/


    val predict2StringFunc = udf(predict2String _)


    val model = ALSModel.load("model/product/als1")

    // 为每个用户推荐rating最高的10个商品，即潜在最喜欢的10个商品
    //但是需要过滤掉用户已经买过的商品（undone）
    val result: DataFrame = model.recommendForAllUsers(10)
      .withColumn("favorProductsId", predict2StringFunc('recommendations))
      .withColumnRenamed("userId", "id")
      .drop('recommendations)
      .select('id.cast(LongType), 'favorProductsId)

    result.show(100, false)

    val separator = ","
    lazy val first = result.first()

    val numAttrs = first.toString().split(separator).length
    val attrs = Array.tabulate(numAttrs)(n => "col_" + n)
    //按指定分隔符拆分value列，生成splitCols列
    var newDF = result.withColumn("splitCols", split($"favorProductsId", separator))
    attrs.zipWithIndex.foreach(x => {
      newDF = newDF.withColumn(x._1, $"splitCols".getItem(x._2))
    })
    //newDF.show()

    val newDF1 = newDF.drop("favorProductsId","splitCols")
    newDF1.show(100,false)

    def catalog2 =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_goods"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |    "productId":{"cf":"cf", "col":"productId", "type":"string"},
         |    "productName":{"cf":"cf", "col":"productName", "type":"string"}
         |  }
         |}""".stripMargin

    val source2: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog2)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val result11 = source2.withColumnRenamed("id", "userId")

    //source22.groupBy("productId")

    val source22 = result11.dropDuplicates("productId")
    source22.groupBy("productId")
    source22.show(100,false)

    val result0 = newDF1.join(source22,newDF1.col("col_0") === source22.col("productId"))
      .drop("productId","col_0","userId")
      //.withColumn("id",substring('id,-3,3).cast(LongType))
        .withColumnRenamed("productName","productName0")
        .drop("col_1","col_2","col_3","col_4","col_5","col_6","col_7","col_8","col_9","col_10")

    result0.show(100,false)

    val result1 = newDF1.join(source22,newDF1.col("col_1") === source22.col("productId"))
      .drop("productId","col_1","userId")
      //.withColumn("id",substring('userId,-3,3).cast(LongType))
      .withColumnRenamed("productName","productName1")
      .drop("col_0","col_2","col_3","col_4","col_5","col_6","col_7","col_8","col_9","col_10")

    val result2 = newDF1.join(source22,newDF1.col("col_2") === source22.col("productId"))
      .drop("productId","col_2","userId")
      //.withColumn("id",substring('userId,-3,3).cast(LongType))
      .withColumnRenamed("productName","productName2")
      .drop("col_1","col_0","col_3","col_4","col_5","col_6","col_7","col_8","col_9","col_10")

    val result3 = newDF1.join(source22,newDF1.col("col_3") === source22.col("productId"))
      .drop("productId","col_3","userId")
      //.withColumn("id",substring('userId,-3,3).cast(LongType))
      .withColumnRenamed("productName","productName3")
      .drop("col_1","col_2","col_0","col_4","col_5","col_6","col_7","col_8","col_9","col_10")

    val result4 = newDF1.join(source22,newDF1.col("col_4") === source22.col("productId"))
      .drop("productId","col_4","userId")
      //.withColumn("id",substring('userId,-3,3).cast(LongType))
      .withColumnRenamed("productName","productName4")
      .drop("col_1","col_2","col_3","col_0","col_5","col_6","col_7","col_8","col_9","col_10")

    val result5 = newDF1.join(source22,newDF1.col("col_5") === source22.col("productId"))
      .drop("productId","col_5","userId")
      //.withColumn("id",substring('userId,-3,3).cast(LongType))
      .withColumnRenamed("productName","productName5")
      .drop("col_1","col_2","col_3","col_4","col_0","col_6","col_7","col_8","col_9","col_10")

    val result6 = newDF1.join(source22,newDF1.col("col_6") === source22.col("productId"))
      .drop("productId","col_6","userId")
      //.withColumn("id",substring('userId,-3,3).cast(LongType))
      .withColumnRenamed("productName","productName6")
      .drop("col_1","col_2","col_3","col_4","col_5","col_0","col_7","col_8","col_9","col_10")

    val result7 = newDF1.join(source22,newDF1.col("col_7") === source22.col("productId"))
      .drop("productId","col_7","userId")
      //.withColumn("id",substring('userId,-3,3).cast(LongType))
      .withColumnRenamed("productName","productName7")
      .drop("col_1","col_2","col_3","col_4","col_5","col_6","col_0","col_8","col_9","col_10")

    val result8 = newDF1.join(source22,newDF1.col("col_8") === source22.col("productId"))
      .drop("productId","col_8","userId")
      //.withColumn("id",substring('userId,-3,3).cast(LongType))
      .withColumnRenamed("productName","productName8")
      .drop("col_1","col_2","col_3","col_4","col_5","col_6","col_7","col_0","col_9","col_10")


    def recommendationCatalog =
      s"""{
         |  "table":{"namespace":"default", "name":"user_profile"},
         |  "rowkey":"id",
         |   "columns":{
         |     "id":{"cf":"rowkey", "col":"id", "type":"Long"},
         |     "productName8":{"cf":"cf", "col":"productName8", "type":"string"}
         |   }
         |}""".stripMargin

    result8.write
      .option(HBaseTableCatalog.tableCatalog, recommendationCatalog)
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

  def predict2String(arr: Seq[Row]) = {
    arr.map(_.getAs[Int]("productId")).mkString(",")
  }


}



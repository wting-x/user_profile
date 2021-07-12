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

object ALSModell {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()

    import spark.implicits._

    def logsCatalog =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_logs"},
         |  "rowkey":"id",
         |   "columns":{
         |     "id":{"cf":"rowkey", "col":"id", "type":"Long"},
         |     "global_user_id":{"cf":"cf", "col":"global_user_id", "type":"string"},
         |     "loc_url":{"cf":"cf", "col":"loc_url", "type":"string"}
         |   }
         |}""".stripMargin

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

    val url2ProductId = udf(getProductId _)

    val logsDF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, logsCatalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val source2: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog2)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val ratingDF = logsDF.select('global_user_id.as("userId").cast(IntegerType),
    url2ProductId('loc_url).as("productId").cast(IntegerType)).filter('productId.isNotNull)
      .groupBy('userId, 'productId)
      .agg(count('productId).as('rating))

    ratingDF.show(100)

    val source22 = source2.withColumnRenamed("userId", "id")
      .withColumnRenamed("productId", "productId1")

    val result = ratingDF.join(source22,ratingDF.col("productId") === source22.col("productId1"))
      .drop("id","productId1")
    result.show(100,false)

    /*// 将数据集切分为两份，其中训练集占80%(0.8), 测试集占20%(0.2)
    val Array(trainSet, testSet) = ratingDF.randomSplit(Array(0.8, 0.2))

    // 回归模型评测器
    val evaluator: RegressionEvaluator = new RegressionEvaluator()
      .setLabelCol("rating")
      .setPredictionCol("predict")
      .setMetricName("rmse")*/

    //训练模型
    val als = new ALS()
      .setUserCol("userId") //用户列
      .setItemCol("productId") //商品列
      .setRatingCol("rating") //rating列
      .setPredictionCol("predict") //预测列的名字
      .setColdStartStrategy("drop")
      .setAlpha(10)
      .setMaxIter(5) //最大迭代次数50
      .setRank(5)
      .setRegParam(0.01) //防止过拟合的参数
      .setImplicitPrefs(true)

    /*// 通过训练集进行训练，建立模型
    val model: ALSModel = als.fit(trainSet)

    // 通过模型进行预测
    val predictions = model.transform(trainSet)

    val rmse = evaluator.evaluate(predictions)
    println(s"rmse value is ${rmse}")*/

    val model: ALSModel = als.fit(result)

    model.save("model/product/als1")

    val predict2StringFunc = udf(predict2String _)

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

package net.suncaper.model.actionproperty

import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.execution.columnar.STRUCT
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, LongType}

object ProductRecommendationModel {
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

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("GenderName")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    def logsCatalog =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_logs"},
         |  "rowkey":"id",
         |   "columns":{
         |     "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |     "global_user_id":{"cf":"cf", "col":"global_user_id", "type":"string"},
         |     "loc_url":{"cf":"cf", "col":"loc_url", "type":"string"}
         |   }
         |}""".stripMargin

    val url2ProductId = udf(getProductId _)

    val logsDF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, logsCatalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val ratingDF = logsDF.select(
      'global_user_id.as("userId").cast(DataTypes.IntegerType),
      url2ProductId('loc_url).as("productId").cast(DataTypes.IntegerType)
    ).filter('productId.isNotNull)
      .groupBy('userId, 'productId)
      .agg(count('productId) as "rating")

    ratingDF.show(1000,false)
    ratingDF.printSchema()

    val als = new ALS()
      .setUserCol("userId")
      .setItemCol("productId")
      .setRatingCol("rating")
      .setPredictionCol("predict")
      .setColdStartStrategy("drop")
      .setAlpha(10)
      .setMaxIter(10)
      .setRank(10)
      .setRegParam(1.0)
      .setImplicitPrefs(true)

    val model: ALSModel = als.fit(ratingDF)

//    model.save("model/product/als")
//
//    val model = ALSModel.load("model/product/als")
//
    val predict2StringFunc = udf(predict2String _)

    // 为每个用户推荐
    val result0: DataFrame = model.recommendForAllUsers(10)
      .withColumn("favorProductsId", predict2StringFunc('recommendations))
      .withColumnRenamed("userId", "id")
      .drop('recommendations)
      .select('id.cast(LongType), 'favorProductsId)

    result0.printSchema()
    result0.sort('userId).show(100, false)

    def catalog =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_goods"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |    "productId":{"cf":"cf", "col":"productId", "type":"string"},
         |    "productName":{"cf":"cf", "col":"productName", "type":"string"}
         |  }
         |}""".stripMargin

    val source: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
      .select("productId","productName")
        .dropDuplicates("productId")

    source.show(20, false)

    result0.createOrReplaceTempView("temp")
    val result = spark.sql("select id,favorProductsId,col_s from temp lateral view explode(split(favorProductsId,\",\")) t as col_s ").toDF()

    val result_RatingDF = result.join(source,result.col("col_s") === source.col("productId"))

    result_RatingDF.printSchema()
    result_RatingDF.show(1000,false)

    result_RatingDF.createOrReplaceTempView("temp2")
    val result2 = result_RatingDF.sqlContext.sql("select id,concat_ws(',',collect_set(productName)) as favorProducts from temp2 group by id")

    result_RatingDF.printSchema()
    result2.show(100,false)

    def recommendationCatalog =
      s"""{
         |  "table":{"namespace":"default", "name":"user_profile"},
         |  "rowkey":"id",
         |   "columns":{
         |     "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |     "favorProducts":{"cf":"cf", "col":"favorProducts", "type":"string"}
         |   }
         |}""".stripMargin

    result2.write
      .option(HBaseTableCatalog.tableCatalog, recommendationCatalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    spark.stop()
  }

}
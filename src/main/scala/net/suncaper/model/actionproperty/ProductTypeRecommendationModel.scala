package net.suncaper.model.actionproperty

import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.{count, substring, udf}
import org.apache.spark.sql.types.{DataTypes, LongType}

object ProductTypeRecommendationModel {
  def predict2String(arr: Seq[Row]) = {
    arr.map(_.getAs[Int]("productId")).mkString(",")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("GenderName")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    def ordersCatalog =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_orders"},
         |  "rowkey":"id",
         |   "columns":{
         |     "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |     "memberId":{"cf":"cf", "col":"memberId", "type":"string"},
         |     "orderSn":{"cf":"cf", "col":"orderSn", "type":"string"}
         |   }
         |}""".stripMargin

    val ordersDF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, ordersCatalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
      .withColumn("test",substring('memberId,-3,3).cast(LongType))

    ordersDF.show(false)

    def goodsCatalog =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_goods"},
         |  "rowkey":"id",
         |   "columns":{
         |     "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |     "productId":{"cf":"cf", "col":"productId", "type":"string"},
         |     "productType":{"cf":"cf", "col":"productType", "type":"string"},
         |    "cOrderSn":{"cf":"cf", "col":"cOrderSn", "type":"string"}
         |   }
         |}""".stripMargin

    val goodsDF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, goodsCatalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    goodsDF.show(false)

    val tempDF = ordersDF.join(goodsDF,ordersDF.col("orderSn") === goodsDF.col("cOrderSn"))
      .select("test","productId")

    tempDF.show(200,false)

//    val tempDF = ordersDF.select("test","orderSn")

    val ratingDF = tempDF.select(
      'test.as("id").cast(DataTypes.IntegerType),
      'productId.cast(DataTypes.IntegerType)
    ).filter('productId.isNotNull)
      .groupBy('id, 'productId)
      .agg(count('productId) as "rating")

    ratingDF.show(300,false)
    ratingDF.printSchema()

    val als = new ALS()
      .setUserCol("id")
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
    val result0: DataFrame = model.recommendForAllUsers(5)
      .withColumn("favorProductsId", predict2StringFunc('recommendations))
      .drop('recommendations)
      .select('id.cast(LongType), 'favorProductsId)

    result0.printSchema()
    result0.sort('id).show(100, false)

    val source: DataFrame = goodsDF.select("productId","productType")
      .dropDuplicates("productId")

    source.show(20, false)

    result0.createOrReplaceTempView("temp")
    val result = spark.sql("select id,favorProductsId,col_s from temp lateral view explode(split(favorProductsId,\",\")) t as col_s ").toDF()

    val result_RatingDF = result.join(source,result.col("col_s") === source.col("productId"))

    result_RatingDF.printSchema()
    result_RatingDF.show(1000,false)

    result_RatingDF.createOrReplaceTempView("temp2")
    val result2 = result_RatingDF.sqlContext.sql("select id,concat_ws(',',collect_set(productType)) as favorProductsType from temp2 group by id")

    result_RatingDF.printSchema()
    result2.show(100,false)

//    result2.dropDuplicates("favorProductsType").show(false)

    def recommendationCatalog =
      s"""{
         |  "table":{"namespace":"default", "name":"user_profile"},
         |  "rowkey":"id",
         |   "columns":{
         |     "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |     "favorProductsType":{"cf":"cf", "col":"favorProductsType", "type":"string"}
         |   }
         |}""".stripMargin

    result2.write
      .option(HBaseTableCatalog.tableCatalog, recommendationCatalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    spark.stop()
  }
}

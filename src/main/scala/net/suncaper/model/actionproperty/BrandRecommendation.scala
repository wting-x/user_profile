package net.suncaper.model.actionproperty

import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{count, substring, udf}
import org.apache.spark.sql.types.{DataTypes, LongType}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.when

object BrandRecommendation {
  def predict2String(arr: Seq[Row]) = {
    arr.map(_.getAs[Int]("brandIdTest")).mkString(",")
  }

  def predict2String2(arr: Seq[Row]) = {
    arr.mkString(",")
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
         |     "brandId":{"cf":"cf", "col":"brandId", "type":"string"},
         |    "cOrderSn":{"cf":"cf", "col":"cOrderSn", "type":"string"}
         |   }
         |}""".stripMargin

    val goodsDF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, goodsCatalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
      .select('cOrderSn,
        when('brandId === "297" || 'brandId === "301" || 'brandId === "0", "1")
          .when('brandId === "298", "2")
          .when('brandId === "299", "3")
          .when('brandId === "303" || 'brandId === "5", "4")
          .when('brandId === "305", "5")
          .otherwise("0")
          .as("brandIdTest")
      )

    goodsDF.show(false)

    val tempDF = ordersDF.join(goodsDF,ordersDF.col("orderSn") === goodsDF.col("cOrderSn"))
      .select('test,'brandIdTest)

    tempDF.show(200,false)

    //    val tempDF = ordersDF.select("test","orderSn")

    val ratingDF = tempDF.select(
      'test.as("id").cast(DataTypes.IntegerType),
      'brandIdTest.cast(DataTypes.IntegerType)
    ).filter('brandIdTest.isNotNull)
      .groupBy('id, 'brandIdTest)
      .agg(count('brandIdTest) as "rating")

    ratingDF.show(300,false)
    ratingDF.printSchema()

    val als = new ALS()
      .setUserCol("id")
      .setItemCol("brandIdTest")
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
    val result0: DataFrame = model.recommendForAllUsers(4)
      .withColumn("favorBranchIdTest", predict2StringFunc('recommendations))
      .drop('recommendations)
      .select('id.cast(LongType), 'favorBranchIdTest)

    result0.printSchema()
    result0.show(100, false)

    result0.createOrReplaceTempView("temp")
    val result = spark.sql("select id,favorBranchIdTest,col_s from temp lateral view explode(split(favorBranchIdTest,\",\")) t as col_s ").toDF()

    val result_RatingDF = result.select('id,
      when('col_s === "1", "海尔")
        .when('col_s === "2", "卡萨帝")
        .when('col_s === "3", "统帅")
        .when('col_s === "4", "小超人")
        .when('col_s === "5", "摩卡")
        .otherwise("其他")
        .as("brandNameTest"))

    result_RatingDF.printSchema()
    result_RatingDF.show(100,false)

    result_RatingDF.createOrReplaceTempView("temp2")

    val result2 = result_RatingDF.sqlContext.sql("select id,concat_ws(',',collect_set(brandNameTest)) as favorBrandName from temp2 group by id")

    result2.show(100,false)

    result2.dropDuplicates("favorBrandName").show(false)

    def recommendationCatalog =
      s"""{
         |  "table":{"namespace":"default", "name":"user_profile"},
         |  "rowkey":"id",
         |   "columns":{
         |     "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |     "favorBrandName":{"cf":"cf", "col":"favorBrandName", "type":"string"}
         |   }
         |}""".stripMargin

    result2.write
      .option(HBaseTableCatalog.tableCatalog, recommendationCatalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    spark.stop()
  }
}

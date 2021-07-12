package net.suncaper.model.userproperty

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.{count, substring, when}
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}


object RFMPredict {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()

    import spark.implicits._

    val model = KMeansModel.load("model/uservalue/Kmeans")

    def catalog =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_orders"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |    "memberId":{"cf":"cf", "col":"memberId", "type":"string"},
         |    "orderSn":{"cf":"cf", "col":"orderSn", "type":"string"},
         |    "orderAmount":{"cf":"cf", "col":"orderAmount", "type":"string"},
         |    "finishTime":{"cf":"cf", "col":"finishTime", "type":"string"}
         |  }
         |}""".stripMargin

    val source: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val colRencency = "rencency"
    val colFrequency = "frequency"
    val colMoneyTotal = "moneyTotal"
    val colFeature = "feature"
    val colPredict = "predict"
    val days_range = 660

    // 统计距离最近一次消费的时间
    val recencyCol = datediff(date_sub(current_timestamp(), days_range), from_unixtime(max('finishTime))) as colRencency
    // 统计订单总数
    val frequencyCol = count('orderSn) as colFrequency
    // 统计订单总金额
    val moneyTotalCol = sum('orderAmount) as colMoneyTotal

    val RFMResult = source.groupBy('memberId)
      .agg(recencyCol, frequencyCol, moneyTotalCol)

    //2.为RFM打分
    //R: 1-3天=5分，4-6天=4分，7-9天=3分，10-15天=2分，大于16天=1分
    //F: ≥200=5分，150-199=4分，100-149=3分，50-99=2分，1-49=1分
    //M: ≥20w=5分，10-19w=4分，5-9w=3分，1-4w=2分，<1w=1分
    val recencyScore: Column = functions.when((col(colRencency) >= 1) && (col(colRencency) <= 3), 5)
      .when((col(colRencency) >= 4) && (col(colRencency) <= 6), 4)
      .when((col(colRencency) >= 7) && (col(colRencency) <= 9), 3)
      .when((col(colRencency) >= 10) && (col(colRencency) <= 15), 2)
      .when(col(colRencency) >= 16, 1)
      .as(colRencency)

    val frequencyScore: Column = functions.when(col(colFrequency) >= 200, 5)
      .when((col(colFrequency) >= 150) && (col(colFrequency) <= 199), 4)
      .when((col(colFrequency) >= 100) && (col(colFrequency) <= 149), 3)
      .when((col(colFrequency) >= 50) && (col(colFrequency) <= 99), 2)
      .when((col(colFrequency) >= 1) && (col(colFrequency) <= 49), 1)
      .as(colFrequency)

    val moneyTotalScore: Column = functions.when(col(colMoneyTotal) >= 200000, 5)
      .when(col(colMoneyTotal).between(100000, 199999), 4)
      .when(col(colMoneyTotal).between(50000, 99999), 3)
      .when(col(colMoneyTotal).between(10000, 49999), 2)
      .when(col(colMoneyTotal) <= 9999, 1)
      .as(colMoneyTotal)

    val RFMScoreResult = RFMResult.select('memberId, recencyScore, frequencyScore, moneyTotalScore)

    val vectorDF = new VectorAssembler()
      .setInputCols(Array(colRencency, colFrequency, colMoneyTotal))
      .setOutputCol(colFeature)
      .transform(RFMScoreResult)


    val predicted = model.transform(vectorDF)

    val a=predicted
      .withColumn("id",substring('memberId,-3,3).cast(LongType))
      .withColumn("userValueRFM",'predict.cast(StringType))
      // .show(10, false)
      //.drop("memberId","rencency","frequency","moneyTotal","feature","predict")

    val result1111 = a.select('id,
      when('userValueRFM ==="0" , "中频率高消费")
        .when ('userValueRFM ==="1" , "中频率较高消费")
        .when ('userValueRFM ==="2" , "低频率中消费")
        .when ('userValueRFM ==="3" , "高频率高消费")
        .when ('userValueRFM ==="4" , "低频率高消费")
        .when ('userValueRFM ==="5" , "低频率低消费")
        .when ('userValueRFM ==="6" , "较低频率较高消费")
        .otherwise("未知")

        .as("userValue")
    )

    result1111.printSchema()
    result1111.show(50,false)

    /*println("中心的点:")
    for (point <- model.clusterCenters) {
      println("  " + point.toString)
    }*/

    //TODO: 将结果写入HBASE
    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user_profile"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"userValue":{"cf":"cf", "col":"userValue", "type":"string"}
         |}
         |}""".stripMargin

    result1111.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    spark.stop()


  }

}

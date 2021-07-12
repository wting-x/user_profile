package net.suncaper.model.userproperty

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.{substring, when}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}

object jiguanModel {

  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_orders"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |    "memberId":{"cf":"cf", "col":"memberId", "type":"string"},
         |    "city":{"cf":"cf", "col":"city", "type":"string"}
         |
         |  }
         |}""".stripMargin

    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()

    import spark.implicits._

    val source: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
      .withColumn("test",substring('memberId,-3,3).cast(LongType))
        .drop("memberId")
      .drop("id")
      .withColumnRenamed("test", "id")


    val result = source.select('id,
      when('city === "716", "北京市")
        .when('city === "173", "山东青岛")
        .when('city === "335", "甘肃兰州")
        .when('city === "23", "werwer")
        .when('city === "361", "宁夏中卫")
        .when('city === "172", "山东济南")
        .when('city === "135", "安徽合肥")
        .when('city === "175", "江西抚州")
        .when('city === "271", "重庆市")
        .when('city === "338", "甘肃白银")
        .when('city === "42", "河北保定")
        .when('city === "152", "福建福州")
        .when('city === "237", "广东韶关")
        .when('city === "154", "福建莆田")
        .when('city === "115", "江苏苏州")
        .when('city === "155", "福建三明")
        .when('city === "161", "江西南昌")
        .when('city === "236", "广东汕头")
        .when('city === "107", "黑龙江黑河")
        .when('city === "109", "黑龙江大兴安岭")
        .when('city === "161", "江西南昌")
        .when('city === "98", "黑龙江齐齐哈尔")
        .when('city === "67", "内蒙古鄂尔多斯")
        .when('city === "108", "黑龙江绥化")
        .when('city === "62", "内蒙古呼和浩特")
        .otherwise("null")
        .as("jiguan")
    )

    result.show(920, false)

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user_profile"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"jiguan":{"cf":"cf", "col":"jiguan", "type":"string"}
         |}
         |}""".stripMargin

    result.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    spark.stop()


  }

}

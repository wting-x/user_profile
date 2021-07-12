package net.suncaper.model.userproperty

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, row_number}
import org.apache.spark.sql.types.LongType

object ConstellationModel {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_users"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"birthday":{"cf":"cf", "col":"birthday", "type":"string"}
         |}
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

    source.show(20, false)

    val result = source.select('id,
      when((month('birthday)=== "3" && dayofmonth('birthday) >= "21") || (month('birthday)=== "4" && dayofmonth('birthday) <= "20"),"白羊座")
        .when((month('birthday)=== "4" && dayofmonth('birthday) >= "21") || (month('birthday)=== "5" && dayofmonth('birthday) <= "20"),"金牛座")
        .when((month('birthday)=== "5" && dayofmonth('birthday) >= "21") || (month('birthday)=== "6" && dayofmonth('birthday) <= "21"),"双子座")
        .when((month('birthday)=== "6" && dayofmonth('birthday) >= "22") || (month('birthday)=== "7" && dayofmonth('birthday) <= "23"),"巨蟹座")
        .when((month('birthday)=== "7" && dayofmonth('birthday) >= "24") || (month('birthday)=== "8" && dayofmonth('birthday) <= "23"),"狮子座")
        .when((month('birthday)=== "8" && dayofmonth('birthday) >= "24") || (month('birthday)=== "9" && dayofmonth('birthday) <= "23"),"处女座")
        .when((month('birthday)=== "9" && dayofmonth('birthday) >= "24") || (month('birthday)=== "10" && dayofmonth('birthday) <= "23"),"天秤座")
        .when((month('birthday)=== "10" && dayofmonth('birthday) >= "24") || (month('birthday)=== "11" && dayofmonth('birthday) <= "22"),"天蝎座")
        .when((month('birthday)=== "11" && dayofmonth('birthday) >= "23") || (month('birthday)=== "12" && dayofmonth('birthday) <= "21"),"人马座")
        .when((month('birthday)=== "12" && dayofmonth('birthday) >= "22") || (month('birthday)=== "1" && dayofmonth('birthday) <= "20"),"摩羯座")
        .when((month('birthday)=== "1" && dayofmonth('birthday) >= "21") || (month('birthday)=== "2" && dayofmonth('birthday) <= "19"),"水瓶座")
        .when((month('birthday)=== "2" && dayofmonth('birthday) >= "20") || (month('birthday)=== "3" && dayofmonth('birthday) <= "20"),"双鱼座")
        .otherwise("未知")
        .as("constellation")
    )

    result.show(20, false)

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user_profile"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"constellation":{"cf":"cf", "col":"constellation", "type":"string"}
         |}
         |}""".stripMargin

    result.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    spark.stop()
  }

}

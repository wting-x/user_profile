package net.suncaper.model.userproperty

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, row_number}
import org.apache.spark.sql.types.LongType

object AgeRangeModel {

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

    /*val result = source.groupBy('memberId, 'paymentCode)
      .agg(count('paymentCode) as "count")
      .withColumn("row_num", row_number() over Window.partitionBy('memberId).orderBy('count.desc))
      .where('row_num === 1)
      .withColumn("id", 'memberId.cast(LongType)).drop("count", "row_num","memberId")*/
    val result = source.select('id,
          when(year('birthday)>="2020" && year('birthday)<"2030", "20后")
            .when(year('birthday)>="2010" && year('birthday)<"2020", "10后")
            .when(year('birthday)>="2000" && year('birthday)<"2010", "00后")
            .when(year('birthday)>="1990" && year('birthday)<"2000", "90后")
            .when(year('birthday)>="1980" && year('birthday)<"1990", "80后")
            .when(year('birthday)>="1970" && year('birthday)<"1980", "70后")
            .when(year('birthday)>="1960" && year('birthday)<"1970", "60后")
            .when(year('birthday)>="1950" && year('birthday)<"1960", "50后")
            .otherwise("未知")
            .as("ageRange")
        )

    result.show(20, false)

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user_profile"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"ageRange":{"cf":"cf", "col":"ageRange", "type":"string"}
         |}
         |}""".stripMargin

    result.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    spark.stop()
  }

}

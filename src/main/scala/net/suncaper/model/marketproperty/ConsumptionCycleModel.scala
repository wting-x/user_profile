package net.suncaper.model.marketproperty

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, row_number}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ConsumptionCycleModel {

  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_orders"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |    "memberId":{"cf":"cf", "col":"memberId", "type":"string"},
         |    "finishTime":{"cf":"cf", "col":"finishTime", "type":"string"}
         |  }
         |}""".stripMargin

    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val source: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
      .withColumn("test",substring('memberId,-3,3).cast(LongType))
      .withColumn("Time",'finishTime.cast(LongType))
      .where('Time > "0")
        .orderBy('test,'Time).drop("memberId","id")

    //source.show(900, false)

    val result = source.groupBy('test)
      .agg(max('Time) as "max",min('Time) as "min",count('Time) as "count")
      //.withColumn("lifecycle",max('finishTime)-min('finishTime))
      .withColumn("id", 'test).drop("test")

    //result.show(200,false)

    //val temp = result.select('id,'count,from_unixtime('min).as("from_start"),from_unixtime('max).as("from_end"))

    var consumption=datediff(from_unixtime('max),from_unixtime('min)).as("consumption")
    val userComsup = result.select('id,'count, consumption).orderBy('id)
    val userComsup1 = userComsup
      .withColumn("consumptionCycle",'consumption.cast(LongType) / 'count.cast(LongType))

    val classify = userComsup1.select('id,
      when('consumptionCycle >="150","6月")
        .when('consumptionCycle<"150" && 'consumptionCycle >="120","5月")
        .when('consumptionCycle<"120" && 'consumptionCycle >="90","4月")
        .when('consumptionCycle<"90" && 'consumptionCycle >="60","3月")
        .when('consumptionCycle<"60" && 'consumptionCycle >="30","2月")
        .when('consumptionCycle<"30" && 'consumptionCycle >="14","1月")
        .when('consumptionCycle<"14" && 'consumptionCycle >="7","2周")
        .when('consumptionCycle<"7" ,"7日")
        .otherwise("未知")
        .as("consumptionCycle"))

    classify.orderBy('id).show(950,false)

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user_profile"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"consumptionCycle":{"cf":"cf", "col":"consumptionCycle", "type":"string"}
         |}
         |}""".stripMargin

    classify.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    spark.stop()
  }
}

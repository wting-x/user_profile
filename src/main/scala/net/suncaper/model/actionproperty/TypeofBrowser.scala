package net.suncaper.model.actionproperty


  import org.apache.spark.sql.{DataFrame, SparkSession}
  import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
  import org.apache.spark.sql.functions.{col, when}
  import org.apache.spark.sql.types.LongType
  //有问题
  object TypeofBrowser {
    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder()
        .appName("HBaseTest")
        .master("local") //设定spark 程序运行模式。local在本地运行
        .getOrCreate()

      def catalog =
        s"""{
           |"table":{"namespace":"default", "name":"tbl_logs"},
           |"rowkey":"id",
           |"columns":{
           |"id":{"cf":"rowkey", "col":"id", "type":"long"},
           |"global_user_id":{"cf":"cf", "col":"global_user_id", "type":"string"},
           |"ref_url":{"cf":"cf", "col":"ref_url", "type":"string"},
           |"loc_url":{"cf":"cf", "col":"loc_url", "type":"string"}
           |}
           |}""".stripMargin

      import spark.implicits._

      val readDF: DataFrame = spark.read
        .option(HBaseTableCatalog.tableCatalog, catalog)
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()


      //     转换
      val result = readDF.select('global_user_id,
        when('loc_url like "%login%", "登录页")
          .when('loc_url like "%index%", "首页")
          .when('loc_url like "http://www.eshop.com/", "首页")
          .when('loc_url like "http://m.eshop.com/", "首页")
          .when('loc_url like "%/c/%", "分类页")
          .when('loc_url like "%/product/%", "商品页")
          .when('loc_url like "%order%", "我的订单页")
          .otherwise("其他")
          .as("browser_loc_url"),
        when('ref_url like "%login%", "登录页")
          .when('ref_url like "%index%", "首页")
          .when('ref_url like "http://www.eshop.com/", "首页")
          .when('ref_url like "http://m.eshop.com/", "首页")
          .when('ref_url like "%/c/%", "分类页")
          .when('ref_url like "%/product/%", "商品页")
          .when('ref_url like "%order%", "我的订单页")
          .otherwise("其他")
          .as("browser_ref_url")
      )

        .withColumn("id",col("global_user_id").cast(LongType))
        .drop("'loc_url","global_user_id","ref_url")

          def catalogWrite =
            s"""{
               |"table":{"namespace":"default", "name":"user_profile"},
               |"rowkey":"id",
               |"columns":{
               |"id":{"cf":"rowkey", "col":"id", "type":"long"},
               |"browser_ref_url":{"cf":"cf", "col":"browser_ref_url", "type":"string"},
               |"browser_loc_url":{"cf":"cf", "col":"browser_loc_url", "type":"string"}
               |}
               |}""".stripMargin

          result.write
            .option(HBaseTableCatalog.tableCatalog, catalogWrite)
            .option(HBaseTableCatalog.newTable, "5")
            .format("org.apache.spark.sql.execution.datasources.hbase")
            .save()


      spark.stop()
    }

}

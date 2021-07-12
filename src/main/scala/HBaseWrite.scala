package net.suncaper.project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

object HBaseWrite {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HBaseTest")
      .master("local") //设定spark 程序运行模式。local在本地运行
      .getOrCreate()

    def catalog = s"""{
                     |"table":{"namespace":"default", "name":"tbl_users"},
                     |"rowkey":"id",
                     |"columns":{
                     |"id":{"cf":"rowkey", "col":"id", "type":"long"},
                     |"username":{"cf":"cf", "col":"username", "type":"string"}
                     |}
                     |}""".stripMargin

    //    table：去读取哪张表的的数据  ，namespace：默认空间下name:tabl_users
    //    rowkey:主键 id
    //    columns：读取哪些列
    //    id:   cf:列族  col:列名 type:列的数据类型
    //    user_agent：  cf:列族  col:列名 type:列的数据类型

    val df=spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()


    def catalogWrite = s"""{
                     |"table":{"namespace":"default", "name":"tbl_users_clone"},
                     |"rowkey":"id",
                     |"columns":{
                     |"id":{"cf":"rowkey", "col":"id", "type":"long"},
                     |"username":{"cf":"cf", "col":"username", "type":"string"}
                     |}
                     |}""".stripMargin

    df.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable,"5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    spark.stop()

  }

}

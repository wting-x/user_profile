import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object Hbase2MySQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HBaseTest")
      .master("local") //设定spark 程序运行模式。local在本地运行
      .getOrCreate()

    def catalog = s"""{
                     |"table":{"namespace":"default", "name":"user_profile"},
                     |"rowkey":"id",
                     |"columns":{
                     |"id":{"cf":"rowkey", "col":"id", "type":"long"},
                     |"gender":{"cf":"cf", "col":"gender", "type":"string"},
                     |"nationality":{"cf":"cf", "col":"nationality", "type":"string"},
                     |"politicalFace":{"cf":"cf", "col":"politicalFace", "type":"string"},
                     |"paymentCode":{"cf":"cf", "col":"paymentCode", "type":"string"},
                     |"ageRange":{"cf":"cf", "col":"ageRange", "type":"string"},
                     |"constellation":{"cf":"cf", "col":"constellation", "type":"string"},
                     |"LatestLogin":{"cf":"cf", "col":"LatestLogin", "type":"string"}
                     |}
                     |}""".stripMargin



    val studentDF = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    studentDF.show(10,false)
    //      .option("delimiter", "\t")
    //      .schema(schema)
    //      .csv("dataset/user_profile")

    studentDF
      .write.format("jdbc").mode(SaveMode.Overwrite)
      .option("url", "jdbc:mysql://localhost:3306/userpro?useUnicode=true&characterEncoding=UTF-8")
      .option("dbtable", "user_profile")
      .option("user", "root")
      .option("password", "123456")
      .save()



    spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/userpro?useUnicode=true&characterEncoding=UTF-8")
      .option("dbtable", "user_profile")
      .option("user", "root")
      .option("password", "123456")
      .load()
      .show()

    spark.stop()

    //    table：去读取哪张表的的数据  ，namespace：默认空间下name:tabl_users
    //    rowkey:主键 id
    //    columns：读取哪些列
    //    id:   cf:列族  col:列名 type:列的数据类型
    //    user_agent：  cf:列族  col:列名 type:列的数据类型
  }

}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

object HBaseRead {

  def main(args: Array[String]): Unit = {
    def catalog = s"""{
                     |"table":{"namespace":"default", "name":"user_profile"},
                     |"rowkey":"id",
                     |"columns":{
                     |"id":{"cf":"rowkey", "col":"id", "type":"long"},
                     |"num":{"cf":"cf", "col":"num", "type":"string"},
                     |"gender":{"cf":"cf", "col":"gender", "type":"string"},
                     |"jiguan":{"cf":"cf", "col":"jiguan", "type":"string"},
                     |"job":{"cf":"cf", "col":"job", "type":"string"},
                     |"nationality":{"cf":"cf", "col":"nationality", "type":"string"},
                     |"politicalFace":{"cf":"cf", "col":"politicalFace", "type":"string"},
                     |"paymentCode":{"cf":"cf", "col":"paymentCode", "type":"string"},
                     |"ageRange":{"cf":"cf", "col":"ageRange", "type":"string"},
                     |"constellation":{"cf":"cf", "col":"constellation", "type":"string"},
                     |"ComsumptionAbility":{"cf":"cf", "col":"ComsumptionAbility", "type":"string"},
                     |"MaxSingleComsumption":{"cf":"cf", "col":"MaxSingleComsumption", "type":"string"},
                     |"PerCustomerTransation":{"cf":"cf", "col":"PerCustomerTransation", "type":"string"},
                     |"purchaseFrequency":{"cf":"cf", "col":"purchaseFrequency", "type":"string"},
                     |"device":{"cf":"cf", "col":"device", "type":"string"},
                     |"userValue":{"cf":"cf", "col":"userValue", "type":"string"},
                     |"favorProducts":{"cf":"cf", "col":"favorProducts", "type":"string"},
                     |"discount":{"cf":"cf", "col":"discount", "type":"string"},
                     |"favorProducts":{"cf":"cf", "col":"favorProducts", "type":"string"},
                     |"favorProductsType":{"cf":"cf", "col":"favorProductsType", "type":"string"},
                     |"PriceSensitivity":{"cf":"cf", "col":"PriceSensitivity", "type":"string"},
                     |"favorProductsType":{"cf":"cf", "col":"favorProductsType", "type":"string"},
                     |"favorBrandName":{"cf":"cf", "col":"favorBrandName", "type":"string"},
                     |
                     |"browser_loc_url":{"cf":"cf", "col":"browser_loc_url", "type":"string"},
                     |"productName0":{"cf":"cf", "col":"productName0", "type":"string"},
                     |"productName1":{"cf":"cf", "col":"productName1", "type":"string"},
                     |"productName2":{"cf":"cf", "col":"productName2", "type":"string"},
                     |"productName3":{"cf":"cf", "col":"productName3", "type":"string"},
                     |"productName4":{"cf":"cf", "col":"productName4", "type":"string"},
                     |"productName5":{"cf":"cf", "col":"productName5", "type":"string"},
                     |"productName6":{"cf":"cf", "col":"productName6", "type":"string"},
                     |"productName7":{"cf":"cf", "col":"productName7", "type":"string"},
                     |"productName8":{"cf":"cf", "col":"productName8", "type":"string"},
                     |"LoginFrequency":{"cf":"cf", "col":"LoginFrequency", "type":"string"},
                     |"ViewFrequency":{"cf":"cf", "col":"ViewFrequency", "type":"string"}
                     |}
                     |}""".stripMargin

    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()

    spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
      .show(1000,false)

    spark.stop()
  }

}

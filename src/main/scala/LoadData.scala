/* SimpleApp.scala */
// import org.apache.spark.SparkContext
// import org.apache.spark.SparkContext._
// import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
// import org.apache.spark.sql.functions._ // for sort(desc("x"))
import java.nio.file._
// import org.apache.spark.implicits._
import org.apache.spark.sql.types.{StructType, StructField, StringType, DecimalType}

object LoadData {
  def main(args: Array[String]): Unit = {
    // val logFile = "YOUR_SPARK_HOME/README.md" // Should be some file on your system

    // create paths
    val datRootPath = Paths.get(sys.env("SWSDATA"), "faoswsTrade", "data", "original")
    val datFilePath = Paths.get(datRootPath.toString, datFile)

    // https://github.com/databricks/spark-csv
    val sqlContext = new SparkSession.Builder().master("local[4]").appName("Spark SQL Example").config("spark.sql.warehouse.dir", datRootPath.toString).getOrCreate()
    // // For implicit conversions like converting RDDs to DataFrames
    // import sqlContext.implicits._

    // general
    val datYear = 2009
    val datDelim = ","

    // Eurostat
    // val datFile = "nc" + datYear + "52.dat"
    // val datSchema = StructType(Array(
    //                              StructField("DECLARANT", StringType, true),
    //                              StructField("PARTNER", StringType, true),
    //                              StructField("PRODUCT_NC", StringType, true),
    //                              StructField("FLOW", StringType, true),
    //                              StructField("STAT_REGIME", StringType, true),
    //                              StructField("PERIOD", StringType, true),
    //                              // precision and scale of decimal type
    //                              // according to comext support
    //                              StructField("VALUE_1000ECU", DecimalType(17, 3), true),
    //                              StructField("QUANTITY_TON", DecimalType(17, 3), true),
    //                              StructField("SUP_QUANTITY", DecimalType(14, 0), true)
    //                            )
    // )

    // UNSD Tariffline
    val datFile = "ct_tariffline_unlogged_" + datYear + ".csv"

    val datSchema = StructType(Array(
                                 StructField("chapter", StringType, true),
                                 StructField("rep", StringType, true),
                                 StructField("tyear", StringType, true),
                                 StructField("curr", StringType, true),
                                 StructField("hsrep", StringType, true),
                                 StructField("flow", StringType, true),
                                 StructField("repcurr", StringType, true),
                                 StructField("comm", StringType, true),
                                 StructField("prt", StringType, true),
                                 StructField("weight", DecimalType(20,2), true),
                                 StructField("qty", DecimalType(20,2), true),
                                 StructField("qunit", StringType, true),
                                 StructField("tvalue", DecimalType(20,2), true),
                                 StructField("est", StringType, true),
                                 StructField("ht", StringType, true)
                               )
    )

    // // identify schema
    // val dfString  = sqlContext.read.format("csv").option("header", "true").load(datFilePath.toString)
    // dfString.columns

    // load with schema
    val df = sqlContext.read.format("csv").option("header", "true").schema(datSchema).load(datFilePath.toString)
    // test dataset
    // df.head(6)
    // df.show()
    // df.printSchema()

    // df.select("DECLARANT", "PARTNER").distinct().show()
    // df.select("PRODUCT_NC", "SUP_QUANTITY").distinct().show()
    // df.select("SUP_QUANTITY").distinct().show(100)
    // How many bids per item?
    // df.groupBy("DECLARANT", "PARTNER", "FLOW", "STAT_REGIME").count.show
    // df.groupBy("DECLARANT", "FLOW").count.show(100)
    // df.groupBy("DECLARANT", "PARTNER").count.orderBy("count").show(100)
    // import org.apache.spark.sql.DataFrame
    // df.groupBy("DECLARANT", "PARTNER").count.sort($"count".desc).show(100)
    // df.groupBy("DECLARANT", "PARTNER").count.sort(desc("count")).show(100)

    df.select("rep", "prt").distinct().show()



  }
}

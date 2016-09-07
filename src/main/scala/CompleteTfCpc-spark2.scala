// import org.apache.spark.SparkContext
// import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
// import org.apache.hadoop.fs.s3a.S3AFileSystem
// import org.apache.spark.sql.hive.HiveContext
// SparkSession is now the new entry point of Spark that replaces the old SQLContext and HiveContext. Note that the old SQLContext and HiveContext are kept for backward compatibility. A new catalog interface is accessible from SparkSession - existing API on databases and tables access such as listTables, createExternalTable, dropTempView, cacheTable are moved here.
import org.apache.spark.sql.types.{StructType, StructField, StringType, DecimalType}

object CompleteTfCpcSpark2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Complete Trade Flow CPC")
      // .config("spark.some.config.option", "some-value")
      .getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    // AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for account '058644585154', user "tradeuser" specified as environment variables
    val fcl_2_cpc_csvFile = "s3a://us-west-2-databricks/faosws/fcl_2_cpc.csv"
    // val fcl_2_cpc_csv = spark.read.text(fcl_2_cpc_csvFile.toString).as[String]
    // // get columns
    // val fcl_2_cpc_csv = spark.read.format("csv").option("header", "true").load(fcl_2_cpc_csvFile.toString)
    // fcl_2_cpc_csv.columns
    case class class_fcl2cpc(fcl: String, cpc: String)
    val fcl2cpc = spark.read.format("csv").option("header", "true").load(fcl_2_cpc_csvFile.toString).as[class_fcl2cpc]
    // test dataset
    // fcl2cpc.head(6)
    // fcl2cpc.show()
    // fcl2cpc.printSchema()

    val nc200852_datFile = "s3a://us-west-2-databricks/nc200852.dat"
    val nc200852_dat = spark.read.text(nc200852_datFile.toString).as[String]

    val ct_tariffline_unlogged_2008_csvFile = "s3a://us-west-2-databricks/ct_tariffline_unlogged_2008.csv"
    val ct_tariffline_unlogged_2008_csv = spark.read.text(ct_tariffline_unlogged_2008_csvFile.toString).as[String]


    // println("\n" + fcl_2_cpc_csv.first() + "\n" + nc200852_dat.first() + "\n" + ct_tariffline_unlogged_2008_csv.first() + "\n")

    sc.stop()
  }
}


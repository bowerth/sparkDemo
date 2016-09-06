// import org.apache.spark.SparkContext
// import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
// import org.apache.hadoop.fs.s3a.S3AFileSystem
// import org.apache.spark.sql.hive.HiveContext
// SparkSession is now the new entry point of Spark that replaces the old SQLContext and HiveContext. Note that the old SQLContext and HiveContext are kept for backward compatibility. A new catalog interface is accessible from SparkSession - existing API on databases and tables access such as listTables, createExternalTable, dropTempView, cacheTable are moved here.

object CompleteTfCpc {
  def main(args: Array[String]): Unit = {

    // val conf = new SparkConf().setAppName("Complete Trade Flow CPC").setMaster("local")
    // val sc = new SparkContext(conf)
    // val hiveCtx = new org.apache.spark.sql.hive.HiveContext(sc)

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Complete Trade Flow CPC")
      // .config("spark.some.config.option", "some-value")
      .getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    // can also set "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" in spark.properties
    // val hadoopConf = sc.hadoopConfiguration
    // hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    // AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for account '058644585154', user "tradeuser" specified as environment variables
    val fcl_2_cpc_csvFile = "s3a://us-west-2-databricks/faosws/fcl_2_cpc.csv"
    // val fcl_2_cpc_csv = sc.textFile(fcl_2_cpc_csvFile.toString, 2)
    val fcl_2_cpc_csv = spark.read(fcl_2_cpc_csvFile.toString, 2)



    val nc200852_datFile = "s3a://us-west-2-databricks/nc200852.dat"
    val nc200852_dat = sc.textFile(nc200852_datFile.toString, 2)

    val ct_tariffline_unlogged_2008_csvFile = "s3a://us-west-2-databricks/ct_tariffline_unlogged_2008.csv"
    val ct_tariffline_unlogged_2008_csv = sc.textFile(ct_tariffline_unlogged_2008_csvFile.toString, 2)


    // println("\n" + fcl_2_cpc_csv.first() + "\n" + nc200852_dat.first() + "\n" + ct_tariffline_unlogged_2008_csv.first() + "\n")

    sc.stop()
  }
}


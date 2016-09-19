package org.fao.trade.load

// import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
// import org.apache.spark.sql.Encoder
import org.apache.spark.sql.SparkSession
// import org.apache.spark.sql.Row
// import org.apache.spark.sql.types._
import java.nio.file._

object LoadDataDS {

  case class fcl2cpc(fcl: String, cpc: String)

  case class esclass(DECLARANT: String,
                     PARTNER: String,
                     PRODUCT_NC: String,
                     FLOW: String,
                     STAT_REGIME: String,
                     PERIOD: String,
                     // precision and scale of decimal type
                     // according to comext support
                     VALUE_1000ECU: String, // Double,
                     QUANTITY_TON: String, // Double,
                     SUP_QUANTITY: String) // Double

  case class tlclass(chapter: String,
                     rep: String,
                     tyear: String,
                     curr: String,
                     hsrep: String,
                     flow: String,
                     repcurr: String,
                     comm: String,
                     prt: String,
                     weight: String,
                     qty: String,
                     qunit: String,
                     tvalue: String,
                     est: String,
                     ht: String)

  def main(args: Array[String]): Unit = {

    // val origDir = Paths.get(sys.env("SWSDATA"), "faoswsTrade", "data", "original").toString
    // val warehouseDir = "s3a://us-west-2-databricks"
    // val warehouseLocation = "file:${system:user.dir}/spark-warehouse"

    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("Load Data")
    // .config("spark.sql.parquet.compression.codec", "snappy")
      // .config("spark.sql.warehouse.dir", warehouseLocation)
      // .enableHiveSupport()
      .getOrCreate()

    // spark.conf.get("spark.sql.warehouse.dir")

    val s3bucket = sys.env("AWS_S3_BUCKET")

    // val filename = "faosws/fcl_2_cpc.csv"
    // val filename = "nc200852.dat"
    val filename = "ct_tariffline_unlogged_2008.csv"

    val textfile = Paths.get(s3bucket, filename).toString
    val parquetfile = textfile.replace(".dat", ".parquet").replace(".csv", ".parquet")

    // runTextToParquet(spark = spark, textfile = textfile, parquetfile = parquetfile)
    runShowParquet(spark = spark, parquetfile = parquetfile)

    spark.stop()
  }

  private def runTextToParquet(spark: SparkSession, textfile: String, parquetfile: String): Unit = {
    import spark.implicits._
    // val parquetfile = textfile.replace(".csv", ".parquet").replace(".dat", ".parquet")

    val classDS = spark.read.option("header", true).format("csv").load(textfile).as[fcl2cpc]
    // val classDS = spark.read.option("header", true).format("csv").load(textfile).as[esclass]
    // val classDS = spark.read.option("header", true).format("csv").load(textfile).as[tlclass]

    classDS.write.mode("overwrite").parquet(parquetfile)
  }

  private def runShowParquet(spark: SparkSession, parquetfile: String): Unit = {
    val parquetFileDF = spark.read.parquet(parquetfile)
    parquetFileDF.createOrReplaceTempView("parquetTable")
    spark.sql("SELECT * FROM parquetTable LIMIT 10").show()
  }

  // private def runLoadTLData(spark: SparkSession, year: Int): Unit = {

  // }
  //   // general
  //   // val datDelim = ","

  //   // Eurostat
  //   // val datFile = "nc" + datYear + "52.dat"
  //   // val datSchema = StructType(Array(
  //   //                              StructField("DECLARANT", StringType, true),
  //   //                              StructField("PARTNER", StringType, true),
  //   //                              StructField("PRODUCT_NC", StringType, true),
  //   //                              StructField("FLOW", StringType, true),
  //   //                              StructField("STAT_REGIME", StringType, true),
  //   //                              StructField("PERIOD", StringType, true),
  //   //                              // precision and scale of decimal type
  //   //                              // according to comext support
  //   //                              StructField("VALUE_1000ECU", DecimalType(17, 3), true),
  //   //                              StructField("QUANTITY_TON", DecimalType(17, 3), true),
  //   //                              StructField("SUP_QUANTITY", DecimalType(14, 0), true)
  //   //                            )
  //   // )

  //   // UNSD Tariffline
  //   val datFile = "ct_tariffline_unlogged_" + datYear + ".csv"
  //   val datFilePath = Paths.get(datRootPath.toString, datFile)

  //   val datSchema = StructType(Array(
  //                                StructField("chapter", StringType, true),
  //                                StructField("rep", StringType, true),
  //                                StructField("tyear", StringType, true),
  //                                StructField("curr", StringType, true),
  //                                StructField("hsrep", StringType, true),
  //                                StructField("flow", StringType, true),
  //                                StructField("repcurr", StringType, true),
  //                                StructField("comm", StringType, true),
  //                                StructField("prt", StringType, true),
  //                                StructField("weight", DecimalType(20,2), true),
  //                                StructField("qty", DecimalType(20,2), true),
  //                                StructField("qunit", StringType, true),
  //                                StructField("tvalue", DecimalType(20,2), true),
  //                                StructField("est", StringType, true),
  //                                StructField("ht", StringType, true)
  //                              )
  //   )

  //   // // identify schema
  //   // val dfString  = sqlContext.read.format("csv").option("header", "true").load(datFilePath.toString)
  //   // dfString.columns

  //   // load with schema
  //   val df = sqlContext.read.format("csv").option("header", "true").schema(datSchema).load(datFilePath.toString)
  //   // test dataset
  //   // df.head(6)
  //   // df.show()
  //   // df.printSchema()

  //   // df.select("DECLARANT", "PARTNER").distinct().show()
  //   // df.select("PRODUCT_NC", "SUP_QUANTITY").distinct().show()
  //   // df.select("SUP_QUANTITY").distinct().show(100)
  //   // How many bids per item?
  //   // df.groupBy("DECLARANT", "PARTNER", "FLOW", "STAT_REGIME").count.show
  //   // df.groupBy("DECLARANT", "FLOW").count.show(100)
  //   // df.groupBy("DECLARANT", "PARTNER").count.orderBy("count").show(100)
  //   // import org.apache.spark.sql.DataFrame
  //   // df.groupBy("DECLARANT", "PARTNER").count.sort($"count".desc).show(100)
  //   // df.groupBy("DECLARANT", "PARTNER").count.sort(desc("count")).show(100)

  //   df.select("rep", "prt").distinct().show()

  // }

  // private def runHiveExample(spark: SparkSession): Unit = {

  //   import spark.implicits._
  //   import spark.sql

  //   sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
  //   sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

  //   sql("SELECT * FROM src").show()

  //   sql("SELECT COUNT(*) FROM src").show()

  //   // The results of SQL queries are themselves DataFrames and support all normal functions.
  //   val sqlDF = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

  //   // The items in DaraFrames are of type Row, which allows you to access each column by ordinal.
  //   val stringsDS = sqlDF.map {
  //     case Row(key: Int, value: String) => s"Key: $key, Value: $value"
  //   }
  //   stringsDS.show()

  // spark.catalog.listTables.show(false)
  //   spark.catalog.listDatabases.show(false)

  // }

}

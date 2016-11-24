package controller.load

// activator "run-main controller.load.LoadDataDS"
    
// import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
// import org.apache.spark.sql.Encoder
import org.apache.spark.sql.SparkSession
// import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import java.nio.file._
// import java.io.File
  // import models.fcl2cpc
import models._
// import models.esclass
// import models.tlclass
// import models.hsfclmap
// import org.fao.trade.xml.Uncs
// import models.Uncs
// import utils.fileUtils

object LoadDataDS {

  def main(args: Array[String]): Unit = {

    val origDir = Paths.get(sys.env("DRYFAOFBS"), "data", "original").toString
    val warehouseLocation = sys.env("SPARK_WAREHOUSE")
    val derivS3bucket = sys.env("AWS_S3_BUCKET_DERIVED")
    val origS3bucket = sys.env("AWS_S3_BUCKET_ORIGINAL")
    // sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    // %sql set spark.sql.parquet.compression.codec=snappy

    val spark = SparkSession
      .builder()
      .master("local[8]")
      .appName("Load Data")
      // .config("spark.sql.parquet.compression.codec", "gzip")
      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      // .enableHiveSupport()
      .getOrCreate()

    val fileext = ".csv.gz"

    // // hsfclmap2
    // val fileprefix = "hsfclmap2"
    // val folder = "hsfclmap2"
    // // val outfilename = Paths.get(origDir, "spark_count_statregime.csv").toString
    // val outfilename = ""

    // // Eurostat SWS
    // val fileprefix = "ce_combinednomenclature_unlogged_"
    // val folder = "ce_combinednomenclature_unlogged"
    // // val outfilename = Paths.get(origDir, "spark_count_statregime.csv").toString
    // val outfilename = ""

    // SWS UNSD Tariffline
    val fileprefix = "ct_tariffline_unlogged_"
    val folder = "ct_tariffline_unlogged"
    // val outfilename = Paths.get(origDir, "spark_count_ct_tl_hsrep.csv").toString
    val outfilename = ""

    // // hsfclmap2
    // val fileprefix = "hsfclmap2"
    // val fileext = ".csv.gz"
    // val folder = "hsfclmap2"
    // val outfilename = ""

    // use partitioed parquetfiles, partition by year
    val parquetfolder = Paths.get(derivS3bucket, folder).toString
    val timerange = 2000 to 2008
    // val timerange = null
    // val filenames = for (yr <- yrs) yield filename + yr.toString + fileext

    // for (filename <- filenames) {
    // for (filename <- filenames.toArray) {
    // val yr = 2012

    // case class tlclass2(chapter: String,
    //                    rep: String,
    //                    tyear: String,
    //                    curr: String,
    //                    hsrep: String,
    //                    flow: String,
    //                    repcurr: String,
    //                    comm: String,
    //                    prt: String,
    //                    weight: Decimal,
    //                    qty: Double,
    //                    qunit: String,
    //                    tvalue: Double,
    //                    est: String,
    //                    ht: String)


    runMultipleTextToParquet(spark = spark, origS3bucket = sys.env("AWS_S3_BUCKET_ORIGINAL"), fileprefix = fileprefix, fileext = fileext, timerange = timerange, parquetfolder = parquetfolder)

    // no time period, e.g. hsfclmap
    // runTextToParquet(spark = spark, origS3bucket = sys.env("AWS_S3_BUCKET_ORIGINAL"), fileprefix = fileprefix, fileext = fileext, parquetfolder = parquetfolder)

    // runShowParquet(spark = spark, parquetfile = parquetfolder, show = true, outfilename = outfilename)

    // runXmlDownload(url = "http://comtrade.un.org/ws/getsdmxtarifflinev1.aspx?px=H2&y=2005,2006&r=400&rg=1&p=392&cc=442190900&comp=false",
    //                filename = "data/TariffLineSdmx.xml")

    // val xmlMessage = runXmlRead(filename = "data/TariffLineSdmx.xml")
    // print("\n" + xmlMessage + "\n\n")

    spark.stop

  }

  // private def runTextToParquet(spark: SparkSession, textfile: String, parquetfile: String): Unit = {
  private def runMultipleTextToParquet(spark: SparkSession, origS3bucket: String, fileprefix: String, fileext: String, timerange: Range, parquetfolder: String): Unit = {

    // val origS3bucket = sys.env("AWS_S3_BUCKET_ORIGINAL")

    import spark.implicits._
    for (year <- timerange.toArray) {
      val filename = fileprefix + year.toString + fileext
      val textfile = Paths.get(origS3bucket, filename).toString
      // val parquetfile = textfile.replace(".dat", ".parquet").replace(".csv", ".parquet")
      // val parquetfile = Paths.get(s3bucket, "ct_tariffline_unlogged").toString
      val subfolder = "year=" + year.toString
      val parquetfile = Paths.get(parquetfolder, subfolder).toString
      // runTextToParquet(spark = spark, textfile = textfile, parquetfile = parquetfile)

      // val classDS = spark.read.option("header", true).format("csv").load(textfile).as[esclass]
      // val classDS = spark.read.option("header", true).format("csv").load(textfile).as[fcl2cpc]
      // val classDS = spark.read.option("header", true).format("csv").load(textfile).as[tlclass]
      val classDS = spark.read.option("header", true).format("csv").load(textfile).as[tlclass]
      // val classDS = spark.read.option("header", true).format("csv").load(textfile)

      classDS.repartition(8).write.mode("overwrite").parquet(parquetfile)
    }

    // val parquetfile = textfile.replace(".csv", ".parquet").replace(".dat", ".parquet")
    // classDS.write.mode("overwrite").parquet(parquetfile)
  }

    private def runTextToParquet(spark: SparkSession, origS3bucket: String, fileprefix: String, fileext: String, parquetfolder: String): Unit = {

    import spark.implicits._
      val filename = fileprefix + fileext
      val textfile = Paths.get(origS3bucket, filename).toString
      // val parquetfile = textfile.replace(".dat", ".parquet").replace(".csv", ".parquet")
      // val parquetfile = Paths.get(s3bucket, "ct_tariffline_unlogged").toString
      val parquetfile = Paths.get(parquetfolder).toString
      // runTextToParquet(spark = spark, textfile = textfile, parquetfile = parquetfile)

      // val classDS = spark.read.option("header", true).format("csv").load(textfile).as[hsfclmap]
      val classDS = spark.read.option("header", true).format("csv").load(textfile).as[hsfclmap]

      classDS.repartition(8).write.mode("overwrite").parquet(parquetfile)

    // val parquetfile = textfile.replace(".csv", ".parquet").replace(".dat", ".parquet")
    // classDS.write.mode("overwrite").parquet(parquetfile)
  }


  // private def runXmlDownload(url: String, filename: String): String = {

  //   fileUtils.fileDownloader(url = url, filename = filename)

  // }

  // private def runXmlRead(filename: String): String = {

  //   // val comtradeUrl = "http://comtrade.un.org/ws/getsdmxtarifflinev1.aspx?px=H2&y=2005,2006&r=400&rg=1&p=392&cc=442190900&comp=false"
  //   // val xmlFilename = "data/TariffLineSdmx.xml"
  //   // val xmlFile = new File(xmlFilename)

  //   val xmlFile = new File(filename)
  //   val tariffUncs = scala.xml.XML.loadFile(xmlFile)
  //   val comtr = Uncs.fromXml(tariffUncs, group = 0, section = 0, obs = 0).toString

  //   return(comtr)

  // }

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

  // private def runJSONExample(spark: SparkSession): Unit = {

  //   // {"RPT":"400", "time":"2005", "CL":"H2", "UNIT_MULT":"1", "DECIMALS":"1", "CURRENCY":"USD", "FREQ":"A", "TIME_FORMAT":"P1Y", "REPORTED_CLASSIFICATION":"H2", "FLOWS_IN_DATASET":"MXR", "SECTIONS":[{ "TF":"1", "REPORTED_CURRENCY":"JOD", "CONVERSION_FACTOR":"1.410440", "VALUATION":"CIF", "TRADE_SYSTEM":"Special", "PARTNER":"Origin", "OBS":[{ "CC-H2":"442190900", "PRT":"392", "netweight":"438", "qty":"438", "QU":"8", "value":"2238.36828", "EST":"0", "HT":"0" }, { "CC-H2":"442190900", "PRT":"422", "netweight":"88883", "qty":"88883", "QU":"8", "value":"385604.42292", "EST":"0", "HT":"0" }]}]}

  //   import org.apache.spark.sql.functions._
  //   import spark.implicits._

  //   val uncsDF = spark.read.format("json").load("examples/src/main/resources/uncs_simple_2.json")

  //   uncsDF.select($"RPT", explode($"SECTIONS").as("SECTIONS_flat"))
  //   val flattened = uncsDF.select($"RPT", explode($"SECTIONS").as("SECTIONS_flat"))
  //   flattened.show()

  //   val sections = flattened.select("RPT", "SECTIONS_flat.OBS")
  //   sections.show()

  //   val obs = sections.select($"RPT", explode($"OBS").as("OBS_flat"))
  //   obs.show()

  //   // .explode($"OBS").as("OBS_flat"))

  // }
}

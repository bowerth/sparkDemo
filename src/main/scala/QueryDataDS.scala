package controller.query

import org.apache.spark.sql.SparkSession
import java.io.File

object QueryDataDS {

  def main(args: Array[String]): Unit = {

    // val args = Array("ct_tariffline_unlogged", "chapter", "test.csv")

    val warehouseLocation = sys.env("SPARK_WAREHOUSE")
    val derivS3bucket = sys.env("AWS_S3_BUCKET_DERIVED")

    val spark = SparkSession.builder().master("local[8]").appName("Load Data").config("spark.sql.parquet.compression.codec", "snappy").config("spark.sql.warehouse.dir", warehouseLocation).getOrCreate()

    val parquetfile = derivS3bucket + "/" + args(0)
    // val parquetfile = derivS3bucket + "/" + "ct_tariffline_unlogged"
    // "ce_combinednomenclature_unlogged"

    val parquetFileDF = spark.read.option("mergeSchema", "true").parquet(parquetfile)
    parquetFileDF.printSchema()
    parquetFileDF.createOrReplaceTempView("parquetTable")

    // query 1
    val selectedData = spark.sql("SELECT year, " + args(1) + ", COUNT(*) AS cnt FROM parquetTable GROUP BY year, " + args(1) + " ORDER BY year DESC, " + args(1)).cache()


    // val selectedData = spark.sql("SELECT year, hsrep, COUNT(*) AS cnt FROM parquetTable GROUP BY year, hsrep ORDER BY year DESC, hsrep").cache()

    selectedData.show(100)

    // examples query1
    // ce_combinednomenclature_unlogged
    // activator "run-main controller.query.QueryDataDS ce_combinednomenclature_unlogged declarant spark_count_ce_cn8_declarant.csv"
    // activator "run-main controller.query.QueryDataDS ce_combinednomenclature_unlogged partner spark_count_ce_cn8_partner.csv"
    // activator "run-main controller.query.QueryDataDS ce_combinednomenclature_unlogged statregime spark_count_ce_cn8_statregime.csv"
    // ct_tariffline_unlogged
    // activator "run-main controller.query.QueryDataDS ct_tariffline_unlogged hsrep spark_count_ct_tl_hsrep.csv"
    // activator "run-main controller.query.QueryDataDS ct_tariffline_unlogged chapter spark_count_ct_tl_chapter.csv"
    // activator "run-main controller.query.QueryDataDS ct_tariffline_unlogged rep spark_count_ct_tl_rep.csv"
    // activator "run-main controller.query.QueryDataDS ct_tariffline_unlogged prt spark_count_ct_tl_prt.csv"


    val outfilename = args(2)
    // val outfilename = "spark_count_ct_tl_hsrep.csv"

    if (outfilename != "") {

      val tmpParquetDir = "Posts.tmp.parquet"

      selectedData.repartition(1).write
        .mode("overwrite")
        .format("csv")
        .option("header", "true")
        .save(tmpParquetDir)

      val dir = new File(tmpParquetDir)
      // val outfolder = "data/derived"
      val outfile = new File(outfilename)
      // val outfile = new File(outfolder + "/" + outfilename)
      if (outfile.exists) outfile.delete
      val tmpTsvFileName = dir.list.filter(_.endsWith(".csv"))(0)
      val tmpTsvFile = tmpParquetDir + File.separatorChar + tmpTsvFileName
        (new File(tmpTsvFile)).renameTo(outfile)
      dir.listFiles.foreach( f => f.delete )
      dir.delete

    }

    spark.stop

  }
}

package controller.query

import org.apache.spark.sql.SparkSession

object QueryDataDS {

  def main(args: Array[String]): Unit = {

    val warehouseLocation = sys.env("SPARK_WAREHOUSE")
    val derivS3bucket = sys.env("AWS_S3_BUCKET_DERIVED")

    val spark = SparkSession.builder().master("local[8]").appName("Load Data").config("spark.sql.parquet.compression.codec", "snappy").config("spark.sql.warehouse.dir", warehouseLocation).getOrCreate()

    val parquetfile = derivS3bucket + "/" + args(0)
    // "ce_combinednomenclature_unlogged"

    val parquetFileDF = spark.read.option("mergeSchema", "true").parquet(parquetfile)
    parquetFileDF.printSchema()
    parquetFileDF.createOrReplaceTempView("parquetTable")

    val selectedData = spark.sql("SELECT year, " + args(1) + ", COUNT(*) AS cnt FROM parquetTable GROUP BY year, " + args(1) + " ORDER BY year DESC, " + args(1)).cache()


    selectedData.show(100)

    // activator "run-main controller.query.QueryDataDS ce_combinednomenclature_unlogged stat_regime"
    //  activator "run-main controller.query.QueryDataDS ct_tariffline_unlogged hsrep"

  }
}

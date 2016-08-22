/* SimpleApp.scala */
import org.apache.spark.SparkContext
// import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
// import java.nio.file.Path
// import java.nio.file.Paths
// import java.nio.file.FileSystem

object SimpleApp {
  def main(args: Array[String]): Unit = {

    // setMaster:
    //  - "local" to run locally with one thread
    //  - "local[4]" to run locally with 4 cores
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

    // val logFile = "C:\\Users\\Werthb\\Programs\\apache-spark\\spark-2.0.0-bin-hadoop2.7\\README.md"
    val logFile = java.nio.file.Paths.get(sys.env("SPARK_HOME"), "README.md")
    // val logFile = "s3n://ir-comext-1/2016S2/data/nc200852.dat"
    // AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for account '058644585154', user "tradeuser" specified as environment variables
    // "s3a://..." suggested by databricks
    // java.io.IOException: No FileSystem for scheme: s3a
    // val logFile = "s3a://ir-faosws-1/fcl_2_cpc.csv"

    // val logData = sc.textFile(logFile, 2).cache()
    val logData = sc.textFile(logFile.toString, 2).cache()
    // val test = logData.first()
    // // test: String = fcl,cpc

    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    // SparkContext.stop()
    sc.stop()
  }
}

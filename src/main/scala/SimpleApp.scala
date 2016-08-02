/* SimpleApp.scala */
import org.apache.spark.SparkContext
// import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]): Unit = {
    // val logFile = "YOUR_SPARK_HOME/README.md" // Should be some file on your system
    val logFile = "C:\\Users\\Werthb\\Programs\\apache-spark\\spark-2.0.0-bin-hadoop2.7\\README.md"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    // setMaster:
    //  - "local" to run locally with one thread
    //  - "local[4]" to run locally with 4 cores
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}

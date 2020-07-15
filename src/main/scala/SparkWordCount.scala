import SparkWordCount.spark
import org.apache.spark.sql.SparkSession

object SparkWordCount extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    .getOrCreate()

  val lines = spark.sparkContext.parallelize(
    Seq("Spark Intellij Idea Scala test one",
      "Spark Intellij Idea Scala test two",
      "Spark Intellij Idea Scala test three"))

  val counts = lines
    .flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)

  counts.foreach(println)

  case class Transaction(TransID: String, CustID: String, TransTotal: String, TransNumItems: String, TransDesc: String)

  val transactions = spark.sparkContext.textFile("data/transactions.txt").map(_.split(",")).map(t =>
      Transaction(t(0), t(1), t(2), t(3), t(4)))

}
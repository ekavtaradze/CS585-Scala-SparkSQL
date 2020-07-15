import SparkWordCount.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, StringType}

object SparkWordCount extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    .getOrCreate()

  /*val lines = spark.sparkContext.parallelize(
    Seq("Spark Intellij Idea Scala test one",
      "Spark Intellij Idea Scala test two",
      "Spark Intellij Idea Scala test three"))

  val counts = lines
    .flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)

  counts.foreach(println)*/

  case class Transaction(TransID: String, CustID: String, TransTotal: String, TransNumItems: String, TransDesc: String)

  import spark.sqlContext.implicits._

  //start with transactions as Data Frame
  val transactionsDF = spark.sparkContext.textFile("data/dummy.txt").map(_.split(",")).map(t =>
      Transaction(t(0), t(1), t(2), t(3), t(4))).toDF() //an RDD

  //transactions.registerTempTable("transactions")
 // transactionsDF.select("TransID")

  //T1:	Filter	out	(drop)	the	transactions	from	T whose	total	amount	is	less	than	$200
  val T1 = transactionsDF.where('TransTotal >=200)
  T1.collect().foreach(println)

/*T2:	Over	T1,	group	the	transactions	by	the	Number	of	Items	it	has, and	for	each	group
  calculate	the	sum	of	total	amounts,	the	average	of	total	amounts,	the	min	and	the	max	of
  the	total	amounts. */
  val grouping = T1.groupBy('TransNumItems);
/*
3) Report	back	T2	to	the	client	side
4) T3:	Over	T1,	group	the	transactions	by	customer	ID,	and	for	each	group	report	the
  customer	ID,	and	the	transactions’	count.
5) T4:	Filter	out	(drop)	the	transactions	from	T whose	total	amount	is	less	than	$600
  6) T5:	Over	T4,	group	the	transactions	by	customer	ID,	and	for	each	group	report	the
  customer	ID,	and	the	transactions’	count.
7) T6:	Select	the	customer	IDs	whose		T5.count	*	5 <	T3.count
8) Report	back	T6	to	the	client	side*/





}
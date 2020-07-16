//import QuestionOne.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.functions._

object QuestionOne extends App {

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Question One")
      .getOrCreate()

    case class Transaction(TransID: String, CustID: String, TransTotal: Float, TransNumItems: Int, TransDesc: String)

    import spark.sqlContext.implicits._


    val T = spark.sparkContext.textFile("data/dummy.txt").map(_.split(",")).map(t =>
      Transaction(t(0), t(1), t(2).toFloat, t(3).toInt, t(4)))

    //start with transactions as Data Frame
    val transactionsDF = T.toDF() //an RDD

    /*1) T1:	Filter	out	(drop)	the	transactions	from	T whose	total	amount	is	less	than	$200
  * */
    val T1 = transactionsDF.where('TransTotal >= 200)


    /*T2:	Over	T1,	group	the	transactions	by	the	Number	of	Items	it	has, and	for	each	group
    calculate	the	sum	of	total	amounts,	the	average	of	total	amounts,	the	min	and	the	max	of
    the	total	amounts. */
    val T2 = T1.groupBy("TransNumItems").agg(sum("TransTotal"), avg("TransTotal"),
      min("TransTotal"), max("TransTotal"))


    /*3) Report	back	T2	to	the	client	side*/
    T2.show()


    /*4) T3:	Over	T1,	group	the	transactions	by	customer	ID,	and	for	each	group	report	the
      customer	ID,	and	the	transactions’	count*/
    val T3 = T1.groupBy("CustID").agg(count("TransID").as("Count"))


    /*5) T4:	Filter	out	(drop)	the	transactions	from	T whose	total	amount	is	less	than	$600*/
    val T4 = transactionsDF.where('TransTotal >= 600)


    /*6) T5:	Over	T4,	group	the	transactions	by	customer	ID,	and	for	each	group	report	the
    customer	ID,	and	the	transactions’	count.*/
    val T5 = T4.groupBy('CustID).agg(count("TransID").as("Count"))


    /*7) T6:	Select	the	customer	IDs	whose		T5.count	*	5 <	T3.count*/
    T5.createTempView("t5Temp")
    T3.createTempView("t3Temp")
    val T6 = spark.sql("SELECT CustID FROM (SELECT t5Temp.CustID, t5Temp.Count AS T5Count, t3Temp.Count AS T3Count" +
      " FROM t5Temp INNER JOIN t3Temp ON t5Temp.CustID = t3Temp.CustID) WHERE T5Count*5 < T3Count")


    /*8) Report	back	T6	to	the	client	side*/
    T6.show()

    spark.stop()

}
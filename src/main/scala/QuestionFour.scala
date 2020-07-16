import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object QuestionFour extends App{
   // def main(args: Array[String]) {
     /* val logFile = "README.md" // Should be some file on your system
      val spark = SparkSession.builder.master("local[*]").appName("Simple Application").getOrCreate()
      val logData = spark.read.textFile(logFile).cache()
      val numAs = logData.filter(line => line.contains("a")).count()
      val numBs = logData.filter(line => line.contains("b")).count()
      println(s"Lines with a: $numAs, Lines with b: $numBs")
      spark.stop()*/

      val spark = SparkSession.builder
        .master("local[*]")
        .appName("Question One")
        .getOrCreate()

      case class Matrix(X: Int, Y: Int, Value: Int)

      //val sqlContext= new org.apache.spark.sql.SQLContext(SparkContext)


      //import sqlContext.implicits._
      import spark.sqlContext.implicits._


      //Got Dataframes
      val matrix1 = spark.sparkContext.textFile("data/M1.txt").map(_.split(",")).map(t =>
        Matrix(t(0).toInt, t(1).toInt, t(2).toInt)).toDF()
      val matrix2 = spark.sparkContext.textFile("data/M2.txt").map(_.split(",")).map(t =>
        Matrix(t(0).toInt, t(1).toInt, t(2).toInt)).toDF().show()

      //Map matrix 1 to Key: y   Value: M1, x, Value
       //Map matrix 2 to Key: x   Value: M2, y, Value

      //Reduce
      ///Matrix1.X, Matrix2.Y, Matrix1 xy * Matrix 2 xy  = v


      //start with transactions as Data Frame
     // val transactionsDF = T.toDF() //an RDD
  //  }
}

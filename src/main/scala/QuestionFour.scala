import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object QuestionFour{
   def main(args: Array[String]) {
     /* val logFile = "README.md" // Should be some file on your system
      val spark = SparkSession.builder.master("local[*]").appName("Simple Application").getOrCreate()
      val logData = spark.read.textFile(logFile).cache()
      val numAs = logData.filter(line => line.contains("a")).count()
      val numBs = logData.filter(line => line.contains("b")).count()
      println(s"Lines with a: $numAs, Lines with b: $numBs")
      spark.stop()*/

     //val spark: SparkSession = SparkSession.builder.master("local").getOrCreate

     val spark = SparkSession.builder
        .master("local[*]")
        .appName("Question One")
        .getOrCreate()

     val sc = spark.sparkContext // Just used to create test RDDs

      case class Matrix(X: Int, Y: Int, Value: Int)


     val matrixSchema = new StructType()
       .add(StructField("X",IntegerType, true))
       .add(StructField("Y",IntegerType, true))
       .add(StructField("Value",IntegerType, true))

      import spark.sqlContext.implicits._



     val matrix1RDD= spark.sparkContext.textFile("data/M1.txt").map(_.split(","))
       .map(t => Row(t(0).toInt, t(1).toInt, t(2).toInt))
     val matrix2RDD= spark.sparkContext.textFile("data/M2.txt").map(_.split(","))
       .map(t => Row(t(0).toInt, t(1).toInt, t(2).toInt))


     val matrix1DF = spark.sqlContext.createDataFrame(matrix1RDD, matrixSchema).show()
     val matrix2DF = spark.sqlContext.createDataFrame(matrix2RDD, matrixSchema).show()


  }

}

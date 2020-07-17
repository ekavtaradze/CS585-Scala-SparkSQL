import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

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


    /* val matrix1RDD= spark.sparkContext.textFile("data/M1.txt").map(line=> (line.split(",")(1),
       Row(line(1).toInt, "M2", line(0).toInt, line(2).toInt) ))
     println(matrix1RDD)
     val matrix1DF = spark.sqlContext.createDataFrame(matrix1RDD).show()*/
     //  .map(t => Row(t(0).toInt, t(1).toInt, t(2).toInt))
/*   val matrix2RDD= spark.sparkContext.textFile("data/M2.txt").map(_.split(","))
       .map(t => Row(t(0).toInt, t(1).toInt, t(2).toInt))*/

    /* val matrix1RDD= spark.sparkContext.textFile("data/M1.txt").map(_.split(","))
       .map(t => Row(t(0).toInt, t(1).toInt, t(2).toInt))
     val matrix2RDD= spark.sparkContext.textFile("data/M2.txt").map(_.split(","))
       .map(t => Row(t(0).toInt, t(1).toInt, t(2).toInt))


     val matrix1DF = spark.sqlContext.createDataFrame(matrix1RDD, matrixSchema)
     val matrix2DF = spark.sqlContext.createDataFrame(matrix2RDD, matrixSchema)

     matrix1DF.show()
     matrix2DF.show()*/

     //map 1 should look like this
   //  Key: j       value: (FlagM, i, Mij)
     //map 1 should look like this
     //  Key: j         value: (FlagN, k, Njk)

   val matrix1MapSchema = new StructType()
       .add(StructField("J",IntegerType, true))
       .add(StructField("Matrix1", StringType, true))
       .add(StructField("I",IntegerType, true))
       .add(StructField("Value1",IntegerType, true))

     val matrix2MapSchema = new StructType()
       .add(StructField("J",IntegerType, true))
       .add(StructField("Matrix2", StringType, true))
       .add(StructField("K",IntegerType, true))
       .add(StructField("Value2",IntegerType, true))

     val M1= spark.sparkContext.textFile("data/M1.txt").map(_.split(","))
       .map(t => Row(t(1).toInt, "M1", t(0).toInt, t(2).toInt))
     val M2= spark.sparkContext.textFile("data/M2.txt").map(_.split(","))
       .map(t => Row(t(0).toInt, "M2", t(1).toInt, t(2).toInt))

     val m1DF = spark.sqlContext.createDataFrame(M1, matrix1MapSchema)
     val m2DF = spark.sqlContext.createDataFrame(M2, matrix2MapSchema)

     m1DF.show()
     m2DF.show()



    val joined =  m1DF.join(m2DF, "J")

     joined.show()

     /*
     Reduce Function (For a given j)•Separate values from M and N•Produce output (i, k, Mij * Njk  = v)
      */
     //val reduce = joined.select($"I",$"J", $"K", $"Value1" * $"Value2")
    val reduce = joined.select($"I", $"K", $"Value1" * $"Value2")
    // val r = joined.select($"I", $"K", $"Value1 * Value2".alias("Value"))

  reduce.show()





  }

}

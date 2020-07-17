import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object QuestionFour {
  def main(args: Array[String]) {

    //val spark: SparkSession = SparkSession.builder.master("local").getOrCreate

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Question One")
      .getOrCreate()

    //  val sc = spark.sparkContext // Just used to create test RDDs

    //  case class Matrix(X: Int, Y: Int, Value: Int)

    import spark.sqlContext.implicits._


    //map 1 should look like this
    //  Key: j       value: (FlagM, i, Mij)
    //map 1 should look like this
    //  Key: j         value: (FlagN, k, Njk)

    val matrix1MapSchema = new StructType()
      .add(StructField("J", IntegerType, true))
      .add(StructField("Matrix1", StringType, true))
      .add(StructField("I", IntegerType, true))
      .add(StructField("Value1", IntegerType, true))

    val matrix2MapSchema = new StructType()
      .add(StructField("J", IntegerType, true))
      .add(StructField("Matrix2", StringType, true))
      .add(StructField("K", IntegerType, true))
      .add(StructField("Value2", IntegerType, true))

    val M1 = spark.sparkContext.textFile("data/M1.txt").map(_.split(","))
      .map(t => Row(t(1).toInt, "M1", t(0).toInt, t(2).toInt))
    val M2 = spark.sparkContext.textFile("data/M2.txt").map(_.split(","))
      .map(t => Row(t(0).toInt, "M2", t(1).toInt, t(2).toInt))

    val m1DF = spark.sqlContext.createDataFrame(M1, matrix1MapSchema)
    val m2DF = spark.sqlContext.createDataFrame(M2, matrix2MapSchema)

    m1DF.show()
    m2DF.show()


    val joined = m1DF.join(m2DF, "J")

    joined.show()

    /*
    Reduce Function (For a given j)•Separate values from M and N•Produce output (i, k, Mij * Njk  = v)
     */
    //val reduce = joined.select($"I",$"J", $"K", $"Value1" * $"Value2")
    val reduceJoin = joined.select($"I", $"K", ($"Value1" * $"Value2").alias("Value"))
    // val r = joined.select($"I", $"K", $"Value1 * Value2".alias("Value"))

    reduceJoin.show()


    /*
    STARTING AGGREGATION
     */
    //val m1Agg = reduce.select()

    val reduceGroupBy = reduceJoin.groupBy("I", "K")
    val reduceSum = reduceGroupBy.sum("Value").show()
    // val grouped = joined


  }

}

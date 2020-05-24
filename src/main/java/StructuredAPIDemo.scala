import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col,column,expr,lit}
import org.apache.spark.sql.functions.{desc, asc}

object StructuredAPIDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Hello World").getOrCreate()
    method1(spark)
    method2(spark)
    createRows()
    createDF(spark)
  }

  def method1 (spark : SparkSession): Unit = {
    val df = spark.range(20).toDF("number")
    df.show()
    df.select(df.col("number") + 10).show()
    spark.range(1,11,2,3).toDF().collect().foreach(println)
  }

  def method2 ( spark:SparkSession) :Unit = {
    val sch = StructType ( Array(StructField( "id" , IntegerType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("gender",StringType,nullable = true),
      StructField("subject", StringType,nullable = true),
      StructField("marks", IntegerType, nullable = true)))

    //val df1 = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("C:\\Users\\Chandrika Sanjay\\student.csv")
    val df1 = spark.read.format("csv").option("header", "true").schema(sch).load("C:\\Users\\Chandrika Sanjay\\student.csv")
    df1.printSchema()
    println("Column names in the dataframe: ")
    for ( i <- df1.columns) {
      println(i)
    }
    println(df1.first())
  }

  def createRows() :Unit = {
    val myRow = Row ("Sankir", 1234, null, false)
    println(myRow(0))
    myRow(1)
    myRow(2)
    myRow(3)
    myRow(0).asInstanceOf[String]
    myRow.getString(0)
    myRow.getInt(1)
    myRow.getString(2)
    myRow.getBoolean(3)
  }

  def createDF(spark:SparkSession) :Unit = {
    val sc = spark.sparkContext

    val mySchema = new StructType(Array( StructField("name", StringType, nullable = true),
      StructField("salary", IntegerType, nullable = true),
      StructField("city", StringType, nullable = true)) )

    val myRows = Seq( Row("aaa", 50000, "Bangalore"),
      Row("bbb", 75000, "Mumbai"),
      Row("ccc", 150000, "Chennai" ))

    

    val rdd1 = sc.parallelize(myRows)

    val myDF = spark.createDataFrame(rdd1, mySchema)

    myDF.show()

    myDF.select ("name").show

    myDF.select(myDF.col("salary") + 10000 ).show

    myDF.select(myDF.col("name"), col("name"), column("name"),  expr("name")).show

    myDF.selectExpr("name as NEWname").show()

    myDF.selectExpr("salary + 10000 as NEWsal").show()

    myDF.select( expr("*"), lit(1).as("One")).show()

    myDF.orderBy(desc("salary")).show

    myDF.orderBy(asc("city")).show()

    myDF.withColumn("Age", lit(30)).show

    myDF.withColumn("Salary less than 75000", expr("salary <= 75000")).show

    myDF.withColumnRenamed("name", "NEWNAME").show()

    myDF.drop("city").show()

    myDF.withColumn("salary2", col("salary").cast("Long")).show

    myDF.withColumn("salary2", col("salary").cast("Long")).printSchema()
  }

}

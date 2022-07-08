import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object SparkUDF extends App {

  def adultOrNot(age:Int):String={
    if(age<25)
      "Young"
    else
      "Adult"
  }
  val spark = SparkSession
    .builder()
    .appName("UDF Functions")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._;
  val dframe = spark.read
    .format("csv")
    .option("inferSchema","true")
    .option("path","E://Data//sessionDemo.txt")
    .load()
    .toDF("name","age","city")

  val dframe2 = spark.read
    .format("csv")
    .option("inferSchema","true")
    .option("path","E://Data//sessionDemo.txt")
    .load()
    .toDF("name","age","city")

  // UDF: Column Object : cannot be used with SQL
  val ObjectFunction = udf(adultOrNot(_:Int):String)

  val way1 = dframe.withColumn("Type",ObjectFunction(col("age")));

  way1.show(10)

  // UDF: String Object: can be used with SQL
  spark.udf.register("StringFunction",adultOrNot(_:Int):String)

  val way2 = dframe2.withColumn("Type",expr("StringFunction(age)"))

  way2.show(10)

  // SQL Query

  dframe2.createOrReplaceTempView("people")

  val way3 = spark.sql("select name,age,city,StringFunction(age) as Type from people")

  way3.show(10);

  spark.close();
}

import org.apache.spark.sql._;
import org.apache.spark.sql.functions._;


object DataframeDataset extends App {

  case class SampleClass(name:String,age:Int,city:String);
  // Creating Spark Session
  val sparkSession = SparkSession
    .builder
    .appName("Dataframes and Datasets")
    .master("local[*]")
    .getOrCreate()

  // Reading the data into dataframe
  val dataframe = sparkSession.read
    .option("inferSchema","true")
    .csv("E://Data//sessionDemo.txt")

  dataframe.show(10,false);

  // Reading using case class
  import sparkSession.implicits._

  // converting the dataframe to Dataset
  var dataset = dataframe.toDF("name","age","city")
    .as[SampleClass];

  dataset.show(10,false);

  // adding new column to the dataframe
  val dataset2 = dataset.withColumn("salary",col("age")*10)

  // modifying the existing column
  dataset.withColumn("age",col("age").*(2)).show()

  // Creating a table and displaying
  dataset.createOrReplaceTempView("sampleTable")

  sparkSession.sql("select name,age,'India' as country from sampleTable").show(10);

  dataset2.show(10,false);

  sparkSession.close();
}

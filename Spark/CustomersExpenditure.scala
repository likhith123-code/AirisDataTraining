import org.apache.spark.SparkContext

object CustomersExpenditure extends App{

  def splitColumns(line:String):(Int,Float)={
    var parts = line.split(",");
    var id = parts(0).toInt;
    val amount = parts(2).toFloat;
    (id,amount);
  }
  val context = new SparkContext("local[*]","Customer Expenditures");

  // data: customer id, product id, amount Spent

  val file = context.textFile("E://Data//customerorders.csv");

  val cols = file.map(splitColumns);

  // Summation of the Amount
  val reduceAmount = cols.reduceByKey((x,y)=>x+y);

  // Sorting based on Amount Spent
  val sorting = reduceAmount.sortBy(x=>x._2,false);

  // printing the Top 10 customers
  sorting.take(10).foreach(println);
}

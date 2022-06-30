import org.apache.spark._;
object AverageByAge extends App{

  def splitColumns(line:String):(Int,Int) = {
    var parts = line.split(",");
    val age = parts(2).toInt;
    val friends = parts(3).toInt;
    (age,friends);
  }
  val context = new SparkContext("local[*]","Average By Age")
  // reading the data
  val file = context.textFile("E://Data//friendsdataNew.txt");

  // data: id,name,age,friends
  val ageAndFriends = file.map(splitColumns);

  // Making the tuple: (age,(friends,1))

  val tuples = ageAndFriends.mapValues(x=>(x,1));

  // Reduce By Key
  val reduceTheValues = tuples.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2));

  // Making the average
  val averageofAges = reduceTheValues.mapValues(x=>(x._1/x._2)).sortBy(x=>x._1);

  averageofAges.collect().foreach(println);


}

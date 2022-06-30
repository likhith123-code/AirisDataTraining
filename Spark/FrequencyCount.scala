import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext;


object FrequencyCount extends App{
 // displaying only Error Logs
 Logger.getLogger("org").setLevel(Level.ERROR);

 val sc = new SparkContext("local[*]","WordCount");

 // Reading the file
 val file = sc.textFile("E://WordCountExample.txt")

 // Creating a flat map for all the words
 val flatMap = file.flatMap(x=>x.split(" "))

 // Create each word as (word,1) tuple
 val wordsTuple = flatMap.map(x=>(x,1))

 // Reducing the counts: Sort by words
 val wordCount = wordsTuple.reduceByKey((x,y)=>x+y).sortByKey()

 /*
 Sort by Count: Swap - Sort By key in descending(key will be count) - Swap
 val wordCount = wordsTuple.reduceByKey((x,y)=>x+y).map(x=>(x._2,x._1)) : Shuffling, swapping
 val wordCount = wordsTuple.reduceByKey((x,y)=>x+y).map(x=>(x._2,x._1)).sortByKey() : Sort by frequency
 val wordCount = wordsTuple.reduceByKey((x,y)=>x+y).map(x=>(x._2,x._1)).sortByKey(false) : Sort by freq desc order
  */

 // printing on to the console
 wordCount.collect().foreach(println)
}

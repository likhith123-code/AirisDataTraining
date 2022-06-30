import scala.io.StdIn;
object PerfectSquare {

  def checkFactor(num:Int):Boolean = {
    var count:Int = 0;
    for(i <- 1 to num/2){
      if(i*i == num)
        return true;
    }
    false;
  }
  def main(args:Array[String]){
    println("Enter the number of People : ");
    var people:Int = scala.io.StdIn.readInt();
    var array:Array[String] = new Array[String](people);
    println("Enter the values : ");
    var index:Int = 0;
    var values:String = scala.io.StdIn.readLine();
    array = values.split(" ");
    var finalArray:Array[Int] = array.map(x => x.toInt);
    var count:Int = 0;
    for(i <- finalArray) {
      if (checkFactor(i)) {
        count = count + 1;
      }
    }
    println("Number of People getting discount will be : "+count);
  }
}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object TestScala {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf()
    conf.setAppName("Spark hello world")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)

    val file = sc.textFile("./src/resources/input.txt")

    val ints = file
      .map(line => line.split(" "))
      .map(x => x.map(y => y.toInt))

    println()
    println("Array of numbers")

    ints.map(s => s.mkString(" ")).foreach(println)

    println("Printing sums of array")
    val sums = ints.map(line => line.reduce((x1, x2) => x1 + x2))
    sums.foreach(println)

    println("Printing the sum of numbers of each row, which are multiples of the 5 (number %5 == 0)")
    val sumsByFive = ints.map(line => line.filter(x => x % 5 == 0).sum)
    sumsByFive.foreach(println)

    println("Maximum and minimum of each row")
    val maxs = ints.map(line => line.max)
    val mins = ints.map(line => line.min)
    mins.foreach(println)
    maxs.foreach(println)

    println("Distinct numbers in each row")
    val uniques = ints.map(line => line.distinct)
    uniques.map(s => s.mkString(" ")).foreach(println)

    //We give a try with flatMap
    val flatInts = file.flatMap(line => line.split(" ")).map(x => x.toInt)
    println("Flat maps")
    flatInts.foreach(println)

    println("Flat sum")
    println(flatInts.reduce((x1, x2) => x1 + x2))

    println("Flat mult 5")
    flatInts.filter(_ % 5 == 0).foreach(println)

    println("Max min flat")
    println(flatInts.max())
    println(flatInts.min())

    println("Flat distincts")
    flatInts.distinct().foreach(println)
  }
}

package questions

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder
      .master("local")
      .appName("main")
      .config("spark.driver.memory", "5g")
      .getOrCreate()

    val path = getClass().getResource("/allCountries.txt").toString
    val processor = new GeoProcessor(spark, path)

    //example for printing
    val filtered = processor.filterData(processor.file)

    val mostCommonWords = processor.mostCommonWords(filtered)

    mostCommonWords.take(10).foreach(println(_))

    //processor.filterElevation("FI", filtered).take(10).foreach(x => print(x + " "))

    //stop spark
    spark.stop()
  }
}

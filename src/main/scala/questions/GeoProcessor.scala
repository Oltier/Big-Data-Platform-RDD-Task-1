package questions

import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

case class HotelsDto(name: String, latitude: Double, longitude: Double, havDistance: Double = Double.MaxValue)

case class Location(longitude: Double, latitude: Double)

/** GeoProcessor provides functionalites to
  * process country/city/location data.
  * We are using data from http://download.geonames.org/export/dump/
  * which is licensed under creative commons 3.0 http://creativecommons.org/licenses/by/3.0/
  *
  * @param spark    reference to SparkSession
  * @param filePath path to file that should be modified
  */
class GeoProcessor(spark: SparkSession, filePath: String) extends Serializable {

  //read the file and create an RDD
  //DO NOT EDIT
  val file: RDD[String] = spark.sparkContext.textFile(filePath)
  val nameColumnId = 1
  val countryCodeColumnId = 8
  val demColumnId = 16
  val latitudeColumnId = 4
  val longitudeColumnId = 5

  /** filterData removes unnecessary fields and splits the data so
    * that the RDD looks like RDD(Array("<name>","<countryCode>","<dem>"),...))
    * Fields to include:
    *   - name
    *   - countryCode
    *   - dem (digital elevation model)
    *
    * @return RDD containing filtered location data. There should be an Array for each location
    */
  def filterData(data: RDD[String]): RDD[Array[String]] = {
    /* hint: you can first split each line into an array.
    * Columns are separated by tab ('\t') character.
    * Finally you should take the appropriate fields.
    * Function zipWithIndex might be useful.
    */
    data
      .map(_.split("\t"))
      .map(data => Array(data(nameColumnId), data(countryCodeColumnId), data(demColumnId)))
  }


  /** filterElevation is used to filter to given countryCode
    * and return RDD containing only elevation(dem) information
    *
    * @param countryCode code e.g(AD)
    * @param data        an RDD containing multiple Array[<name>, <countryCode>, <dem>]
    * @return RDD containing only elevation information
    */
  def filterElevation(countryCode: String, data: RDD[Array[String]]): RDD[Int] = {
    val countryCodeId = 1
    val demId = 2

    data.filter(_ (countryCodeId) == countryCode).map(_ (demId).toInt)
  }


  /** elevationAverage calculates the elevation(dem) average
    * to specific dataset.
    *
    * @param data : RDD containing only elevation information
    * @return The average elevation
    */
  def elevationAverage(data: RDD[Int]): Double = {
    data.sum / data.count
  }

  /** mostCommonWords calculates what is the most common
    * word in place names and returns an RDD[(String,Int)]
    * You can assume that words are separated by a single space ' '.
    *
    * @param data an RDD containing multiple Array[<name>, <countryCode>, <dem>]
    * @return RDD[(String,Int)] where string is the word and Int number of
    * occurrences. RDD should be in descending order (sorted by number of occurrences).
    * e.g ("hotel", 234), ("airport", 120), ("new", 12)
    */
  def mostCommonWords(data: RDD[Array[String]]): RDD[(String, Int)] = {
    val nameId = 0
    data.map(_ (nameId)).flatMap(_.split(' ')).map(word => (word, 1)).reduceByKey(_ + _).sortBy(_._2, ascending = false)
  }

  /** mostCommonCountry tells which country has the most
    * entries in geolocation data. The correct name for specific
    * countrycode can be found from countrycodes.csv.
    *
    * @param data filtered geoLocation data
    * @param path to countrycode.csv file
    * @return most common country as String e.g Finland or empty string "" if countrycodes.csv
    *         doesn't have that entry.
    */
  def mostCommonCountry(data: RDD[Array[String]], path: String): String = {
    val countryIdColumn = 1
    val countryIdsAndNames = spark.sparkContext.textFile(path).map(_.split(',')).map(keyValue => (keyValue(0), keyValue(1)))
    val countryIds = data.map(array => Array(array(countryIdColumn)))
    val mostCommonCountryId = mostCommonWords(countryIds).take(1)(0)._1
    val countryName = countryIdsAndNames.filter(_._2 == mostCommonCountryId).map(_._1)
    if (countryName.isEmpty()) {
      ""
    } else {
      countryName.first()
    }
  }

  //
  /**
    * How many hotels are within 10 km (<=10000.0) from
    * given latitude and longitude?
    * https://en.wikipedia.org/wiki/Haversine_formula
    * earth radius is 6371e3 meters.
    *
    * Location is a hotel if the name contains the word 'hotel'.
    * Don't use feature code field!
    *
    * Important
    * if you want to use helper functions, use variables as
    * functions, e.g
    * val distance = (a: Double) => {...}
    *
    * @param lat  latitude as Double
    * @param long longitude as Double
    * @return number of hotels in area
    */
  def hotelsInArea(lat: Double, long: Double): Int = {
    val maxDistanceMetres = 10000.0
    val hotelLocations = file.map(_.split("\t"))
      .map(arr => HotelsDto(arr(nameColumnId), arr(latitudeColumnId).toDouble, arr(longitudeColumnId).toDouble))
      .filter(_.name.toLowerCase.contains("hotel"))
      .map(hotel => Location(hotel.longitude, hotel.latitude))

    val centreLocation = Location(long, lat)
    val hotelsInDistance = hotelLocations
      .filter(hotelLocation => calculateDistanceInMetres(centreLocation, hotelLocation) <= maxDistanceMetres)

    hotelsInDistance.count().toInt
  }

  private def calculateDistanceInMetres(centreLocation: Location, hotelLocation: Location): Double = {
    val earthRadiusMetres = 6371e3
    val latitudeDistance = Math.toRadians(centreLocation.latitude - hotelLocation.latitude)
    val longitudeDistance = Math.toRadians(centreLocation.longitude - hotelLocation.longitude)
    val sinLatitude = Math.sin(latitudeDistance / 2)
    val sinLongitude = Math.sin(longitudeDistance / 2)
    val a = Math.pow(sinLatitude, 2) + (Math.cos(Math.toRadians(centreLocation.latitude)) *
      Math.cos(Math.toRadians(hotelLocation.latitude)) *
      Math.pow(sinLongitude, 2))
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))

    earthRadiusMetres * c
  }

  //GraphX exercises

  /**
    * Load FourSquare social graph data, create a
    * graphx graph and return it.
    * Use user id as vertex id and vertex attribute.
    * Use number of unique connections between users as edge weight.
    * E.g
    * ---------------------
    * | user_id | dest_id |
    * ---------------------
    * |    1    |    2    |
    * |    1    |    2    |
    * |    2    |    1    |
    * |    1    |    3    |
    * |    2    |    3    |
    * ---------------------
    * || ||
    * || ||
    * \   /
    * \ /
    * +
    *
    * _ 3 _
    * /' '\
    * (1)  (1)
    * /      \
    * 1--(2)--->2
    * \       /
    * \-(1)-/
    *
    * Hints:
    *  - Regex is extremely useful when parsing the data in this case.
    *  - http://spark.apache.org/docs/latest/graphx-programming-guide.html
    *
    * @param path to file. You can find the dataset
    *             from the resources folder
    * @return graphx graph
    *
    */
  def loadSocial(path: String): Graph[Int, Int] = {
    ???
  }

  /**
    * Which user has the most outward connections.
    *
    * @param graph graphx graph containing the data
    * @return vertex_id as Int
    */
  def mostActiveUser(graph: Graph[Int, Int]): Int = {
    ???
  }

  /**
    * Which user has the highest pageRank.
    * https://en.wikipedia.org/wiki/PageRank
    *
    * @param graph graphx graph containing the data
    * @return user with highest pageRank
    */
  def pageRankHighest(graph: Graph[Int, Int]): Int = {
    ???
  }
}

/**
  *
  * Change the student id
  */
object GeoProcessor {
  val studentId = "728272"
}
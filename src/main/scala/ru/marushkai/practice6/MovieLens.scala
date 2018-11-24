package ru.marushkai.practice6

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession


object MovieLens {

  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)

  case class Movie(movieId: Int, movieName: String, rating: Float)

  case class UserMovie(userId: Int, movieId: Int)

  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val personalRatingsFile = "./src/resources/movielens/medium/personalRatings.txt"
    val moviesFile = "./src/resources/movielens/medium/movies.dat"
    val ratingsFile = "./src/resources/movielens/medium/ratings.dat"

//    val personalRatingsFile = "hdfs://10.8.51.6:9000/nikita/movielens/medium/personalRatings.txt"
//    val moviesFile = "hdfs://10.8.51.6:9000/nikita/movielens/medium/movies.dat"
//    val ratingsFile = "hdfs://10.8.51.6:9000/nikita/movielens/medium/ratings.dat"

    val sparkSession = SparkSession
      .builder()
      .appName("MovieLens Recommendation system")
      .master("local[2]")
      .config("spark.executor.memory", "10g")
      .config("spark.driver.memory", "10g")
      .config("spark.driver.maxResultSize","10g")
      .config("spark.logConf","true")
      .getOrCreate()

    import sparkSession.implicits._

    //Create personal ratings df
    val personalRatingsDf = sparkSession.read.textFile(personalRatingsFile)
      .map(parseRating)
      .toDF()

    //Create ratings df
    val generalRatingsDf = sparkSession.read.textFile(ratingsFile)
        .map(parseRating)
        .toDF()

    //Create movies df
    val moviesRDD = sparkSession.read.textFile(moviesFile).map { line =>
      val fields = line.split("::")
      (fields(0).toInt, fields(1))
    }

    generalRatingsDf.show(10)
    personalRatingsDf.show(10)


    val numRatings = generalRatingsDf.distinct().count()
    val numUsers = generalRatingsDf.select("userId").distinct().count()
    val numMovies = moviesRDD.count()

    // Get movies dictionary
    val movies = moviesRDD.collect.toMap

    val allMoviesDf = movies.map(it => UserMovie(0, it._1)).toSeq.toDF()

    allMoviesDf.show(10)

    println("Got " + numRatings + " ratings from "
      + numUsers + " users on " + numMovies + " movies.")

    val ratingWithMyRats = generalRatingsDf.union(personalRatingsDf)

    val Array(training, test) = ratingWithMyRats.randomSplit(Array(0.75, 0.25))

    training.show(10)
    test.show(10)

//    movies.foreach(print)

    val als = new ALS()
      .setMaxIter(10)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")

    val model = als.fit(training)

//    val actualTest = test.drop(test.col("rating"))

    val predictions = model.transform(test).na.fill(0)

    predictions.show()

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")

    val rmse = evaluator.evaluate(predictions)

    println(s"Root-mean-square error = $rmse")

    val myPredictions = model.transform(personalRatingsDf.drop(personalRatingsDf.col("rating"))).na.drop

    myPredictions.show(10)

    val myMovies = myPredictions.map(r => Movie(r.getInt(1), movies(r.getInt(1)), r.getFloat(3))).toDF

    myMovies.show(100)


    val allMoviesPredictions = model.transform(allMoviesDf).na.fill(0)

//    allMoviesPredictions.show(100)

    val allMoviesToShow = allMoviesPredictions.map(r => Movie(r.getInt(1), movies(r.getInt(1)), r.getFloat(2))).toDF()

    allMoviesToShow.orderBy(allMoviesToShow.col("rating").desc).show(100)
  }
}

package ru.marushkai.practice4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object Diabetes {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val fileName = "hdfs://10.8.51.6:9000/nikita/diabets.csv"

    val sparkSession = SparkSession
      .builder()
      .appName("Diabetes logistic regression")
      .master("spark://10.8.51.6:7077")
      .config("spark.executor.memory", "10g")
      .config("spark.driver.memory", "10g")
      .config("spark.driver.maxResultSize","10g")
      .config("spark.logConf","true")
      .getOrCreate()

    import sparkSession.implicits._

    val df = sparkSession.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .csv(fileName)

    df.show(10)

    df.printSchema()

    //Feature Extraction

    val DFAssembler = new VectorAssembler().

      setInputCols(Array(
        "pregnancy", "glucose", "arterial pressure",
        "thickness of TC", "insulin", "body mass index",
        "heredity", "age"))
      .setOutputCol("features")

    val features = DFAssembler.transform(df)
    features.show(10)

    val labeledTransformer = new StringIndexer().setInputCol("diabet").setOutputCol("label")

    val labeledFeatures = labeledTransformer.fit(features).transform(features)

    labeledFeatures.show(10)

    // Split data into training (60%) and test (40%)

    val splits = labeledFeatures.randomSplit(Array(0.6, 0.4), seed = 11L)
    val trainingData = splits(0)
    val testData = splits(1)

    val evaluator = new BinaryClassificationEvaluator()
          .setLabelCol("label")
          .setRawPredictionCol("rawPrediction")
          .setMetricName("areaUnderROC")

    val results = new mutable.HashMap[Double, Double]()
    for (i <- 1 to 5) {
      var lr = new LogisticRegression()
        .setMaxIter(500)
        .setRegParam(i * 0.01)
        .setElasticNetParam(i * 0.01)

      var model = lr.fit(trainingData)

      var predictions = model.transform(testData)

      val accuracy = evaluator.evaluate(predictions)

      results.put(i * 0.01, accuracy)
    }

    println(results.maxBy(_._2))
//    val lr = new LogisticRegression()
//      .setMaxIter(1000)
//      .setRegParam(0.1)
//      .setElasticNetParam(0.3)
//
//    //Train Model
//    val model = lr.fit(trainingData)
//
//    println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")
//
//    //Make predictions on test data
//    val predictions = model.transform(testData)
//
//    println("Predictions:\n")
//
//    predictions.orderBy($"prediction".desc).show(100)
//
//    //Evaluate the precision and recall
//    val countProve = predictions.where("label == prediction").count()
//    val count = predictions.count()
//
//    println(s"Count of true predictions: $countProve Total Count: $count")
//
//    val evaluator = new BinaryClassificationEvaluator()
//      .setLabelCol("label")
//      .setRawPredictionCol("rawPrediction")
//      .setMetricName("areaUnderROC")
//
//    val accuracy = evaluator.evaluate(predictions)
//
//    println(s"Accuracy = ${accuracy}")

  }
}

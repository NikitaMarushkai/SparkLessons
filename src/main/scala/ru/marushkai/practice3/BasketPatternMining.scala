package ru.marushkai.practice3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class ItemsRow(InvoiceNo: String, StockCode: String, Description: String)

object BasketPatternMining {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val fileName = "./src/resources/Online_Retail.csv"
    val sparkSession = SparkSession
      .builder()
      .appName("Basket pattern mining")
      .master("local[2]")
      .getOrCreate()

    import sparkSession.implicits._

    val df = sparkSession.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("nullValue", "")
      .option("timestampFormat", "dd.MM.yyyy HH:mm")
      .csv(fileName)

    df.show(10)

    val ds: Dataset[ItemsRow] = df.as[ItemsRow]

    val readyDf = ds.rdd.groupBy(x => x.InvoiceNo).map(x => (x._1, x._2.map(_.StockCode).toArray.distinct)).toDF("InvoiceNo", "StockCodes").limit(1000)

    val fpgrowth = new FPGrowth()
      .setItemsCol("StockCodes")
      .setMinSupport(0.02)
      .setMinConfidence(0.01)

    val model = fpgrowth.fit(readyDf)

    model.freqItemsets.show()

    model.associationRules.orderBy($"confidence".asc)show()


    model.transform(readyDf).show()
  }
}

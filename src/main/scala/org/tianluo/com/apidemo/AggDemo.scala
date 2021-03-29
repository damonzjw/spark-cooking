package org.tianluo.com.apidemo

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.tianluo.com.util.SparkUtils

object AggDemo {
  val spark = SparkUtils.getSpark()

  val dataPath = "F:\\03_github\\Spark-The-Definitive-Guide\\data\\retail-data\\all\\online-retail-dataset.csv"

  val df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(dataPath)
    .cache()

  val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "dd/m/yyyy H:mm"))

  val dfNoNull = dfWithDate.drop()

  def main(args: Array[String]): Unit = {
    rollUp
  }

  def rollUp = {
    import spark.implicits._

    val rolledUpDF = dfNoNull.rollup("date", "country")
      .agg(sum("Quantity"))
      .selectExpr("date", "country", "`sum(Quantity)` as total_quantity")
      .orderBy("date")
    rolledUpDF.show()
    rolledUpDF.where("Country IS NULL").show()
    rolledUpDF.where("Date IS NULL").show()
    rolledUpDF
      .where("date='2010-01-12'")
      .select('date, 'country, 'total_quantity,
        rank().over(Window.orderBy("total_quantity")),
        dense_rank().over(Window.orderBy("total_quantity"))
      ).show(100)
  }

}

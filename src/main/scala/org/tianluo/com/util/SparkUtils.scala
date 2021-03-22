package util

import org.apache.spark.sql.SparkSession

object SparkUtils {
  def main(args: Array[String]): Unit = {
    val spark = getSpark()
    spark.catalog.listTables().show()
  }

  def getSpark(is_local: Boolean = true): SparkSession = {
    val master = if (is_local) "local[2]" else "yarn"
    SparkSession
      .builder()
      .master(master)
      .getOrCreate()
  }
}

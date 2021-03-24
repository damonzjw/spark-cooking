package org.tianluo.com.apidemo

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.tianluo.com.model.NumData
import org.tianluo.com.util.SparkUtils

/*
* 练习spark窗口函数的应用
* 1.分析函数
* 2.聚合函数
*
* */
object OverDemo {
  val spark = SparkUtils.getSpark()

  def main(args: Array[String]): Unit = {
    aggFunction()
  }

  /*
  *   聚合函数
  *   sum()over() count()over() agg()over()
  * */
  def aggFunction(): Unit = {
    import spark.implicits._


    /*  分区总和
    *
    *   over(partition by id )
    * */
    val windowPartition = Window.partitionBy('id)


    /*  累计求和
    *
    *   over(partition by id order by date)
    * */
    val windowAcc = Window.partitionBy('id).orderBy('date)


    val list_data = List(
      NumData(1, 2, "2020-01-01"),
      NumData(1, 3, "2020-01-02"),
      NumData(1, 4, "2020-01-03"),
      NumData(1, 6, "2020-01-04"),
      NumData(1, 7, "2020-01-05"),
      NumData(1, 1, "2020-01-06"),
      NumData(2, 2, "2020-01-01"),
      NumData(2, 3, "2020-01-02"),
      NumData(2, 4, "2020-01-03"),
      NumData(2, 2, "2020-01-04"),
      NumData(2, 1, "2020-01-05"),
      NumData(2, 6, "2020-01-06")
    )

    val ds: Dataset[NumData] = spark.createDataset(list_data)
    val sumData = ds
      .select(
        'id,
        'date,
        'num,
        sum('num).over().as("total_num"),
        sum('num).over(windowPartition).as("partition_total_num"),
        sum('num).over(windowAcc).as("partition_order_total_num"))
      .orderBy('id, 'date)
    sumData.show()
  }

  /*
  * 分析函数
  *
  * */
}


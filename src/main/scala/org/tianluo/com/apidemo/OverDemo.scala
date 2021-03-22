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

    /*  累加前3天
    *
    *   over(partition by id order by date rows between 3 and current row)
    * */
    val window_3 = Window.partitionBy('id).orderBy('date).rowsBetween(-3, 0)

    /*  累加前3天，后5天
    *
    *   over(partition by id order by date rows between 3 and 5)
    * */
    val window_3_5 = Window.partitionBy('id).orderBy('date).rowsBetween(-3, 5)


    /*  累加昨日、今日、明日
    *
    *   over(partition by id order by date rows between -1 and 1)
    * */
    val window_1_1 = Window.partitionBy('id).orderBy('date).rowsBetween(-1, 1)

    /*  累加分区内所有行
    *
    *   over(partition by id order by date rows between unbounded preceding and unbounded following)
    * */
    val windowAll = Window.partitionBy('id).orderBy('date).rowsBetween(Long.MinValue, Long.MaxValue)

    /*  分区总和
    *
    *   over(partition by id )
    * */
    val windowPartition = Window.partitionBy('id)
    /*  累计求和
    *
    * */

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
    ds.select(
      'id,
      'date,
      'num,
      sum('num).over().as("total_num"),
      sum('num).over(windowPartition).as("partition_total_num"),
      sum('num).over(Window.partitionBy('id).orderBy('date)).as("partition_order_total_num")
    ).orderBy('id, 'date).show()
  }

  /*
  * 分析函数
  *
  * */
}


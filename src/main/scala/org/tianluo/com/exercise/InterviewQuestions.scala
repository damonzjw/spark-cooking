package org.tianluo.com.exercise

import org.tianluo.com.util.SparkUtils

object InterviewQuestions {

  val spark = SparkUtils.getSpark()


  def main(args: Array[String]): Unit = {
    sortArry()
  }

  def sortArry() = {
    import spark.implicits._
    List(("a", "1|2|3"), ("b", "2|3"), ("c", "1"))
      .toDF("class", "codes").createOrReplaceTempView("t1")

    List((1, "Tom"), (2, "Jim"), (3, "Herry"))
      .toDF("code", "name")
      .createOrReplaceTempView("t2")

    spark.sql(
      """
        |  SELECT class, code
        |  FROM t1
        |  lateral view explode(sort_array(split(codes,"\\|")))  t as code
        |""".stripMargin).show()
  }

  def crossJoin = {
    import spark.implicits._
    val data = List(
      ("001", "a"),
      ("001", "b"),
      ("001", "d"),
      ("001", "e"),
      ("001", "f"),
      ("002", "b"),
      ("002", "c"),
      ("002", "d"),
      ("002", "e"),
      ("002", "f"),
      ("003", "b"),
      ("003", "d"),
      ("003", "e"),
      ("003", "f"),
      ("004", "c"),
      ("004", "e"),
      ("004", "f")
    ).toDF("ip", "user")

    data.createOrReplaceTempView("t1")
    spark.sql(
      """
        |select t1.ip,t1.user,t2.user from t1 cross join (select ip,user from t1) t2 on t2.ip=t1.ip
        |where t1.user<t2.user
        |
        |""".stripMargin).show(100)
  }

  def overExer={


  }
}

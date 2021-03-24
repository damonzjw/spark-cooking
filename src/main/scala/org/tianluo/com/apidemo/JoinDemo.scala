package org.tianluo.com.apidemo

import org.apache.spark.sql.Column
import org.tianluo.com.util.SparkUtils

object JoinDemo {
  val spark = SparkUtils.getSpark()

  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val person = Seq(
      (0, "Bill Chambers", 0, Seq(100)),
      (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
      (2, "Michael Armbrust", 1, Seq(250, 100))
    )
      .toDF("id", "name", "graduate_program", "spark_status")

    val graduateProgram = Seq(
      (0, "Masters", "School of Information", "UC Berkeley"),
      (2, "Masters", "EECS", "UC Berkeley"),
      (1, "Ph.D.", "EECS", "UC Berkeley"))
      .toDF("id", "degree", "department", "school")

    val sparkStatus = Seq(
      (500, "Vice President"),
      (250, "PMC Member"),
      (100, "Contributor"))
      .toDF("id", "status")

    person.createOrReplaceTempView("person")
    graduateProgram.createOrReplaceTempView("graduateProgram")
    sparkStatus.createOrReplaceTempView("sparkStatus")

    // Inner Join
    val joinExpression: Column = person.col("graduate_program") === graduateProgram.col("id")
    val wrongJoinExpression: Column = person.col("name") === graduateProgram.col("school")

    var joinType = "inner"
    println(s"======================$joinType")
    person.join(graduateProgram, joinExpression, joinType).show()

    //  保留左右两边的数据
    joinType = "outer"
    println(s"======================$joinType")
    person.join(graduateProgram, joinExpression, joinType).show()

    // 保留左表的所有数据，若右表没有匹配的以null填充
    joinType = "left_outer"
    println(s"======================$joinType")
    graduateProgram.join(person, joinExpression, joinType).show()

    // 保留右表的所有数据，若左表没有匹配的以null填充
    joinType = "right_outer"
    println(s"======================$joinType")
    person.join(graduateProgram, joinExpression, joinType).show()

    // 只返回左边key的数据
    joinType = "left_semi"
    println(s"======================$joinType")
    graduateProgram.join(person, joinExpression, joinType).show()

    // 只返回左边Key在右边中没有的数据
    joinType = "left_anti"
    println(s"======================$joinType")
    graduateProgram.join(person, joinExpression, joinType).show()

    //
    joinType = "cross"
    println(s"======================$joinType")
    graduateProgram.join(person, joinExpression, joinType).show()

    import org.apache.spark.sql.functions.expr
    println("======================complexJoin")
    person
      .withColumnRenamed("id", "personId")
      .join(sparkStatus, expr("array_contains(spark_status, id)"))
      .show()
  }
}

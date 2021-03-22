# Spark-Cooking

> 该项目用于总结spark数据开发的代码框架和应用案例，同时包含spark API的使用和练习

## 窗口函数应用

~~~scala
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
~~~


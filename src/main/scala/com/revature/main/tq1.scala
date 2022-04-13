package com.revature.main

import com.revature.main.Project2.{saveDataFrameAsCSV, spark}
import org.apache.spark.sql.functions.{col, desc, not, when}

object tq1 {

  def deathVSpopulation(): Unit =
  {
    //Reading File Data as DataFrame
    var startTime = System.currentTimeMillis()
    var df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("time_series_covid_19_deaths_US.csv")
    var transTime = (System.currentTimeMillis() - startTime) / 1000d
    //Converting related values into Double and Changing the Column Names
    startTime = System.currentTimeMillis()
    df = df.withColumnRenamed("Admin2", "County")
      .withColumnRenamed("Province_state", "State")
      .withColumnRenamed("5/2/21", "Deaths")
      .withColumn("Population", col("Population").cast("double"))
      .withColumn("Deaths", col("Deaths").cast("double"))
    df = df.select("State","Population","Deaths")
      .orderBy("State")
    //Adding up the Columns, and Printing the Result in a New Column
    startTime = System.currentTimeMillis()
    df = df.groupBy(col("State"))
      .sum("Population", "Deaths")
    df = df.withColumn("fraction", col("sum(Deaths)")/col("sum(Population)"))
      .withColumn(  "Percent", col("fraction") * 100 )
      .drop("fraction")
    df = df.withColumn("Percent", when(col("Percent").isNull,0.0) otherwise (col("Percent")))
      .filter(not(df("Percent")===0.0))
    //Shows Death percentage Ascending
    val ascending = df.orderBy("Percent")
    saveDataFrameAsCSV(ascending.limit(10),"bestStates.csv")
    ascending.show()
    //Shows Death percentage Descending
    val descending =df.orderBy(desc("Percent"))
    saveDataFrameAsCSV(descending.limit(10),"worstStates.csv")
    descending.show()
    transTime = (System.currentTimeMillis() - startTime) / 1000d
    println(s"Unique locations: ${df.count()}")
    println(s"Transaction time: $transTime seconds\n")
  }

}

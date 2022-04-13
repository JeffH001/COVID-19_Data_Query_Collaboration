package com.revature.main

import com.revature.main.Project2.{saveDataFrameAsCSV, spark}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, isnull, month, not, to_date, udf, when, year}

object dq1 extends Serializable {
  /**
    * Compare state population to number of deaths from April 2020 - April 2021
    */
  def statePopulationVsDeath(): Unit =
  {
    //Showing the entire file
    println("Showing time_series_covid_19_deaths_US.csv")
    var startTime = System.currentTimeMillis()
    var df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("time_series_covid_19_deaths_US.csv")
    var transTime = (System.currentTimeMillis() - startTime) / 1000d

    println("Converting INTO Double")
    startTime = System.currentTimeMillis()
    df = df.withColumnRenamed("Admin2", "County")
      .withColumnRenamed("Province_state", "State")
      .withColumnRenamed("5/2/21", "Deaths")
      .withColumn("Population", col("Population").cast("double"))
      .withColumn("Deaths", col("Deaths").cast("double"))
    df = df.select("State","Population","Deaths")
      .orderBy("State")
    println("Showing Only Relevant Columns")
    startTime = System.currentTimeMillis()
    df = df.groupBy(col("State"))
      .sum("Population", "Deaths")

    //Shows Population Ascending
    val ascending = df.orderBy( "sum(Population)" )
    saveDataFrameAsCSV(ascending.limit(10),"bottomTenStates.csv")

    //Shows Population Descending
    val descending =df.orderBy(desc("sum(Population)"))
    saveDataFrameAsCSV(descending.limit(10),"topTenStates.csv")
    df.show(false)

  }

}

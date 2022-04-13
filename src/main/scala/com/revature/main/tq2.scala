package com.revature.main

import com.revature.main.Project2.{saveDataFrameAsCSV, spark}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, not, regexp_replace, when}

object tq2 {
  var joined:DataFrame=null
  var tTS:DataFrame=null
  var t2020:DataFrame=null
  /**
    * This is a "dummy" query which you can use as example code.
    */

  def deathBYcovid(): Unit =  {
    println("Dataframe read from CSV:")
    //var startTime = System.currentTimeMillis()
    tTS = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
      .load("time_series_covid_19_deaths_US.csv")
    //Converting related values into Double and Changing the Column Names
    tTS = tTS.withColumnRenamed("Province_state", "State")
      .withColumnRenamed("12/31/20", "Deaths")
      .withColumn("Deaths", col("Deaths").cast("double"))
    tTS = tTS.select("State","Deaths")
      .groupBy("State")
      .sum("Deaths")
      .orderBy("State")
    tTS=tTS.withColumnRenamed("sum(Deaths)","Covid_Deaths")
    tTS.show(3)


    t2020 = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
      .load("usDeath2020.csv")
    t2020 = t2020.withColumnRenamed("State/Territory", "State")
      //.withColumn("Deaths", col("Deaths").cast("double"))
    .withColumn("Deaths", regexp_replace(col("Deaths"),",",""))
    t2020 = t2020.select(col("State"),col("Deaths").as("2020_Deaths"))
      .withColumn("2020_Deaths", col("2020_Deaths").cast("double"))
    t2020.show(3)

    joined = tTS.join(t2020, usingColumns = Seq("State"))
      .where(tTS.col("State")===t2020.col("State"))
      .withColumn("fraction",col("Covid_Deaths")/col("2020_Deaths"))
      .withColumn("CovidDeathPercentage",col("fraction")/100)
      .drop("fraction")
      .withColumn("CovidDeathPercentage", when(col("CovidDeathPercentage").isNull,0.0) otherwise (col("CovidDeathPercentage")))
      .filter(not(joined("CovidDeathPercentage")===0.0))
    joined.show(50)

    //heres a note I made







//    var transTime = (System.currentTimeMillis() - startTime) / 1000d
//    println(s"Transaction time: $transTime seconds")

  }
}

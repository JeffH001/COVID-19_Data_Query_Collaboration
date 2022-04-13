package com.revature.main

import com.revature.main.Project2.{saveDataFrameAsCSV, spark}
import org.apache.spark.sql.functions.{col, not, when}


object eq1 {
  /**
    *Fetches the deaths per state, prints to CSV.
    *
    *
    */

  def getDeathsPerState (): Unit = {
    // Read "time_series_covid_19_confirmed_US.csv" data as a dataframe
    println("Reading US COVID deaths by county into a dataframe from CSV:")
    var startTime = System.currentTimeMillis()
    var df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("time_series_covid_19_deaths_US.csv")
    //var states = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("apportionment.csv")
    var transTime = (System.currentTimeMillis() - startTime) / 1000d
    println(s"Table length: ${df.count()}")
    println(s"Transaction time: $transTime seconds")
    df = df.withColumnRenamed("5/2/21", "Deaths")
      .withColumnRenamed("Province_State", "Name")
      .withColumn("Deaths", col("Deaths").cast("double"))
      .withColumn("Population", col("Population").cast("double"))
    // Sums the population and name columns, sorts by name
    df = df.groupBy(col("Name"))
      .sum("Population", "Deaths")
      .orderBy("Name")
    df = df.withColumn("fraction", col("sum(Deaths)")/col("sum(Population)"))
      .withColumn(  "Percent", col("fraction") * 100 )
      .drop("fraction")
    df = df.withColumn("Percent", when(col("Percent").isNull,0.0) otherwise (col("Percent")))
      .filter(not(df("Percent")===0.0))
    df.show(100)
    saveDataFrameAsCSV(df, "statePopDeathsPercent2.csv")

  }


  /**
    * This loads the 2020 census population data into a CSV.
    */
  private def populationDensity (): Unit = {
    // Read "covid_19_data.csv" data as a dataframe
    println("Dataframe read from CSV:")
    var startTime = System.currentTimeMillis()
    var popDensity = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("apportionment.csv")
    var transTime = (System.currentTimeMillis() - startTime) / 1000d
    popDensity.printSchema()
    println("Modified dataframe:")
    startTime = System.currentTimeMillis()
    println("Table filled from dataframe:")
    startTime = System.currentTimeMillis()
    spark.sql("DROP TABLE IF EXISTS popdensitytable2020")
    popDensity.createOrReplaceTempView("popdensitytable")  // Copies the dataframe into a view as "temptable"
    spark.sql("CREATE TABLE popdensitytable2020 AS SELECT * FROM popdensitytable WHERE year = 2020")  // Loads the data into the table from the view
    spark.catalog.dropTempView("temptable")  // View no longer needed
    transTime = (System.currentTimeMillis() - startTime) / 1000d
    var tabledat = spark.sql("SELECT * FROM popdensitytable2020 WHERE `Resident Population Density` IS NOT NULL").orderBy("`Name`")
    tabledat.show(false)
    println(s"Transaction time: $transTime seconds\n")

    // Write the data out as a file to be used for visualization
    println("Save unique locations as file...")
    startTime = System.currentTimeMillis()
    val fname = saveDataFrameAsCSV(tabledat, "populationDensity2020.csv")
    transTime = (System.currentTimeMillis() - startTime) / 1000d
    println(s"Saved as: $fname")
    println(s"Save completed in $transTime seconds.\n")
  }

}

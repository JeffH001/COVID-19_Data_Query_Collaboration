package com.revature.main

import com.revature.main.Project2.{saveDataFrameAsCSV, spark}
import org.apache.spark.sql.functions.{col, date_format, not, to_timestamp, when}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}


object eq2 {
  /**
    *Fetches the deaths per state, prints to CSV.
    *
    *
    */

  def getTotalDeathsPerState2019  (): Unit = {
    // Read "time_series_covid_19_confirmed_US.csv" data as a dataframe
    println("Reading US total deaths by state into a dataframe from CSV:")
    var startTime = System.currentTimeMillis()
    var df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("WeeklyDeaths2014-2019.csv")
    df.printSchema()
    var transTime = (System.currentTimeMillis() - startTime) / 1000d
    println(s"Table length: ${df.count()}")
    println(s"Transaction time: $transTime seconds")
    df = df.withColumnRenamed("All  Cause", "Deaths") //CDC decided to give "All Cause" two spaces.
      .withColumnRenamed("Jurisdiction of Occurrence", "Name")
      .withColumnRenamed("MMWR Year", "Year")
      .withColumnRenamed("MMWR Week", "Week")
      .withColumn("Deaths", col("Deaths").cast("double"))
    df = df.filter(df("Year") === "2019")
      .where("Name == 'Texas'")
    df = df.select("Deaths", "Name", "Week")
    val pivotDF = df.groupBy("Name").pivot("Week").sum("Deaths").orderBy("Name")
    pivotDF.show(100)
    //saveDataFrameAsCSV(df, "pivotedStateDeathsByWeek2019NonAggregate.csv")
    df.unpersist()


    var df2 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("WeeklyDeaths2020.csv")
    df2.printSchema()
    println(s"Table length: ${df2.count()}")
    println(s"Transaction time: $transTime seconds")
    df2 = df2.withColumnRenamed("All Cause", "Deaths") //This one has only one space for some reason.
      .withColumnRenamed("Jurisdiction of Occurrence", "Name")
      .withColumnRenamed("MMWR Year", "Year")
      .withColumnRenamed("MMWR Week", "Week")
      .withColumn("Deaths", col("Deaths").cast("double"))
      .where("Name == 'Texas'")
    df2 = df2.select("Deaths", "Name", "Week")
    val pivotDF2 = df2.groupBy("Name").pivot("Week").sum("Deaths").orderBy("Name")
    pivotDF2.show(100)
    //saveDataFrameAsCSV(df2, "pivotedStateDeathsByWeek2020NonAggregate.csv")
    df2.unpersist()

    var df3 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("time_series_covid_19_deaths_US.csv")
    df3 = df3.withColumnRenamed("Admin2", "County")
      .withColumnRenamed("Province_state", "State")
      .where("State != 'American Samoa' AND " +
        "State != 'Diamond Princess' AND " +
        "State != 'Grand Princess' AND " +
        "State != 'Guam' AND " +
        "State != 'Northern Mariana Islands' AND " +
        "State != 'Puerto Rico' AND " +
        "State != 'Virgin Islands'")
    df3 = df3.withColumnRenamed("1/25/20", "4")
      .withColumnRenamed("2/1/20", "5")
      .withColumnRenamed("2/8/20", "6")
      .withColumnRenamed("2/15/20", "7")
      .withColumnRenamed("2/22/20", "8")
      .withColumnRenamed("2/29/20", "9")
      .withColumnRenamed("3/7/20", "10")
      .withColumnRenamed("3/14/20", "11")
      .withColumnRenamed("3/21/20", "12")
      .withColumnRenamed("3/28/20", "13")
      .withColumnRenamed("4/4/20", "14")
      .withColumnRenamed("4/11/20", "15")
      .withColumnRenamed("4/18/20", "16")
      .withColumnRenamed("4/25/20", "17")
      .withColumnRenamed("5/2/20", "18")
      .withColumnRenamed("5/9/20", "19")
      .withColumnRenamed("5/16/20", "20")
      .withColumnRenamed("5/23/20", "21")
      .withColumnRenamed("5/30/20", "22")
      .withColumnRenamed("6/6/20", "23")
      .withColumnRenamed("6/13/20", "24")
      .withColumnRenamed("6/20/20", "25")
      .withColumnRenamed("6/27/20", "26")
      .withColumnRenamed("7/4/20", "27")
      .withColumnRenamed("7/11/20", "28")
      .withColumnRenamed("7/18/20", "29")
      .withColumnRenamed("7/25/20", "30")
      .withColumnRenamed("8/1/20", "31")
      .withColumnRenamed("8/8/20", "32")
      .withColumnRenamed("8/15/20", "33")
      .withColumnRenamed("8/22/20", "34")
      .withColumnRenamed("8/29/20", "35")
      .withColumnRenamed("9/5/20", "36")
      .withColumnRenamed("9/12/20", "37")
      .withColumnRenamed("9/19/20", "38")
      .withColumnRenamed("9/26/20", "39")
      .withColumnRenamed("10/3/20", "40")
      .withColumnRenamed("10/10/20", "41")
      .withColumnRenamed("10/17/20", "42")
      .withColumnRenamed("10/24/20", "43")
      .withColumnRenamed("10/31/20", "44")
      .withColumnRenamed("11/7/20", "45")
      .withColumnRenamed("11/14/20", "46")
      .withColumnRenamed("11/21/20", "47")
      .withColumnRenamed("11/28/20", "48")
      .withColumnRenamed("12/5/20", "49")
      .withColumnRenamed("12/12/20", "50")
      .withColumnRenamed("12/19/20", "51")
      .withColumnRenamed("12/26/20", "52")

    df3 = df3.groupBy(col("State"))
      .sum("Population", "4","5", "6","7", "8","9", "10",
        "11", "12","13", "14","15", "16","17", "18", "19", "20","21", "22","23", "24",
        "25", "26","27", "28","29", "30","31", "32","33", "34","35", "36","37", "38",
        "39", "40","41", "42","43", "44","45", "46","47", "48","49", "50","51", "52")

    //IMPORTANT This must be mapped in such a manner that if week > 4 (or 1, or whatever's the start)
    //it subtracts the week's death value from the previous week's death value
    //So that it becomes non-aggregate to match the other CDC data.
    df3.show(100)
    // saveDataFrameAsCSV(df3, "officialCovidDeathsByWeek2020.csv")


    /*

    var df3 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("time_series_covid_19_deaths_US.csv")
    	 df3 = df3.withColumnRenamed("Admin2", "County")
			.withColumnRenamed("Province_state", "State")
			//.withColumnRenamed("5/2/21", "Deaths")
    df3 = df3.withColumnRenamed("1/25/20", "Week 4")
         .withColumnRenamed("2/1/20", "Week 5")
         .withColumnRenamed("2/8/20", "Week 6")
         .withColumnRenamed("2/15/20", "Week 7")
         .withColumnRenamed("2/22/20", "Week 8")
         .withColumnRenamed("2/29/20", "Week 9")
         .withColumnRenamed("3/7/20", "Week 10")
         .withColumnRenamed("3/14/20", "Week 11")
         .withColumnRenamed("3/21/20", "Week 12")
         .withColumnRenamed("3/28/20", "Week 13")
         .withColumnRenamed("4/4/20", "Week 14")
         .withColumnRenamed("4/11/20", "Week 15")
         .withColumnRenamed("4/18/20", "Week 16")
         .withColumnRenamed("4/25/20", "Week 17")
         .withColumnRenamed("5/2/20", "Week 18")
         .withColumnRenamed("5/9/20", "Week 19")
         .withColumnRenamed("5/16/20", "Week 20")
         .withColumnRenamed("5/23/20", "Week 21")
         .withColumnRenamed("5/30/20", "Week 22")
         .withColumnRenamed("6/6/20", "Week 23")
         .withColumnRenamed("6/13/20", "Week 24")
         .withColumnRenamed("6/20/20", "Week 25")
         .withColumnRenamed("6/27/20", "Week 26")
      .withColumnRenamed("7/4/20", "Week 27")
      .withColumnRenamed("7/11/20", "Week 28")
      .withColumnRenamed("7/18/20", "Week 29")
      .withColumnRenamed("7/25/20", "Week 30")
      .withColumnRenamed("8/1/20", "Week 31")
      .withColumnRenamed("8/8/20", "Week 32")
      .withColumnRenamed("8/15/20", "Week 33")
      .withColumnRenamed("8/22/20", "Week 34")
      .withColumnRenamed("8/29/20", "Week 35")
      .withColumnRenamed("9/5/20", "Week 36")
      .withColumnRenamed("9/12/20", "Week 37")
      .withColumnRenamed("9/19/20", "Week 38")
      .withColumnRenamed("9/26/20", "Week 39")
      .withColumnRenamed("10/3/20", "Week 40")
      .withColumnRenamed("10/10/20", "Week 41")
      .withColumnRenamed("10/17/20", "Week 42")
      .withColumnRenamed("10/24/20", "Week 43")
      .withColumnRenamed("10/31/20", "Week 44")
      .withColumnRenamed("11/7/20", "Week 45")
      .withColumnRenamed("11/14/20", "Week 46")
      .withColumnRenamed("11/21/20", "Week 47")
      .withColumnRenamed("11/28/20", "Week 48")
      .withColumnRenamed("12/5/20", "Week 49")
      .withColumnRenamed("12/12/20", "Week 50")
      .withColumnRenamed("12/19/20", "Week 51")
      .withColumnRenamed("12/26/20", "Week 52")
    df3 = df3.groupBy(col("State"))
      .sum("Population", "Week 4","Week 5", "Week 6","Week 7", "Week 8","Week 9", "Week 10",
        "Week 11", "Week 12","Week 13", "Week 14","Week 15", "Week 16","Week 17", "Week 18", "Week 19", "Week 20","Week 21", "Week 22","Week 23", "Week 24",
        "Week 25", "Week 26","Week 27", "Week 28","Week 29", "Week 30","Week 31", "Week 32","Week 33", "Week 34","Week 35", "Week 36","Week 37", "Week 38",
        "Week 39", "Week 40","Week 41", "Week 42","Week 43", "Week 44","Week 45", "Week 46","Week 47", "Week 48","Week 49", "Week 50","Week 51", "Week 52")


		//	.withColumn("Population", col("Population").cast("double"))
		//	.withColumn("Deaths", col("Deaths").cast("double"))

    df3.show(100)
    saveDataFrameAsCSV(df, "statePopDeaths2020ByWeek.csv")
*/
  }

}

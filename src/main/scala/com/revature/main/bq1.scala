package com.revature.main

import com.revature.main.Project2.{saveDataFrameAsCSV, spark}
import com.revature.main.bq1.FORCE_TIMER_PRINT
import com.revature.main.mySparkUtils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, isnull, lag, lit, month, not, regexp_replace, to_date, when, year}



object bq1 {
    //Constant to switch csv write on/off for debug purposes
    val WRITE_TO_CSV_ON:Boolean=true
    //Constant to force Timers to print for debug purposes
    val FORCE_TIMER_PRINT:Boolean=false

    var tempsHistAvg:DataFrame=null
    var covidAvgDaily:DataFrame=null
    var populationNormalization:DataFrame=null
    var joined:DataFrame=null

  /**
    * Creates a .csv file entitled '30yrAvgTempByCountryByMonth.csv', that contains historical average (1982-2012) temperature of country by month
    */
  def normalizePopulation(): Unit = {
    // Read "covid_19_data.csv" data as a dataframe
    println("Dataframe read from CSV:")
    startTimer()
    var raw = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
      .load("population_by_country_2020.csv")
    var df=raw.select(col("Country (or dependency)"),col("Population (2020)").as("Population2020"))
      .withColumnRenamed("Country (or dependency)", "Country")
    df=df.withColumn("Country", when(col("Country").like("DR Congo"), "Democratic Republic of Congo") otherwise col("Country"))
    df.printSchema()
    df.printLength
    stopTimer()
    // Write the data out as a file to be used for visualization
    populationNormalization=df
    csvWriterHelper(populationNormalization, "PopulationNormalization.csv")
  }

    /**
      * Creates a .csv file entitled '30yrAvgTempByCountryByMonth.csv', that contains historical average (1982-2012) temperature of country by month
      */
    def createHistoricalTemperatureAverageByCountryByMonth(): Unit = {
      // Read "covid_19_data.csv" data as a dataframe
      println("Dataframe read from CSV:")
      startTimer()
      var raw = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
        .load("GlobalLandTemperaturesByCountry.csv").withColumn("dt", to_date(col("dt")))
        .withColumnRenamed("dt", "Date")
      //drop all rows containing any null value
      raw = raw.na.drop()
      var df = raw.filter(raw("Date").gt(lit("1982-01-01"))).filter(raw("Date").lt(lit("2012-12-31")))
      //add month column
      df=df.withColumn("Country", when(col("Country").contains("United Kingdom"), "United Kingdom") otherwise col("Country"))
      df=df.withColumn("Country", when(col("Country").like("Congo (Democratic Republic Of The)"), "Democratic Republic of Congo") otherwise col("Country"))
      df = df.withColumn("Month", month(col("Date")))
      df = df.groupBy("Country", "Month")
        .avg("AverageTemperature", "AverageTemperatureUncertainty")
        .orderBy("Country", "Month")
      println(s"Table length: ${df.count()}")
       stopTimer()
      // Write the data out as a file to be used for visualization
      tempsHistAvg=df
      csvWriterHelper(tempsHistAvg, "30yrAvg1982-2012TempByCountryByMonth.csv")
    }

    /**
      * TODO
      */
    def calcAvgDailyConfirmedDeathsRecovered(): Unit = {
      // Read "covid_19_data.csv" data as a dataframe
      println("Dataframe read from CSV:")
      startTimer()
      var df = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
        .load("covid_19_data.csv")
        .withColumn("ObservationDate", to_date(col("ObservationDate"), "MM/dd/yyyy"))
        .withColumnRenamed("ObservationDate", "Date")
        .withColumnRenamed("Country/Region", "Country")

        .withColumn("Year", year(col("Date")))
        .withColumn("Month", month(col("Date")))
      df = df.filter(df("Date").gt(lit("2020-01-31")))
        .filter(df("Date").lt(lit("2021-05-01")))
      df = df.filter(not(col("Country").like("China")))
        .withColumn("Country", when(col("Country").contains("China"), "China") otherwise col("Country"))
          .withColumn("Country", when(col("Country").like("US"), "United States") otherwise col("Country"))
        .withColumn("Country", when(col("Country").like("UK"), "United Kingdom") otherwise col("Country"))
        .withColumn("Country", when(col("Country").like("Congo (Kinshasa)"), "Democratic Republic of Congo") otherwise col("Country"))
        .withColumn("Country", when(col("Country").like("Congo (Brazzaville)"), "Congo") otherwise col("Country"))

      val rate_window = Window.partitionBy("Country").orderBy("Country")
      df = df.withColumn("confirmed_prev_value", lag(col("Confirmed"), 1).over(rate_window))
      df = df.withColumn("DailyConfirmed", when((col("Confirmed") - col("confirmed_prev_value")<=0), 0)
        .otherwise(col("confirmed") - col("confirmed_prev_value")))
      df = df.withColumn("deaths_prev_value", lag(col("Deaths"), 1).over(rate_window))
      df = df.withColumn("DailyDeaths", when(col("Deaths") - col("deaths_prev_value")<=0, 0)
        .otherwise(col("deaths") - col("deaths_prev_value")))
      df = df.withColumn("recovered_prev_value", lag(col("Recovered"), 1).over(rate_window))
      df = df.withColumn("DailyRecovered",when(col("recovered") - col("recovered_prev_value")<=0, 0)
        .otherwise(col("recovered") - col("recovered_prev_value")))
      .orderBy(col("Country"), col("Year"),col("Month"))
      csvWriterHelper(df,"test.csv")
      //add month column
      df = df.groupBy(col("Country"), col("Year"),col("Month"))
        .avg("DailyConfirmed", "DailyDeaths", "DailyRecovered")
        .orderBy("Country", "Year","Month")
      csvWriterHelper(df,"test2.csv")
      df = df.select(col("Country"),
        col("Year"),
        col("Month"),
        col("avg(DailyConfirmed)").alias("AvgDailyConfirmed"),
        col("avg(DailyDeaths)").alias("AvgDailyDeaths"),
        col("avg(DailyRecovered)").alias("AvgDailyRecovered"))
      //join population table
      df = df.join(populationNormalization,usingColumns = Seq("Country"))
        .where(populationNormalization("Country")===df("Country"))
      df = df.withColumn("Per100000AvgDailyConfirmed",col("AvgDailyConfirmed")/col("Population2020")*100000)
      .withColumn("Per100000AvgDailyDeaths",col("AvgDailyDeaths")/col("Population2020")*100000)
      .withColumn("Per100000AvgDailyRecovered",col("AvgDailyRecovered")/col("Population2020")*100000)
      .orderBy(desc("Population2020"), col("Year"),col("Month"))
      stopTimer()
      df.printSchema()
      // Write the data out as a file to be used for visualization
      covidAvgDaily=df
     csvWriterHelper(covidAvgDaily, "AvgDailyConfirmedDeathsRecovered.csv")
    }

    /**
      * TODO
      */
    def joinAndOutputRatesWithTemperature(): Unit = {
      joined = covidAvgDaily.join(tempsHistAvg, usingColumns = Seq("Country", "Month"))
        .where((covidAvgDaily("Country") === tempsHistAvg("Country")) and ((covidAvgDaily("Month") === tempsHistAvg("Month"))))
      // Write the data out as a file to be used for visualization
      csvWriterHelper(joined,"Visualization-AvgDailyRates&Temperature_ByCountryByTemperature.csv")
    }

  /**
    * Allows write to csv to be one line in other methods, reducing overall code. Method called ios still the csv writer from the Project2 driver
    */
  def csvWriterHelper(df: DataFrame, filename:String):Unit={
    if (WRITE_TO_CSV_ON) {
      // Write the data out as a file to be used for visualization
      startTimer(s"Save $filename as file")
      saveDataFrameAsCSV(df, filename)
      println(s"Saved as: $filename")
      stopTimer("Save $filename")
    }
  }

}

object mySparkUtils {
  var startTime:Double=0.0
  var transTime:Double=0.0

  //implicit class adding the method "Dataframe.printLength"
  implicit class DataframeImplicit(df: org.apache.spark.sql.DataFrame) {
    def printLength ={println(s"Table length: ${df.count()}")}
  }

  def startTimer(a:String="TimedAction", print:Boolean=true): Unit = {
    val action=a.capitalize
    startTime = System.currentTimeMillis()
    if (print || FORCE_TIMER_PRINT){
      println(s"$action timer started at $startTime")
    }
  }

  def stopTimer(a:String="TimedAction", print:Boolean=true): Unit ={
    val action=a.capitalize
    transTime = (System.currentTimeMillis() - startTime) / 1000d
    if (print || FORCE_TIMER_PRINT){
      println(s"$action completed in $transTime seconds")
    }
  }
}
package com.revature.main

import com.revature.main.Project2.{saveDataFrameAsCSV, spark}
import com.revature.main.bq1.joined
import org.apache.curator.shaded.com.google.common.collect.Iterators.limit
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{bround, col, desc, format_number, isnull, lag, last_day, lit, month, not, to_date, when, year}

object dq2 {
  /**
    * Compare ratio of deaths to cases by country from April 2020 - April 2021
    */
  def countryCasesVsDeath(): Unit = {


  }
  // Read "covid_19_data.csv" data as a dataframe
  println("Dataframe read from CSV:")
  var startTime = System.currentTimeMillis()
  var df = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
    .load("covid_19_data.csv")
    .withColumn("ObservationDate", to_date(col("ObservationDate"), "MM/dd/yyyy"))
    .withColumnRenamed("ObservationDate", "Date")
    .withColumnRenamed("Country/Region", "Country")
    .withColumnRenamed("Province/State","State")
    .withColumnRenamed("Confirmed", "Cases")
    .withColumn("Month", month(col("Date")))
    .withColumn("Year", year(col("Date")))
    .withColumn("LastDayOfMonth",last_day(col("Date")))
    .filter(not(col("Country").like("China")))
    .withColumn("Country", when(col("Country").contains("China"), "China")
      otherwise (col("Country")))
  df=df.select(col("Date")
    ,col("State")
  ,col("Country")
  ,col("Cases")
  ,col("Year")
  ,col("Deaths"))
    .filter(col("Date")===col("LastDayOfMonth"))
    .groupBy("Country","Date")
    .sum("Cases",
        "Deaths")
    .withColumn("Month", month(col("Date")))
    .withColumn("Year", year(col("Date")))
    .withColumnRenamed("sum(Deaths)", "MonthDeathsCumulitive")
    .withColumnRenamed("sum(Cases)", "MonthCasesCumulitive")
    .orderBy("Country","Year","Month")
  df=df.select("Country", "Year","Month","MonthCasesCumulitive","MonthDeathsCumulitive","Date")
  df=df.filter(df("Date").gt(lit("2020-03-01"))).filter(df("Date").lt(lit("2021-05-01")))
  df=df.drop("Date")
  val rate_window = Window.partitionBy("Country").orderBy("Country")
  df = df.withColumn("Cases_prev_value", lag(col("MonthCasesCumulitive"), 1).over(rate_window))
  df = df.withColumn("MonthlyNewCases", when(isnull(col("MonthCasesCumulitive") - col("Cases_prev_value")), 0)
    .otherwise(col("MonthCasesCumulitive") - col("Cases_prev_value")))
  df = df.withColumn("deaths_prev_value", lag(col("MonthDeathsCumulitive"), 1).over(rate_window))
  df = df.withColumn("MonthlyNewDeaths", when(isnull(col("MonthDeathsCumulitive") - col("deaths_prev_value")), 0)
    .otherwise(col("MonthDeathsCumulitive") - col("deaths_prev_value")))
  df=df.filter(not(col("Month")===3 and(col("Year")===2020)))
  df=df.select("Country","Year","Month","MonthlyNewCases","MonthlyNewDeaths")
  df = df.withColumn("fraction", col("MonthlyNewDeaths")/col("MonthlyNewCases"))
    .withColumn(  "Percent", col("fraction") * 100 )
   .withColumn("Percent", bround(col("Percent"),3))
    .drop("fraction")
    .orderBy("Country","Year", "Month")
  println("Death and Case Ratio by Month from April 2020 - April 2021")
  df.show()
  //Shows Death/Case Ratio Monthly percentage
  startTime = System.currentTimeMillis()
  val monthly = saveDataFrameAsCSV(df, "MonthlyData.csv")
  val transTime = (System.currentTimeMillis() - startTime) / 1000d
  println(s"Saved as: $monthly")
  println(s"Save completed in $transTime seconds.\n")

  var df3: DataFrame =df.groupBy("Country")
    .sum("MonthlyNewCases","MonthlyNewDeaths")
    .withColumnRenamed("sum(MonthlyNewCases)","MonthlyNewCases")
    .withColumnRenamed("sum(MonthlyNewDeaths)","MonthlyNewDeaths")
  df3 = df3.withColumn("fraction", col("MonthlyNewDeaths")/col("MonthlyNewCases"))
    .withColumn(  "Percent", col("fraction") * 100 )
    .withColumn("Percent", bround(col("Percent"),3))
    .drop("fraction")
    .orderBy("Country")
  println("Death and Case Ratio for a Year (April 2020 - April 2021)")
  df3.show()
  startTime = System.currentTimeMillis()
  val yearly = saveDataFrameAsCSV(df3, "YearlyData.csv")
  println(s"Saved as: $yearly")
  println(s"Save completed in $transTime seconds.\n")
}

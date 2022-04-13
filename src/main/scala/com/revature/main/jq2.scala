package com.revature.main

import com.revature.main.Project2.{saveDataFrameAsCSV, spark}
import org.apache.spark.sql.{ SparkSession, SaveMode, Row, DataFrame }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.collection.mutable.ArrayBuffer
import javax.jdo.annotations.Column

object jq2 {  // By @JeffH001 / Jeffrey Hafner

	/**
	  *  Compare the rates of deaths for each US state + DC against their population sizes to see which US states had the
	  *  best and worst ratios by month and averages from May 2020 to April 2021 (inclusive; i.e. 5/1/20 to 4/30/21).
	  */
	def getStateDeathRates (): Unit = {
		// Note: February had 29 days in 2020, and 28 days in 2021
		val monthLen = Array(0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
		val monthName = Array("", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")

		// Read "time_series_covid_19_deaths_US.csv" data as a dataframe.  Format:
		// UID,iso2,iso3,code3,FIPS,Admin2,Province_State,Country_Region,Lat,Long_,Combined_Key,Population,1/22/20,1/23/20,...,5/2/21
		println("Reading US COVID deaths by county into a dataframe from CSV:")
		var startTime = System.currentTimeMillis()
		var df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("time_series_covid_19_deaths_US.csv")
		var transTime = (System.currentTimeMillis() - startTime) / 1000d
		// df.show(false)
		println(s"Table length: ${df.count()}")
		println(s"Transaction time: $transTime seconds")
		// df.printSchema()

		// Make a list of needed columns and columns to sum up
		var monthEnd = ""
		var month = 4
		var year = 20
		var cols = Seq(col("Province_State"), col("Population"))
		var sumMap = Map("Population" -> "sum")
		while (!(month == 5 && year == 21)) {
			monthEnd = month + "/" + monthLen(month) + "/" + year
			sumMap += (monthEnd -> "sum")  // Add column to the list of columns to be summed
			cols = cols :+ col(monthEnd)  // Add column to the list of columns to select from
			month += 1
			if (month == 13) {
				month = 1
				year += 1
			}
		}

		// Combine US state data, removing unneeded columns, summing the population data and the number of deaths at the end of each month, and removing provinces and cruise ships
		println("\nGrouping data on COVID-19 deaths by state + DC:")
		startTime = System.currentTimeMillis()
		df = df
			.select(cols: _*)
			.where("Province_State != 'American Samoa' AND " +
				   "Province_State != 'Diamond Princess' AND " +
				   "Province_State != 'Grand Princess' AND " +
				   "Province_State != 'Guam' AND " +
				   "Province_State != 'Northern Mariana Islands' AND " +
				   "Province_State != 'Puerto Rico' AND " +
				   "Province_State != 'Virgin Islands'")  // Removes unneeded data
			.groupBy("Province_State")  // Groups the state data together
			.agg(sumMap)  // Performs the aggregation calculations on the columns in the sumMap map
			.withColumnRenamed("sum(Population)", "Population")

		// Rename the date columns to remove the "sum(...)" part
		month = 4
		year = 20
		while (!(month == 5 && year == 21)) {
			monthEnd = month + "/" + monthLen(month) + "/" + year
			df = df.withColumnRenamed(s"sum(${monthEnd})", monthEnd)  // Do rename
			month += 1
			if (month == 13) {
				month = 1
				year += 1
			}
		}

		// Update the dataframe to order the columns and rows properly
		df = df
			.select(cols: _*)
			.orderBy("Province_State")

		// Show the current state of the data
		df.show(51, false)
		transTime = (System.currentTimeMillis() - startTime) / 1000d
		println(s"Table length: ${df.count()}")
		println(s"Transaction time: $transTime seconds")

		// Get the deaths as percent of state population by month
		println("\nCalculating COVID-19 deaths as percent of state population by month...")
		startTime = System.currentTimeMillis()
		var df1 = df  // Dataframe for total deaths each month by state
		var df2 = df  // Dataframe for deaths as a percentage of the population each month by state
		month = 4
		year = 20
		var lastMonth = month + "/" + monthLen(month) + "/" + year
		var monthTitle = ""
		cols = Seq.empty[org.apache.spark.sql.Column]
		month = 5
		while (!(month == 5 && year == 21)) {  // Run the calculations
			monthEnd = month + "/" + monthLen(month) + "/" + year
			monthTitle = monthName(month) + year.toString()
			cols = cols :+ col(monthTitle)  // Build a list of columns to average
			df1 = df1.withColumn(monthTitle, col(monthEnd) - col(lastMonth))  // Total deaths by month
			df2 = df2.withColumn(monthTitle, ((col(monthEnd) - col(lastMonth)) / col("Population")) * 100)  // Percent of total deaths in population by month
			lastMonth = monthEnd
			month += 1
			if (month == 13) {
				month = 1
				year += 1
			}
		}

		// Remove the unneeded columns from the dataframes
		month = 4
		year = 20
		while (!(month == 5 && year == 21)) {
			monthEnd = month + "/" + monthLen(month) + "/" + year
			df1 = df1.drop(monthEnd)
			df2 = df2.drop(monthEnd)
			month += 1
			if (month == 13) {
				month = 1
				year += 1
			}
		}

		// Find the average of the months values for each state + DC
		var avgit = cols.foldLeft(lit(0)){(x, y) => x + y} / cols.length
		df1 = df1.withColumn("Average", avgit)
		df2 = df2.withColumn("Average", avgit)

		// Display the data
		println("\nTotal COVID-19 deaths by month for each US state + DC:")
		df1.show(51, false)
		println("COVID-19 deaths as percent of state population by month:")
		df2.show(51, false)
		transTime = (System.currentTimeMillis() - startTime) / 1000d
		println(s"Transaction time: $transTime seconds")

		// Write the data out as a file to be used for visualization
		println("\nSave data as file...")
		startTime = System.currentTimeMillis()
		val fname1 = saveDataFrameAsCSV(df1, "USStateDeathsByMonth.csv")
		val fname2 = saveDataFrameAsCSV(df2, "USStatePctDeathsByMonth.csv")
		transTime = (System.currentTimeMillis() - startTime) / 1000d
		println(s"Saved number of deaths as: $fname1")
		println(s"Saved percent of deaths as: $fname2")
		println(s"Save completed in $transTime seconds.\n")
	}
}
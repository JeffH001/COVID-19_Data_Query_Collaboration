package com.revature.main

import com.revature.main.Project2.{saveDataFrameAsCSV, spark}
import org.apache.spark.sql.{ SparkSession, SaveMode, Row, DataFrame }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.collection.mutable.ArrayBuffer

object jq1 {  // By @JeffH001 / Jeffrey Hafner

	/**
	  * Percentage of cases which resulted in deaths in the top 10 largest US counties by month from May 2020 to April 2021 (inclusive; i.e. 5/1/20 to 4/30/21).
	  */
	def getTopCountyRates (): Unit = {
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

		// Copy the dataframe data into table "usdeaths"
		println("\nFill Table 'usdeaths' from dataframe:")
		startTime = System.currentTimeMillis()
		spark.sql("DROP TABLE IF EXISTS usdeaths")
		df.createOrReplaceTempView("temptable")  // Copies the dataframe into a view as "temptable"
		spark.sql("CREATE TABLE usdeaths AS SELECT * FROM temptable")  // Loads the data into the "usdeaths" table from the view
		spark.catalog.dropTempView("temptable")  // View no longer needed
		transTime = (System.currentTimeMillis() - startTime) / 1000d
		var usdeaths = spark.sql("SELECT * FROM usdeaths").orderBy(desc("Population"))
		usdeaths.show(false)
		// usdeaths.explain()  // Shows the table's definition
		spark.sql("SELECT COUNT(*) FROM usdeaths").show()
		println(s"Transaction time: $transTime seconds\n")

		// Read "time_series_covid_19_confirmed_US.csv" data as a dataframe.  Format:
		// UID,iso2,iso3,code3,FIPS,Admin2,Province_State,Country_Region,Lat,Long_,Combined_Key,1/22/20,1/23/20,...,5/2/21
		println("Reading US COVID cases by county into a dataframe from CSV:")
		startTime = System.currentTimeMillis()
		df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("time_series_covid_19_confirmed_US.csv")
		transTime = (System.currentTimeMillis() - startTime) / 1000d
		// df.show(false)
		println(s"Table length: ${df.count()}")
		println(s"Transaction time: $transTime seconds")
		// df.printSchema()

		// Copy the dataframe data into table "uscases"
		println("\nFill Table 'uscases' from dataframe:")
		startTime = System.currentTimeMillis()
		spark.sql("DROP TABLE IF EXISTS uscases")
		df.createOrReplaceTempView("temptable")  // Copies the dataframe into a view as "temptable"
		spark.sql("CREATE TABLE uscases AS SELECT * FROM temptable")  // Loads the data into the "usdeaths" table from the view
		spark.catalog.dropTempView("temptable")  // View no longer needed
		transTime = (System.currentTimeMillis() - startTime) / 1000d
		var uscases = spark.sql("SELECT * FROM uscases")
		uscases.show(false)
		// tabledat.explain()  // Shows the table's definition
		spark.sql("SELECT COUNT(*) FROM uscases").show()
		println(s"Transaction time: $transTime seconds\n")

		// Find the data for the top 10 largest counties
		println("Top 10 largest counties - Percentage of cases which resulted in death:")
		startTime = System.currentTimeMillis()
		var topcountyarr = usdeaths.head(10)
		var countyinfo = ArrayBuffer.empty[Row]
		var casesRow = Row.empty
		var rowDat = Row.empty
		//var deathsArr = ArrayBuffer.empty[Int]
		var deathsArr = ArrayBuffer.empty[Double]
		var day = 0
		for (i <- 0 to 9) {  // Build array for table
			casesRow = uscases.where("Combined_Key = '" + topcountyarr(i)(10) + "'").first()  // Get matching "Combined_Key" data from "uscases" table
			// deathsArr = ArrayBuffer.empty[Int]
			deathsArr = ArrayBuffer.empty[Double]
			day = 111  // 111 = "4/30/20" in "usdeaths" table = 110 in "uscases" table
			for (m <- 5 to 12) {  // Monthly cases and deaths for 5/20 to 12/20
				// deathsArr :+= casesRow(day + monthLen(m) - 1).toString().toInt - casesRow(day - 1).toString().toInt  // Number of cases this month
				// deathsArr :+= topcountyarr(i)(day + monthLen(m)).toString().toInt - topcountyarr(i)(day).toString().toInt  // Number of deaths this month
				deathsArr :+= (	(topcountyarr(i)(day + monthLen(m)).toString().toDouble - topcountyarr(i)(day).toString().toDouble) /
								(casesRow(day + monthLen(m) - 1).toString().toInt - casesRow(day - 1).toString().toInt) ) * 100
				day += monthLen(m)
			}
			for (m <- 1 to 4) {  // Monthly cases and deaths for 1/21 to 4/21
				// deathsArr :+= casesRow(day + monthLen(m) - 1).toString().toInt - casesRow(day - 1).toString().toInt  // Number of cases this month
				// deathsArr :+= topcountyarr(i)(day + monthLen(m)).toString().toInt - topcountyarr(i)(day).toString().toInt  // Number of deaths this month
				deathsArr :+= (	(topcountyarr(i)(day + monthLen(m)).toString().toDouble - topcountyarr(i)(day).toString().toDouble) /
								(casesRow(day + monthLen(m) - 1).toString().toInt - casesRow(day - 1).toString().toInt) ) * 100
				day += monthLen(m)
			}
			// 10 = "Combined_Key", 11 = "Population"
			// rowDat = Row(topcountyarr(i)(10), topcountyarr(i)(11), deathsArr(0), deathsArr(1), deathsArr(2), deathsArr(3), deathsArr(4), deathsArr(5), deathsArr(6), deathsArr(7), deathsArr(8), deathsArr(9), deathsArr(10), deathsArr(11), deathsArr(12), deathsArr(13), deathsArr(14), deathsArr(15), deathsArr(16), deathsArr(17), deathsArr(18), deathsArr(19), deathsArr(20), deathsArr(21), deathsArr(22), deathsArr(23))
			rowDat = Row(topcountyarr(i)(10), topcountyarr(i)(11), deathsArr(0), deathsArr(1), deathsArr(2), deathsArr(3), deathsArr(4), deathsArr(5), deathsArr(6), deathsArr(7), deathsArr(8), deathsArr(9), deathsArr(10), deathsArr(11))
			countyinfo += rowDat
		}
		val tableStructure = StructType(Array(  // Describe the data
			StructField("Combined_Key", StringType, false),
			StructField("Population", IntegerType, false),
			StructField("May20", DoubleType, false),
			StructField("Jun20", DoubleType, false),
			StructField("Jul20", DoubleType, false),
			StructField("Aug20", DoubleType, false),
			StructField("Sep20", DoubleType, false),
			StructField("Oct20", DoubleType, false),
			StructField("Nov20", DoubleType, false),
			StructField("Dec20", DoubleType, false),
			StructField("Jan21", DoubleType, false),
			StructField("Feb21", DoubleType, false),
			StructField("Mar21", DoubleType, false),
			StructField("Apr21", DoubleType, false)
			/*
			StructField("Cases_May20", IntegerType, false),
			StructField("Deaths_May20", IntegerType, false),
			StructField("Cases_Jun20", IntegerType, false),
			StructField("Deaths_Jun20", IntegerType, false),
			StructField("Cases_Jul20", IntegerType, false),
			StructField("Deaths_Jul20", IntegerType, false),
			StructField("Cases_Aug20", IntegerType, false),
			StructField("Deaths_Aug20", IntegerType, false),
			StructField("Cases_Sep20", IntegerType, false),
			StructField("Deaths_Sep20", IntegerType, false),
			StructField("Cases_Oct20", IntegerType, false),
			StructField("Deaths_Oct20", IntegerType, false),
			StructField("Cases_Nov20", IntegerType, false),
			StructField("Deaths_Nov20", IntegerType, false),
			StructField("Cases_Dec20", IntegerType, false),
			StructField("Deaths_Dec20", IntegerType, false),
			StructField("Cases_Jan21", IntegerType, false),
			StructField("Deaths_Jan21", IntegerType, false),
			StructField("Cases_Feb21", IntegerType, false),
			StructField("Deaths_Feb21", IntegerType, false),
			StructField("Cases_Mar21", IntegerType, false),
			StructField("Deaths_Mar21", IntegerType, false),
			StructField("Cases_Apr21", IntegerType, false),
			StructField("Deaths_Apr21", IntegerType, false)
			*/
		))
		// Convert the array into a dataframe
		var top10counties = spark.createDataFrame(spark.sparkContext.parallelize(countyinfo), tableStructure)
		transTime = (System.currentTimeMillis() - startTime) / 1000d
		top10counties.show(false)

		// Write the data out as a file to be used for visualization
		println("Save data as file...")
		startTime = System.currentTimeMillis()
		val fname = saveDataFrameAsCSV(top10counties, "top10countiespctdeaths.csv")
		transTime = (System.currentTimeMillis() - startTime) / 1000d
		println(s"Saved as: $fname")
		println(s"Save completed in $transTime seconds.\n")
	}
}
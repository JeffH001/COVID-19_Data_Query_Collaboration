package com.revature.main


import com.revature.main.Project2.{saveDataFrameAsCSV, spark}
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, isnull, lag, lit, month, not, to_date, when}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.io.File
import scala.util.Try

object Project2 {
	var spark:SparkSession = null

	def date_format(value: Any, str: String) = ???

	/**
	  * Main program section.  Sets up Spark session, runs queries, and then closes the session.
	  *
	  * @param args	Executable's paramters (ignored).
	  */
	def main (args: Array[String]): Unit = {
		// Start the Spark session
		System.setProperty("hadoop.home.dir", "C:\\hadoop")
		Logger.getLogger("org").setLevel(Level.ERROR)  // Hide most of the initial non-error log messages
		spark = SparkSession.builder
			.appName("Proj2")
			.config("spark.master", "local[*]")
			.enableHiveSupport()
			.getOrCreate()
		spark.sparkContext.setLogLevel("ERROR")  // Hide further non-error messages
		spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
		println("Created Spark session.\n")
		// Create the database if needed
		spark.sql("CREATE DATABASE IF NOT EXISTS proj2")
		spark.sql("USE proj2")
		// Run the "getUniqueCountries" query

		bq1.normalizePopulation()
		bq1.createHistoricalTemperatureAverageByCountryByMonth()
		bq1.calcAvgDailyConfirmedDeathsRecovered()
		bq1.joinAndOutputRatesWithTemperature()
    dq2.countryCasesVsDeath()

		// End Spark session
		spark.stop()
		println("Transactions complete.")
	}

	/**
		* Gets a list of filenames in the given directory, filtered by optional matching file extensions.
		*
		* @param dir			Directory to search.
		* @param extensions	Optional list of file extensions to find.
		* @return				List of filenames with paths.
		*/
	def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
		dir.listFiles.filter(_.isFile).toList.filter { file => extensions.exists(file.getName.endsWith(_)) }
	}

	/**
		* Moves/renames a file.
		*
		* @param oldName	Old filename and path.
		* @param newName	New filename.
		* @return			Success or failure.
		*/
	def mv(oldName: String, newName: String) = {
		Try(new File(oldName).renameTo(new File(newName))).getOrElse(false)
	}

	def saveDataFrameAsCSV(df: DataFrame, filename: String): String = {
		df.coalesce(1).write.options(Map("header"->"true", "delimiter"->",")).mode(SaveMode.Overwrite).format("csv").save("tempCSVDir")
		val curDir = System.getProperty("user.dir")
		val srcDir = new File(curDir + "/tempCSVDir")
		val files = getListOfFiles(srcDir, List("csv"))
		var srcFilename = files(0).toString()
		val destFilename = curDir + "/" + filename
		FileUtils.deleteQuietly(new File(destFilename))  // Clear out potential old copies
		mv(srcFilename, destFilename)  // Move and rename file
		FileUtils.deleteQuietly(srcDir)  // Delete temp directory
		destFilename
	}



}

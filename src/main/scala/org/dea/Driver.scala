package org.dea

import scala.math.min
import org.apache.spark.sql.SparkSession

import scala.reflect.io.File

object Driver {

  def printHelp(): Unit = {
    println("Invalid Argument! Job accepts Input file path as input parameter and Output Path.")
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      printHelp()
      sys.exit(1)
    }
    val inputDataPath = args(0)
    val inputFile = File(inputDataPath)
    if (!inputFile.exists) {
      throw new RuntimeException(s"Input File $inputDataPath Not Found")
    }

    val numberOfPartitions = min(inputFile.length / (1024 * 1024), 10).toInt
    val spark = {
      getSparkSession
    }
    val process = new DataAnalysis(spark, inputDataPath, numberOfPartitions)
    process.run()
  }

  def getSparkSession: SparkSession = {
    SparkSession.builder()
      .appName("DataEngineeringAnalysis")
      .getOrCreate()

  }

}

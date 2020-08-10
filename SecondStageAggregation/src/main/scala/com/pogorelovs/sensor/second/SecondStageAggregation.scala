package com.pogorelovs.sensor.second

import java.io.File
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.{Duration, LocalDateTime}

import org.apache.commons.cli.{CommandLine, DefaultParser, HelpFormatter, Options}
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.JsonAST.JString
import org.json4s.JsonDSL._
import org.json4s.native.Serialization.write
import org.json4s.{CustomSerializer, DefaultFormats, Formats}

import scala.collection.JavaConverters._

object SecondStageAggregation {

  val newDuration: Duration = Duration.ofHours(1)
  val customDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

  def main(args: Array[String]): Unit = {
    val providedArgs: CommandLine = parseAndCheckAppArguments(args)

    val inputPath = providedArgs.getOptionValue("in")
    val outputPath = providedArgs.getOptionValue("out")

    val conf = new SparkConf()
      .setAppName("Second aggregation")
      .setMaster("local[*]") //TODO: comment for spark-submit

    val spark = SparkContext.getOrCreate(conf)

    //TODO: Regular read first file of file for start date.
    // Since we know it's sorted ...

    val lineModelsRdd = parseInputFile(inputPath, spark, customDateFormat)

    val firstTimestamp = lineModelsRdd.map(l => l.timeslotStart.toString).distinct().sortBy(t => t).take(1)(0) //FIXME(optimisation): it's possible just to read first line of file via regular file read (without spark)

    val startLocalDateTime = Timestamp.valueOf(firstTimestamp).toLocalDateTime
    val resultingLineModels = doAggregation(lineModelsRdd, startLocalDateTime)

    implicit val formats: Formats = DefaultFormats + timestampSerializer + doubleSerializer
    val json = resultingLineModels.map(write(_)).mkString("[", ",\n", "]")

    FileUtils.write(new File(outputPath), json, false)
  }

  def doAggregation(lineModelsRdd: RDD[LineModel], startLocalDateTime: LocalDateTime) = {
    val result = lineModelsRdd
      .map(l => ((Duration.between(startLocalDateTime, l.timeslotStart.toLocalDateTime).dividedBy(newDuration),
        l.locationId), l))
      .map(pair => (pair._1,
        LineModel(pair._2.timeslotStart, pair._2.locationId,
          pair._2.tempMin, pair._2.tempMax, pair._2.tempAvg, pair._2.tempCnt,
          pair._2.presence, if (pair._2.presence) 1 else 0))
      )
      .reduceByKey((v1, v2) => {
        val timestamp = Timestamp.valueOf(v1.timeslotStart.toLocalDateTime.withMinute(0).withSecond(0))
        val locationId = v1.locationId

        val tempMin = applyToDouble(v1.tempMin, v2.tempMin, Math.min)
        val tempMax = applyToDouble(v1.tempMax, v2.tempMax, Math.max)

        val tempAverage = applyToDouble(v1.tempAvg, v2.tempAvg, (v1: Double, v2: Double) => (v1 + v2) / 2)
        val roundedAverage = if (tempAverage.nonEmpty) roundNumber(tempAverage, 2) else tempAverage

        val tempCnt = v1.tempCnt + v2.tempCnt
        val presence = v1.presence || v2.presence
        val presenceCnt = v1.presenceCnt + v2.presenceCnt

        LineModel(timestamp, locationId, tempMin, tempMax, roundedAverage, tempCnt, presence, presenceCnt)
      })

    val resultingLineModels: Array[LineModel] = result.map(l => (l._1._1, l._2))
      .collect()
      .sortBy(l => l._2.timeslotStart.getTime + l._2.locationId)
      .map(_._2)
    resultingLineModels
  }

  val timestampSerializer = new CustomSerializer[Timestamp](_ => (
    {case JString(s) => new Timestamp(customDateFormat.parse(s).getTime)},
    {case s: Timestamp => customDateFormat.format(s)}
  ))

  val doubleSerializer = new CustomSerializer[Option[Double]](_ => (
    {case JString(str) => Option[Double](str.toDouble)},
    {case double: Double => if (double > 0) double else Option.empty[Double]}
  ))

  case class LineModel(timeslotStart: Timestamp,
                       locationId: String,
                       tempMin: Option[Double],
                       tempMax: Option[Double],
                       tempAvg: Option[Double],
                       tempCnt: Long,
                       presence: Boolean,
                       presenceCnt: Long)

  private def parseAndCheckAppArguments(args: Array[String]) = {
    val cliOptions = new Options()
    cliOptions.addOption("in", true, "Input file")
    cliOptions.addOption("out", true, "Output file")

    val providedArgs = new DefaultParser().parse(cliOptions, args)

    val missingOptions = cliOptions.getOptions.asScala
      .map(_.getOpt)
      .filter((option: String) => !providedArgs.hasOption(option))
      .toSet

    if (missingOptions.nonEmpty) {
      System.err.println("Missing required arguments: " + missingOptions.mkString("[", ", ", "]"))
      new HelpFormatter().printHelp("java -jar SecondStageAggregation-develop-all.jar",
        "Required args:", cliOptions, "Example: -in in.csv -out out.json")
      System.exit(-1)
    }
    providedArgs
  }

  def parseInputFile(inputPath: String, spark: SparkContext, dateFormat: SimpleDateFormat) = {
    spark.textFile(inputPath)
      .map(line => {
        val parts = line.split(',')
        val timeslotStart = new Timestamp(dateFormat.parse(parts(0)).getTime)
        val locationId = parts(1)
        val tempMin = if (StringUtils.isNotBlank(parts(2))) Option[Double](parts(2).toDouble) else Option.empty
        val tempMax = if (StringUtils.isNotBlank(parts(3))) Option[Double](parts(3).toDouble) else Option.empty
        val tempAvg = if (StringUtils.isNotBlank(parts(4))) Option[Double](parts(4).toDouble) else Option.empty
        val tempCnt = parts(5).toLong
        val presence = parts(6).toBoolean
        val presenceCnt = parts(7).toLong

        LineModel(timeslotStart, locationId, tempMin, tempMax, tempAvg, tempCnt, presence, presenceCnt)
      })
  }

  def applyToDouble(a: Option[Double], b: Option[Double], f: (Double, Double) => Double): Option[Double] = {
    if (a.nonEmpty && b.nonEmpty)
      Option[Double](f.apply(a.get, b.get))
    else if (a.nonEmpty) a else b
  }

  private def roundNumber(number: Option[Double], precision: Int): Option[Double] = {
    Option[Double](BigDecimal(number.get).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble)
  }
}

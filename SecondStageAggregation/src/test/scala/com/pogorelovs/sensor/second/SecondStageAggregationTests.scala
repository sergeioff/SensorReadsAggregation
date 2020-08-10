package com.pogorelovs.sensor.second

import java.sql.Timestamp

import com.pogorelovs.sensor.second.SecondStageAggregation.{doAggregation, parseInputFile}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class SecondStageAggregationTests extends AnyFlatSpec with should.Matchers {
  private val conf = new SparkConf()
    .setAppName("Second stage aggregation tests")
    .setMaster("local[*]")

  private val spark = SparkContext.getOrCreate(conf)

  private val lineModelsRdd = parseInputFile(
    this.getClass.getResource("/firstStage.csv").getPath, spark, SecondStageAggregation.customDateFormat
  )

  private val firstTimestamp = lineModelsRdd.map(l => l.timeslotStart.toString).distinct().sortBy(t => t).take(1)(0) //FIXME(optimisation): it's possible just to read first line of file via regular file read (without spark)

  private val startLocalDateTime = Timestamp.valueOf(firstTimestamp).toLocalDateTime
  private val resultingLineModels = doAggregation(lineModelsRdd, startLocalDateTime)

  "Aggregation result" should "have 96 lines" in {
    resultingLineModels.length should be(96)
  }

  it should "tempMin calculation correct" in {
    resultingLineModels(0).tempMin should be(Option.empty[Double])
    resultingLineModels(48).tempMin should be(Option[Double](57.89)) // 4 rooms, so 12PM * 4 ROOMS = 48  (12 AM ROOM 0)
    resultingLineModels(49).tempMin should be(Option[Double](63.58)) // (12 AM ROOM 1)
    resultingLineModels(50).tempMin should be(Option[Double](53.59)) // (12 AM ROOM 2)
    resultingLineModels(51).tempMin should be(Option[Double](59.52)) // (12 AM ROOM 3)
  }

  it should "tempMax calculation correct" in {
    resultingLineModels(0).tempMax should be(Option.empty[Double])
    resultingLineModels(48).tempMax should be(Option[Double](69.63)) // 4 rooms, so 12PM * 4 ROOMS = 48  (12 AM ROOM 0)
    resultingLineModels(49).tempMax should be(Option[Double](67.4)) // (12 AM ROOM 1)
    resultingLineModels(50).tempMax should be(Option[Double](68.08)) // (12 AM ROOM 2)
    resultingLineModels(51).tempMax should be(Option[Double](70.55)) // (12 AM ROOM 3)
  }

  it should "tempAvg calculation correct" in {
    resultingLineModels(0).tempAvg should be(Option.empty[Double])
    println(resultingLineModels(48))
    resultingLineModels(48).tempAvg should be(Option[Double]((64.24 + 63.9 + 63.74) / 3)) // 4 rooms, so 12PM * 4 ROOMS = 48  (12 AM ROOM 0)
    resultingLineModels(49).tempAvg should be(Option[Double]((64.99 + 64.93 + 66.07) / 3)) // (12 AM ROOM 1)
    resultingLineModels(50).tempAvg should be(Option[Double]((62.85 + 61.96 + 61.22) / 3)) // (12 AM ROOM 2)
    resultingLineModels(51).tempAvg should be(Option[Double]((64.67 + 64.16 + 66.43) / 3)) // (12 AM ROOM 3)
  }

  it should "tempCnt calculation correct" in {
    resultingLineModels(0).tempCnt should be(0)
    resultingLineModels(48).tempCnt should be(519 + 540 + 540) // 4 rooms, so 12PM * 4 ROOMS = 48  (12 AM ROOM 0)
    resultingLineModels(49).tempCnt should be(173 + 180 + 180) // (12 AM ROOM 1)
    resultingLineModels(50).tempCnt should be(346 + 360 + 360) // (12 AM ROOM 2)
    resultingLineModels(51).tempCnt should be(348 + 360 + 360) // (12 AM ROOM 3)
  }

  it should "presence calculation correct" in {
    resultingLineModels(0).presence should be(false)
    resultingLineModels(48).presence should be(true) // 4 rooms, so 12PM * 4 ROOMS = 48  (12 AM ROOM 0)
    resultingLineModels(49).presence should be(true) // (12 AM ROOM 1)
    resultingLineModels(50).presence should be(true) // (12 AM ROOM 2)
    resultingLineModels(51).presence should be(true) // (12 AM ROOM 3)
  }

  it should "presenceCnt calculation correct" in {
    resultingLineModels(0).presenceCnt should be(0)
    resultingLineModels(48).presenceCnt should be(3) // 4 rooms, so 12PM * 4 ROOMS = 48  (12 AM ROOM 0)
    resultingLineModels(49).presenceCnt should be(3) // (12 AM ROOM 1)
    resultingLineModels(50).presenceCnt should be(2) // (12 AM ROOM 2)
    resultingLineModels(51).presenceCnt should be(3) // (12 AM ROOM 3)
  }
}

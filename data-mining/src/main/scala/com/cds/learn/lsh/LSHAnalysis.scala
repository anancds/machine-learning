package com.cds.learn.lsh

import com.cds.learn.lsh.collision.{CollisionStrategy, SimpleCollisionStrategy}
import com.cds.learn.lsh.linalg.{CosineDistance, DistanceMeasure}
import org.apache.log4j.Logger
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.storage.StorageLevel

/**
  * Copyright (c) 2015, zhejiang Unview Technologies Co., Ltd.
  * All rights reserved.
  * <http://www.uniview.com/>
  * -----------------------------------------------------------
  * Product      :Salut
  * Module Name  :
  * Project Name :Salut
  * Package Name :com.uniview.salut.spark.traffic.lsh
  * Date Created :2016/6/18
  * Creator      :l02461
  * Description  :
  * -----------------------------------------------------------
  * Modification History
  * Date        Name          Description
  * ------------------------------------------------------------
  * 2016/6/18      l02461        Salut project,new code file.
  * ------------------------------------------------------------
  */
object LSHAnalysis {

  private val LOG = Logger.getLogger(getClass)
//  var confUtil: ConfigInfoUtil = null
  var trafficTableNums: Int = 4
  var faceImageTableNums: Int = 4
  var videoTableNums: Int = 4
  var personTableNums: Int = 6
  var trafficSignatureLength: Int = 10
  var faceImageSignatureLength: Int = 10
  var videoSignatureLength: Int = 10
  var personSignatureLength: Int = 10
  var trafficDimensions: Int = 500
  var faceImageDimension: Int = 10
  var videoDimension: Int = 400
  var personDimension: Int = 128
  val measure: String = "cosine"
  var ann: ANN = null
  var trafficHashFunctions: Array[LSHFunction[_]] = Array()
  var faceImagehashFunctions: Array[LSHFunction[_]] = Array()
  var videoHashFunctions: Array[LSHFunction[_]] = Array()
  var personHashFunctions: Array[LSHFunction[_]] = Array()
  val candidateStrategy: CollisionStrategy = new SimpleCollisionStrategy
  val distanceMeasure: DistanceMeasure = CosineDistance
  val persistLevel = StorageLevel.MEMORY_AND_DISK
  val randomSeed = 16

  /** 交通业务标志位 */
  val TRAFFIC_BUSINESS = "traffic"

  /** 人脸业务标志位 */
  val FACE_BUSINESS = "faceImage"

  /** 视频业务标志位 */
  val VIDEO_BUSINESS = "video"


  //偏java的写法，是否有更好的scala写法。
//  def parseConfig() = {
//    try {
//      confUtil = new ConfigInfoUtil(true)
//      trafficTableNums = confUtil.getValue("salut.trafficTableNums").toInt
//      faceImageTableNums = confUtil.getValue("salut.faceImageTableNums").toInt
//      videoTableNums = confUtil.getValue("salut.videoTableNums").toInt
//      personTableNums = confUtil.getValue("salut.personTableNums").toInt
//      trafficSignatureLength = confUtil.getValue("salut.trafficSignatureLength").toInt
//      faceImageSignatureLength = confUtil.getValue("salut.faceImageSignatureLength").toInt
//      videoSignatureLength = confUtil.getValue("salut.videoSignatureLength").toInt
//      personSignatureLength = confUtil.getValue("salut.personSignatureLength").toInt
//      trafficDimensions = confUtil.getValue("salut.trafficDimensions").toInt
//      faceImageDimension = confUtil.getValue("salut.faceImageDimension").toInt
//      videoDimension = confUtil.getValue("salut.videoDimension").toInt
//      personDimension = confUtil.getValue("salut.personDimension").toInt
//
//      confUtil.clear()
//      true
//    } catch {
//      case e: Exception =>
//        LOG.error("Load config error, for ", e)
//        false
//    }
//  }

  /**
    * 初始化ANN
    */
  {
    LOG.info("Begin to init...")
    //读取配置文件
//    parseConfig()
//    if (!parseConfig()) {
//      LOG.error("parse configure failed! system exited!")
//    }
    LOG.info("trafficTableNums are " + trafficTableNums)
    LOG.info("faceImageTableNums are " + faceImageTableNums)
    LOG.info("videoTableNums are " + videoTableNums)
    LOG.info("personTableNums are " + personTableNums)

    LOG.info("trafficSignatureLength is " + trafficSignatureLength)
    LOG.info("faceImageSignatureLength is " + faceImageSignatureLength)
    LOG.info("videoSignatureLength is " + videoSignatureLength)
    LOG.info("personSignatureLength is " + personSignatureLength)

    LOG.info("trafficDimensions length is " + trafficDimensions)
    LOG.info("faceImageDimension length is " + faceImageDimension)
    LOG.info("videoDimension length is " + videoDimension)
    LOG.info("personDimension length is " + personDimension)

    ann = new ANN(trafficDimensions, measure).setTables(trafficTableNums).setSignatureLength(trafficSignatureLength).setRandomSeed(randomSeed)
    trafficHashFunctions = ann.getHashFunctions(trafficDimensions)

    ann = new ANN(faceImageDimension, measure).setTables(faceImageTableNums).setSignatureLength(faceImageSignatureLength).setRandomSeed(randomSeed)
    faceImagehashFunctions = ann.getHashFunctions(faceImageDimension)

    ann = new ANN(videoDimension, measure).setTables(videoTableNums).setSignatureLength(videoSignatureLength).setRandomSeed(randomSeed)
    videoHashFunctions = ann.getHashFunctions(videoDimension)

    ann = new ANN(personDimension, measure).setTables(personTableNums).setSignatureLength(personSignatureLength).setRandomSeed(randomSeed)
    personHashFunctions = ann.getHashFunctions(personDimension)

  }

  /**
    * 计算向量的Hash值
    */
  def computerVectorHashValue(array: Array[Double], business: String): Array[Int] = {

    val vectors = Vectors.dense(array).toSparse
    var arrayBuffer = Array(vectors)
    business match {
      case TRAFFIC_BUSINESS =>
        val values = ANNModel.train(arrayBuffer, trafficHashFunctions, candidateStrategy, distanceMeasure, persistLevel).computerHashValues()
        values

      case FACE_BUSINESS =>
        val values = ANNModel.train(arrayBuffer, faceImagehashFunctions, candidateStrategy, distanceMeasure, persistLevel).computerHashValues()
        values

      case VIDEO_BUSINESS =>
        val values = ANNModel.train(arrayBuffer, videoHashFunctions, candidateStrategy, distanceMeasure, persistLevel).computerHashValues()
        values

      case "person" =>
        val values = ANNModel.train(arrayBuffer, personHashFunctions, candidateStrategy, distanceMeasure, persistLevel).computerHashValues()
        values
    }
  }

  def main(args: Array[String]) {

    //    val Array(tableNums) = args
    //    val sparkConf = new SparkConf().setAppName("LSHAnalysis")
    //    val sc = new SparkContext(sparkConf)
    val localPoints = Array(12.0, 0, 34.0, 45.2, 0, 32.6, 19.3, 0, 21.5, 65)
    val values = computerVectorHashValue(localPoints, "faceImage")
    for (i <- values)
      print(i + "    ")
  }

}

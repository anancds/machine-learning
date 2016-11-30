/**
  * Copyright (c) 2015, zhejiang Unview Technologies Co., Ltd.
  * All rights reserved.
  * <http://www.uniview.com/>
  * -----------------------------------------------------------
  * Product      :BigData
  * Module Name  :
  * Project Name :salut-parent
  * Package Name :com.uniview.salut.spark.traffic.lsh
  * Date Created :2016/6/13
  * Creator      :c02132
  * Description  :
  * -----------------------------------------------------------
  * Modification History
  * Date        Name          Description
  * ------------------------------------------------------------
  * 2016/6/13      c02132         BigData project,new code file.
  * ------------------------------------------------------------
  */
package com.cds.learn.lsh

import java.util.{Random => JavaRandom}

import com.cds.learn.lsh.collision.{CollisionStrategy, SimpleCollisionStrategy}
import com.cds.learn.lsh.linalg.{CosineDistance, DistanceMeasure}

import scala.util.Random
import org.apache.spark.mllib.linalg.{SignRandomProjectionFunction, SparseVector}
import org.apache.spark.storage.StorageLevel


class ANN(
           //距离公式
           private var measureName: String, //计算距离的方式
           private var origDimension: Int, //源数据的维度
           private var numTables: Int, //hash表的数目
           private var signatureLength: Int, //签名的长度
           private var bucketWidth: Double, //暂时未用到
           private var primeModulus: Int, //暂时未用到
           private var numBands: Int, //暂时未用到
           private var randomSeed: Int //随机种子数目，用来产生hash表
         ) {
  /**
    * Constructs an ANN instance with default parameters.
    */
  def this(dimensions: Int, measure: String) = {
    this(
      origDimension = dimensions,
      measureName = measure,
      numTables = 1,
      signatureLength = 16,
      bucketWidth = 0.0,
      primeModulus = 0,
      numBands = 0,
      randomSeed = Random.nextInt()
    )
  }

  /**
    * Number of hash tables to compute
    * 用来计算的hash表数目
    */
  def getTables: Int = {
    numTables
  }

  /**
    * Number of hash tables to compute
    * 用来计算的hash表数目
    */
  def setTables(tables: Int): this.type = {
    numTables = tables
    this
  }

  /**
    * Number of elements in each signature (e.g. # signature bits for sign-random-projection)
    */
  def getSignatureLength: Int = {
    signatureLength
  }

  /**
    * Number of elements in each signature (e.g. # signature bits for sign-random-projection)
    *
    * 每个签名的元素个数
    */
  def setSignatureLength(length: Int): this.type = {
    signatureLength = length
    this
  }

  /**
    * Bucket width (commonly named "W") used by scalar-random-projection hash functions.
    */
  def getBucketWidth: Double = {
    bucketWidth
  }

  /**
    * Bucket width (commonly named "W") used by scalar-random-projection hash functions.
    */
  def setBucketWidth(width: Double): this.type = {
    require(
      measureName == "euclidean" || measureName == "manhattan",
      "Bucket width only applies when distance measure is euclidean or manhattan."
    )
    bucketWidth = width
    this
  }

  /**
    * Common prime modulus used by minhash functions.
    */
  def getPrimeModulus: Int = {
    primeModulus
  }

  /**
    * Common prime modulus used by minhash functions.
    *
    * Should be larger than the number of dimensions.
    */
  def setPrimeModulus(prime: Int): this.type = {
    require(
      measureName == "jaccard",
      "Prime modulus only applies when distance measure is jaccard."
    )
    primeModulus = prime
    this
  }

  /**
    * Number of bands to use for minhash candidate pair generation
    * jaccard 距离才会用到bands参数
    */
  def getBands: Int = {
    numBands
  }

  /**
    * Number of bands to use for minhash candidate pair generation
    * jaccard 距离才会用到bands参数
    */
  def setBands(bands: Int): this.type = {
    require(
      measureName == "jaccard",
      "Number of bands only applies when distance measure is jaccard."
    )
    numBands = bands
    this
  }

  /**
    * Random seed used to generate hash functions
    * 随机种子用来产生hash函数
    */
  def getRandomSeed(): Int = {
    randomSeed
  }

  /**
    * Random seed used to generate hash functions
    * 随机种子用来产生hash函数
    */
  def setRandomSeed(seed: Int): this.type = {
    randomSeed = seed
    this
  }

  def getHashFunctions(origDimension: Int): Array[LSHFunction[_]] = {

    var hashFunctions: Array[LSHFunction[_]] = Array()
    val random = new JavaRandom(randomSeed)
    hashFunctions = (1 to getTables).map(i =>
      SignRandomProjectionFunction.generate(origDimension, getSignatureLength, random)).toArray
    hashFunctions
  }

  /**
    * Build an ANN model using the given dataset.
    * 用给定的数据集构建一个ANN模型
    *
    * @return ANNModel containing computed hash tables
    *         返回值是ANNModel对象，它包含计算的hash表
    */
  def train(
             points: Array[SparseVector],
             persistenceLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK
           ): ANNModel = {
    var hashFunctions: Array[LSHFunction[_]] = Array()
    var candidateStrategy: CollisionStrategy = new SimpleCollisionStrategy
    var distanceMeasure: DistanceMeasure = CosineDistance
    val random = new JavaRandom(randomSeed)
    measureName.toLowerCase match {
      case "cosine" => {
        distanceMeasure = CosineDistance
        hashFunctions = (1 to numTables).map(i =>
          SignRandomProjectionFunction.generate(origDimension, signatureLength, random)).toArray
      }
      case other: Any =>
        throw new IllegalArgumentException(
          "Only hamming, cosine, euclidean, manhattan, and jaccard distances are supported but got $other."
        )
    }
    ANNModel.train(
      points,
      hashFunctions,
      candidateStrategy,
      distanceMeasure,
      persistenceLevel
    )
  }

}

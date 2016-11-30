/**
  * Copyright (c) 2015, zhejiang Unview Technologies Co., Ltd.
  * All rights reserved.
  * <http://www.uniview.com/>
  * -----------------------------------------------------------
  * Product      :BigData
  * Module Name  :
  * Project Name :salut-parent
  * Package Name :com.uniview.salut.spark.traffic.lsh
  * Date Created :2016/6/12
  * Creator      :c02132
  * Description  :
  * -----------------------------------------------------------
  * Modification History
  * Date        Name          Description
  * ------------------------------------------------------------
  * 2016/6/12      c02132         BigData project,new code file.
  * ------------------------------------------------------------
  */
package com.cds.learn.lsh

import org.apache.spark.mllib.linalg.SparseVector

import scala.util.Random

object LshTestHelper {

  def generateRandomPoints(quantity: Int, dimensions: Int, density: Double): Array[SparseVector] = {
    val numElements = math.floor(dimensions * density).toInt
    val points = new Array[SparseVector](quantity)
    var i = 0
    while (i < quantity) {
      val indices = generateIndices(numElements, dimensions)
      val values = generateValues(numElements)
      points(i) = new SparseVector(dimensions, indices, values)
      i += 1
    }
    points
  }

  def generateIndices(quantity: Int, dimensions: Int) = {
    val indices = new Array[Int](quantity)
    var i = 0
    while (i < quantity) {
      val possible = Random.nextInt(dimensions)
      if (!indices.contains(possible)) {
        indices(i) = possible
        i += 1
      }
    }
    indices
  }

  def generateValues(quantity: Int) = {
    val values = new Array[Double](quantity)
    var i = 0
    while (i < quantity) {
      values(i) = Random.nextGaussian()
      i += 1
    }
    values
  }
}

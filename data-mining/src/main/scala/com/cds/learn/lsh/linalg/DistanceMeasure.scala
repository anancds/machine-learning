/**
  * Copyright (c) 2015, zhejiang Unview Technologies Co., Ltd.
  * All rights reserved.
  * <http://www.uniview.com/>
  * -----------------------------------------------------------
  * Product      :BigData
  * Module Name  :
  * Project Name :salut-parent
  * Package Name :com.uniview.salut.spark.traffic.lsh.linalg
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
package com.cds.learn.lsh.linalg

import org.apache.spark.mllib.linalg.{LinalgShim, SparseVector, Vectors}

private[lsh] sealed abstract class DistanceMeasure extends Serializable {
  def compute(v1: SparseVector, v2: SparseVector): Double
}

private[lsh] object CosineDistance extends DistanceMeasure {
  /**
    * Compute cosine distance between vectors
    *
    * LinalgShim reaches into Spark's private linear algebra
    * code to use a BLAS dot product. Could probably be
    * replaced with a direct invocation of the appropriate
    * BLAS method.
    */
  def compute(v1: SparseVector, v2: SparseVector): Double = {
    val dotProduct = LinalgShim.dot(v1, v2)
    val norms = Vectors.norm(v1, 2) * Vectors.norm(v2, 2)
    1.0 - (math.abs(dotProduct) / norms)
  }
}


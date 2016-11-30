package com.cds.learn.lsh

import java.util.Random

import breeze.stats.distributions.CauchyDistribution
import org.apache.spark.mllib.linalg.{DenseMatrix, DenseVector, Vector}

/**
  * A simple random projection based on Spark's existing
  * random generation and multiplication of dense matrices.
  */
class RandomProjection(
                        private[this] val matrix: DenseMatrix
                      ) extends Serializable {

  /**
    * Apply the projection to supplied vector
    */
  def project(vector: Vector): DenseVector = {
    //signatureLength * 1 的矩阵
    matrix.multiply(vector)
  }
}

object RandomProjection {

  /**
    * Generate a random projection based on the input and output
    * dimensions
    */
  def generateGaussian(originalDim: Int, projectedDim: Int, random: Random): RandomProjection = {
    val localMatrix = DenseMatrix.randn(projectedDim, originalDim, random)
    new RandomProjection(localMatrix)
  }

  def generateCauchy(originalDim: Int, projectedDim: Int, random: Random): RandomProjection = {
    def randc(numRows: Int, numCols: Int): DenseMatrix = {
      require(
        numRows.toLong * numCols <= Int.MaxValue,
        s"$numRows x $numCols dense matrix is too large to allocate"
      )
      val cauchyDistribution = new CauchyDistribution(0, 1)
      new DenseMatrix(numRows, numCols, cauchyDistribution.drawMany(numRows * numCols))
    }

    val localMatrix = randc(projectedDim, originalDim)
    new RandomProjection(localMatrix)
  }
}

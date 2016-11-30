package com.cds.learn.lsh

import com.cds.learn.lsh.collision.CollisionStrategy
import com.cds.learn.lsh.linalg.DistanceMeasure
import com.cds.learn.lsh.signatures.HashTableEntry
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
  * Model containing hash tables produced by computing signatures
  * for each supplied vector.
  */
class ANNModel(
                val hashTables: Array[_ <: HashTableEntry[_]],
                val hashFunctions: Array[_ <: LSHFunction[_]],
                val collisionStrategy: CollisionStrategy,
                val measure: DistanceMeasure,
                val numPoints: Int
              ) extends Serializable {

  type Point = (Long, SparseVector)
  type CandidateGroup = Iterable[Point]

  /**
    * Identify pairs of nearest neighbors by applying a
    * collision strategy to the hash tables and then computing
    * the actual distance between candidate pairs.
    * //    */
  //  def neighbors(quantity: Int): RDD[(Long, Array[(Long, Double)])] = {
  //    val candidates = collisionStrategy.apply(hashTables).groupByKey(hashTables.partitions.length).values
  //    val neighbors = computeDistances(candidates)
  //    neighbors.topByKey(quantity)(ANNModel.ordering)
  //  }
  //
  //
  def computerHashValues(): Array[Int] = {
    val hashValues = collisionStrategy.productHashValue(hashTables)
    hashValues
  }

  //
  //  /**
  //    * Identify the nearest neighbors of a collection of new points
  //    * by computing their signatures, filtering the hash tables to
  //    * only potential matches, cogrouping the two RDDs, and
  //    * computing candidate distances in the "normal" fashion.
  //    */
  //  def neighbors(queryPoints: RDD[Point], quantity: Int): RDD[(Long, Array[(Long, Double)])] = {
  //    val modelEntries = collisionStrategy.apply(hashTables)
  //
  //    val queryHashTables = ANNModel.generateHashTable(queryPoints, hashFunctions)
  //    val queryEntries = collisionStrategy.apply(queryHashTables)
  //
  //    val candidateGroups = modelEntries.cogroup(queryEntries).values
  //    val neighbors = computeBipartiteDistances(candidateGroups)
  //    neighbors.topByKey(quantity)(ANNModel.ordering)
  //  }
  //
  //  /**
  //    * Compute the average selectivity of the points in the
  //    * dataset. (See "Modeling LSH for Performance Tuning" in CIKM '08.)
  //    */
  //  def avgSelectivity(): Double = {
  //    val candidates = collisionStrategy.apply(hashTables).groupByKey(hashTables.partitions.length).values
  //    val candidateCounts =
  //      candidates
  //        .flatMap {
  //          case candidates => {
  //            for (
  //              (id1, vector1) <- candidates.iterator;
  //              (id2, vector2) <- candidates.iterator
  //            ) yield (id1, id2)
  //          }
  //        }
  //        .distinct()
  //        .countByKey()
  //        .values
  //
  //    candidateCounts.map(_.toDouble / numPoints).reduce(_ + _) / numPoints
  //  }

  /**
    * Compute the actual distance between candidate pairs
    * using the supplied distance measure.
    */
  private def computeDistances(candidates: RDD[CandidateGroup]): RDD[(Long, (Long, Double))] = {
    candidates
      .flatMap {
        case group => {
          for (
            (id1, vector1) <- group.iterator;
            (id2, vector2) <- group.iterator;
            if id1 < id2
          ) yield ((id1, id2), measure.compute(vector1, vector2))
        }
      }
      .reduceByKey((a, b) => a)
      .flatMap {
        case ((id1, id2), dist) => Array((id1, (id2, dist)), (id2, (id1, dist)))
      }
  }

  /**
    * Compute the actual distance between candidate pairs
    * using the supplied distance measure.
    */
  private def computeBipartiteDistances(candidates: RDD[(CandidateGroup, CandidateGroup)]): RDD[(Long, (Long, Double))] = {
    candidates
      .flatMap {
        case (groupA, groupB) => {
          for (
            (id1, vector1) <- groupA.iterator;
            (id2, vector2) <- groupB.iterator
          ) yield ((id1, id2), measure.compute(vector1, vector2))
        }
      }
      .reduceByKey((a, b) => a)
      .map {
        case ((id1, id2), dist) => (id1, (id2, dist))
      }
  }
}

object ANNModel {
  private val ordering = Ordering[Double].on[(Long, Double)](_._2).reverse

  /**
    * Train a model by computing signatures for the supplied
    * points
    */
  def train(
             points: Array[SparseVector],
             hashFunctions: Array[_ <: LSHFunction[_]],
             collisionStrategy: CollisionStrategy,
             measure: DistanceMeasure,
             persistenceLevel: StorageLevel
           ): ANNModel = {
    val hashTables: Array[_ <: HashTableEntry[_]] = generateHashTable(points, hashFunctions)
    //    hashTables.persist(persistenceLevel)
    new ANNModel(
      hashTables,
      hashFunctions,
      collisionStrategy,
      measure,
      points.length
    )
  }

  def generateHashTable(
                         points: Array[SparseVector],
                         hashFunctions: Array[_ <: LSHFunction[_]]
                       ): Array[_ <: HashTableEntry[_]] = {
    val indHashFunctions: Array[((_$1) forSome {type _$1 <: LSHFunction[_]}, Int)] = hashFunctions.zipWithIndex
    val results = ArrayBuffer[HashTableEntry[_]]()
    points.flatMap {
      vector =>
        indHashFunctions.map {
          case (hashFunc, table) =>
            //table的id，vector是原始数据，hashFunc是signatureLength * originalDim的高斯矩阵
            results += hashFunc.hashTableEntry(table, vector)
        }
    }
    results.toArray
  }
}

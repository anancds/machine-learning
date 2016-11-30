package com.cds.learn.lsh.collision

import com.cds.learn.lsh.signatures.HashTableEntry
import org.apache.spark.mllib.linalg.SparseVector

/**
  * Abstract base class for approaches to identifying collisions from
  * the pre-computed hash tables. This should be sufficiently
  * general to support a variety of collision and candidate identification
  * strategies, including multi-probe (for scalar-random-projection LSH),
  * and banding (for minhash LSH).
  */
abstract class CollisionStrategy {
  type Point = (Long, SparseVector)

  //  def apply(hashTables: RDD[_ <: HashTableEntry[_]]): RDD[(Product, Point)]

  def productHashValue(hashTables: Array[_ <: HashTableEntry[_]]): Array[Int]
}

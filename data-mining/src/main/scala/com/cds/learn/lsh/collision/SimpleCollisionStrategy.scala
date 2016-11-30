package com.cds.learn.lsh.collision

import com.cds.learn.lsh.signatures.HashTableEntry

import scala.collection.mutable.ArrayBuffer
import scala.util.hashing.MurmurHash3

/**
  * A very simple collision strategy for candidate identification
  * based on an OR-construction between hash functions/tables
  *
  * (See Mining Massive Datasets, Ch. 3)
  */
class SimpleCollisionStrategy extends CollisionStrategy with Serializable {

  /**
    * Convert hash tables into an RDD that is "collidable" using groupByKey.
    * The new keys contain the hash table id, and a hashed version of the signature.
    */
  //  def apply(hashTables: RDD[_ <: HashTableEntry[_]]): RDD[(Product, Point)] = {
  //    val entries = hashTables.map(entry => {
  //      // Arrays are mutable and can't be used in RDD keys
  //      // Use a hash value (i.e. an int) as a substitute
  //      val key = (entry.table, MurmurHash3.arrayHash(entry.sigElements)).asInstanceOf[Product]
  //      (key, (entry.id, entry.point))
  //    })
  //    entries
  //  }

  def productHashValue(hashTables: Array[_ <: HashTableEntry[_]]): Array[Int] = {

    val values = new ArrayBuffer[Int]
    for (i <- hashTables.indices) {
      val value = MurmurHash3.arrayHash(hashTables(i).sigElements)
      values += value
    }
    values.toArray

    //    val hashValue = hashTables.map(entry => {
    //      val value = MurmurHash3.arrayHash(entry.sigElements)
    //      value
    //    })
    //    hashValue
  }
}



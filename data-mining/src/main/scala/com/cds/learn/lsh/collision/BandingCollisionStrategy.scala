package com.cds.learn.lsh.collision

import com.cds.learn.lsh.signatures.HashTableEntry

import scala.collection.mutable.ArrayBuffer
import scala.util.hashing.MurmurHash3

/**
  * A banding collision strategy for candidate identification with Minhash
  */
class BandingCollisionStrategy(
                                bands: Int
                              ) extends CollisionStrategy with Serializable {

  /**
    * Convert hash tables into an RDD that is "collidable" using groupByKey.
    * The new keys contain the hash table id, the band id, and a hashed version
    * of the banded signature.
    */
  //  def apply(hashTables: RDD[_ <: HashTableEntry[_]]): RDD[(Product, Point)] = {
  //    val bandEntries = hashTables.flatMap(entry => {
  //      val banded = entry.sigElements.grouped(bands).zipWithIndex
  //      banded.map {
  //        case (bandSig, band) => {
  //          // Arrays are mutable and can't be used in RDD keys
  //          // Use a hash value (i.e. an int) as a substitute
  //          val bandSigHash = MurmurHash3.arrayHash(bandSig)
  //          val key = (entry.table, band, bandSigHash).asInstanceOf[Product]
  //          (key, (entry.id, entry.point))
  //        }
  //      }
  //    })
  //    bandEntries
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
    //      if (value >= 0) {
    //        entry.table.toString + "+" + value.toString
    //      } else {
    //        entry.table.toString + value.toString
    //      }
    //    })
    //    hashValue
  }
}

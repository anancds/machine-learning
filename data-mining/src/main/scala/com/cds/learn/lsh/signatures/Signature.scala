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
package com.cds.learn.lsh.signatures

import org.apache.spark.mllib.linalg.SparseVector

import scala.collection.immutable.BitSet

/**
  * This wrapper class allows ANNModel to ignore the
  * type of the hash signatures in its hash tables.
  */
sealed trait Signature[+T] extends Any {
  val elements: T
}

final case class BitSignature(elements: BitSet) extends AnyVal with Signature[BitSet]

//final case class IntSignature(elements: Array[Int]) extends AnyVal with Signature[Array[Int]]

/**
  * A hash table entry containing an id, a signature, and
  * a table number, so that all hash tables can be stored
  * in a single RDD.
  */
sealed abstract class HashTableEntry[+S <: Signature[_]] {
  val table: Int
  val signature: S
  val point: SparseVector

  def sigElements: Array[Int]
}

final case class BitHashTableEntry(
                                    table: Int,
                                    signature: BitSignature,
                                    point: SparseVector
                                  ) extends HashTableEntry[BitSignature] {
  def sigElements: Array[Int] = {
    signature.elements.toArray
  }
}

//
//final case class IntHashTableEntry(
//                                    id: Long,
//                                    table: Int,
//                                    signature: IntSignature,
//                                    point: SparseVector
//                                  ) extends HashTableEntry[IntSignature] {
//  def sigElements: Array[Int] = {
//    signature.elements
//  }
//}

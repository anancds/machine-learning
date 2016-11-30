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

import com.cds.learn.lsh.signatures.{HashTableEntry, Signature}
import org.apache.spark.mllib.linalg.SparseVector

abstract class LSHFunction[+T <: Signature[_]] {

  /**
    * Compute the hash signature of the supplied vector
    */
  def signature(v: SparseVector): T

  /**
    * Build a hash table entry for the supplied vector
    */
  def hashTableEntry(table: Int, v: SparseVector): HashTableEntry[T]

}

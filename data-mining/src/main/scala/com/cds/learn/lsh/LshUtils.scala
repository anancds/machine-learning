package com.cds.learn.lsh

import com.cds.learn.lsh.json.Json4sUtills
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.sql.Row

import scala.collection.JavaConversions._

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

object LshUtils {

  val LOG = Logger.getLogger(getClass)

  var isCluster: Boolean = _
  var slaveIps: String = _
//  var confUtil: ConfigInfoUtil = null
//
//  //偏java的写法，是否有更好的scala写法。
//  def parseConfig() = {
//    try {
//      confUtil = new ConfigInfoUtil(true)
//      isCluster = confUtil.getValue("salut.isCluster").toBoolean
//      slaveIps = confUtil.getValue("salut.nodes")
//      confUtil.clear()
//      true
//    } catch {
//      case e: Exception =>
//        confUtil.clear()
//        LOG.error("Load config error, for ", e)
//        false
//    }
//  }

  def getSolrUrl(isCluseter: Boolean) = {
    "abc"
  }

  /*def getSolrClient(isCluster: Boolean) = {

    val solrClient = if (isCluster) SolrSupport.getCachedCloudClient("ac")
    else SolrSupport
      .getHttpSolrClient("agc")

    solrClient
  }*/

  //根据目前情况使用该函数，并不是余弦距离，而是（1-余弦距离）
  def cosineDistance(v1: Vector, v2: Vector): Double = {
    val dotProduct = LinalgShim.dot(v1, v2)
    val norms = Vectors.norm(v1, 2) * Vectors.norm(v2, 2)
    math.abs(dotProduct) / norms
  }

  def cosineDistance(v1: Vector, v2: Vector, norm1: Double, norm2: Double) = {
    val dotProduct = LinalgShim.dot(v1, v2)
    val norms = norm1 * norm2
    math.abs(dotProduct) / norms
  }

  //  def lshTrain(dim: Int, rdd: RDD[(Long, SparseVector)], tables: Int = 4,
  //               signatures: Int = 64): ANNModel = {
  //    val annModel = new ANN(dimensions = dim, measure = "cosine").setTables(tables)
  //      .setSignatureLength(signatures).train(rdd)
  //    annModel
  //  }

  //  def getNeighbors(model: ANNModel, num: Int = 10): RDD[(Long, Array[(Long, Double)])] = {
  //    val neighbors = model.neighbors(num)
  //    neighbors
  //  }

  /**
    * 把solr的文档转成spark的RDD
    *
    * @param docs solr 文档集合
    * @return
    */
//  def changeDocsToRDD(docs: SolrDocumentList, sc: SparkContext, business: String) = {
//
//    //    //isInstance判断下
//    business match {
//      case "traffic" =>
//        val ids = docs.map(doc => Row(doc.getFieldValue("record_id").toString.toLong))
//        val rdd = sc.parallelize(ids)
//        rdd.distinct()
//
//      case "faceImage" =>
//        val ids = docs.map(doc => Row(doc.getFieldValue("dul_snap_id").toString.toLong))
//        val rdd = sc.parallelize(ids)
//        rdd.distinct()
//
//      case "video" =>
//        val ids = docs.map(doc => Row(doc.getFieldValue("record_id").toString.toLong))
//        val rdd = sc.parallelize(ids)
//        rdd.distinct()
//
//    }
//  }

//  def changeDocToRDD(doc: SolrDocument, sc: SparkContext) = {
//    val solrDocumentList = new SolrDocumentList()
//    solrDocumentList.add(doc)
//    changeDocsToRDD(solrDocumentList, sc, "traffic")
//  }

  def getFeatures(array: Array[Double]) = {
    val features = LSHAnalysis.computerVectorHashValue(array, "faceImage")
    LOG.info("getFeatures features is" + features.length +"  and features(0):"+ features(0))
    var res = ""
    val result = if (features.length > 0) {
      res += " AND ( "
      for (feature <- features) {
        res += ("features: " + feature + " OR ")
      }
      res = res.dropRight(3)
      res += " )"
      res
    } else {
      LOG.error("getFeatures failed, the features.length is 0 .")
      res
    }
    result
  }

//  def getResponse(isCluster: Boolean, business: String) = {
//    val query = business match {
//      case "faceImage" => getQuery() + " pass_time: " + Json4sUtills.time +
//        getFeatures(Json4sUtills.feature.toArray)
//      case "traffic" => getQuery() + " pass_time2: " + Json4sUtills.time
//      case _ => getQuery() + " start_time: " + Json4sUtills.start_time + " AND end_time: " + Json4sUtills.end_time
//    }
//    LOG.info("solr query is :" + query)
//    val queryMap = Json4sUtills.jsonToMap(query)
//    val _response = SolrIrisQuery.query(Json4sUtills.solrCore, queryMap, slaveIps, isCluster)
//    _response
//  }

  def getQuery() = {
    val result = if (Json4sUtills.query.trim.equals("") || Json4sUtills.query.trim.equals("*:*")) ""
    else {
      Json4sUtills.query + " AND"
    }
    result
  }
}

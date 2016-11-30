/**
  * Copyright (c) 2016, zhejiang Unview Technologies Co., Ltd.
  * All rights reserved.
  * <http://www.uniview.com/>
  * -----------------------------------------------------------
  * Product      :BigData
  * Module Name  :
  * Project Name :salut-parent
  * Package Name :com.uniview.salut.spark.traffic.lsh.json
  * Date Created :2016/6/15
  * Creator      :c02132
  * Description  :
  * -----------------------------------------------------------
  * Modification History
  * Date        Name          Description
  * ------------------------------------------------------------
  * 2016/6/15      c02132         BigData project,new code file.
  * ------------------------------------------------------------
  */
package com.cds.learn.lsh.json

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.cds.learn.lsh.JsonUtils
import org.apache.log4j.Logger
import org.json4s.JsonAST.{JField, JObject, JString}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object Json4sUtills {
  private val LOG = Logger.getLogger(getClass)
  var query: String = _
  var feature: List[Double] = _
  var rowCount: String = _
  var offSet: String = _
  var solrCore: String = _
  var slaveIps: String = _
  var ordered: String = _
  var similarity: Double = _
  //traffic 业务的0x1矩阵比较块
  var compareBlock: List[Double] = _

  //视图库中的video服务需要用到begTime和endTime。begTime和endTime是日期类型，用来读Alluxuio。
  var begTime: String = _
  var endTime: String = _
  //time是traffic和FaceImage的查询solr条件，精确到秒的时间戳。
  var time: String = _

  //start_time和end_time是video业务的查询solr条件，精确到秒的时间戳。
  var start_time: String = _
  var end_time: String = _


  val sdf = new SimpleDateFormat("yyyy-MM-dd")

  def parseWithJackson(str: String, business: String) = {
    try {
      val jsonUtils = new JsonUtils()
      val map: util.Map[String, Any] = jsonUtils.fromJson(str, classOf[util.HashMap[String, Any]])

      feature = map.get("features").asInstanceOf[util.ArrayList[Double]].toList
      query = map.getOrElse("q", "*:*").toString
      similarity = try{map.getOrElse("similarity", 0.0).toString.toDouble/100} catch {
        case e:Exception =>
          LOG.error("the similarity is wrong!")
          0.0
      }
      rowCount = map.getOrElse("rowCount", "20").toString
      offSet = map.getOrElse("offSet", "0").toString
      solrCore = map.get("core").toString
      business match {
        case "faceImage" => time = map.get("pass_time").toString
        case "traffic" => time = map.get("pass_time2").toString
          compareBlock = map.get("compare_block").asInstanceOf[util.ArrayList[Int]].map(_.toDouble).toList
        case _ => val begTime_tmp = map.get("start_time").toString.trim
          val endTime_tmp = map.get("end_time").toString.trim
          time = null

          start_time = "[* TO " + endTime_tmp + "]"
          end_time = "[" + begTime_tmp + " TO *]"
          begTime = sdf.format(new Date(begTime_tmp.toLong * 1000))
          endTime = sdf.format(new Date(endTime_tmp.toLong * 1000))
      }

      if (time != null) {
        val arrayTime = time.trim.split("TO")
        val _begTime = arrayTime(0).drop(1).trim
        val _endTime = arrayTime(1).dropRight(1).trim
        begTime = sdf.format(new Date(_begTime.toLong * 1000))
        endTime = sdf.format(new Date(_endTime.toLong * 1000))
      }
    } catch {
      case e: Exception => {
        LOG.error("The json format is wrong!")
        throw  new Exception("The json format is wrong")
      }
    }

  }

//  def parseJson(str: String, business: String) = {
//
//    //是否需要try catch
//    LOG.info("parseJson in str :" + str)
//
//    val json = parse(StringInput(str))
//    val _query: List[String] = for (JObject(queries) <- json; JField("q", JString(q)) <- queries)
//      yield q
//    query = _query.headOption.orNull
//
//    val _rowCount: List[String] = for (JObject(queries) <- json; JField("rowCount", JString(q))
//    <- queries) yield q
//    rowCount = _rowCount.headOption.orNull
//
//    val _offSet: List[String] = for (JObject(queries) <- json; JField("offSet", JString(q)) <-
//    queries) yield q
//    offSet = _offSet.headOption.orNull
//
//    val _feature: List[List[JValue]] = for (JObject(queries) <- json; JField("features", JArray
//      (q)) <- queries) yield q
//    val _featureTmp = _feature.headOption.orNull
//
//    if (_featureTmp != null && _featureTmp.isInstanceOf[List[JDouble]]) {
//      feature = for (JDouble(a) <- _featureTmp) yield a
//    }
//
//    //根据solrCore来判断不同的业务。
//    val _solrCore: List[String] = for (JObject(queries) <- json; JField("core", JString(q)) <-
//    queries) yield q
//    solrCore = _solrCore.headOption.orNull
//
//    business match {
//      case "faceImage" => val _time: List[String] = for (JObject(queries) <- json; JField
//        ("pass_time", JString(q)) <- queries) yield q
//        val _timeTmp = _time.headOption.orNull
//        time = _timeTmp
//      case "traffic" => val _time: List[String] = for (JObject(queries) <- json; JField
//        ("pass_time2", JString(q)) <- queries) yield q
//        val _timeTmp = _time.headOption.orNull
//        time = _timeTmp
//      case _ => val _begTime: List[String] = for (JObject(queries) <- json; JField
//        ("start_time", JString(q)) <- queries) yield q
//        val _begTimeTmp = _begTime.headOption.orNull
//        begTime = _begTimeTmp
//
//        val _endTime: List[String] = for (JObject(queries) <- json; JField
//          ("end_time", JString(q)) <- queries) yield q
//        val _endTimeTmp = _endTime.headOption.orNull
//        begTime = _endTimeTmp
//
//    }
//  }

  def jsonToMap(_query: String): Map[String, String] = {
    val solrMap = Map("q" -> _query)
    solrMap
  }

 /* def renderJson(code: Int, message: String, ulTotalNum: Long, offSet: Int, ulRealNum: Long, resultList:
  List[util.HashMap[String, String]]) = {

    val jsonUtils = new JsonUtils()
    jsonUtils.put("code", code.toString)
    jsonUtils.put("message", message)
    jsonUtils.put("ulTotalNum", ulTotalNum.toString)
    jsonUtils.put("offSet", offSet.toString)
    jsonUtils.put("ulRealNum", ulRealNum.toString)
    jsonUtils.put("idList", resultList.toArray)
    jsonUtils.toJson
  }*/

  def renderJson(code: Int, message: String, ulTotalNum: Long, offSet: Int, ulRealNum: Long, resultArray:
  Array[util.HashMap[String, String]]) = {
    val jsonUtils = new JsonUtils()
    jsonUtils.put("code", code.toString)
    jsonUtils.put("message", message)
    jsonUtils.put("ulTotalNum", ulTotalNum.toString)
    jsonUtils.put("offSet", offSet.toString)
    jsonUtils.put("ulRealNum", ulRealNum.toString)
    jsonUtils.put("idList", resultArray)
    jsonUtils.toJson
  }

  def main(args: Array[String]) {
    val str = "{\"bHasQueryCondition\":true,\"bHasQueryPageInfo\":true,\"stCommonQueryCondition\":{\"ulItemNum\":9," +
      "\"astQueryConditionList\":[{\"szQueryColumn\":\"camera_id\",\"szQueryData\":\"HIC5421HI-L_49_1\"," +
      "\"ulLogicFlag\":99},{\"szQueryColumn\":\"pass_time\",\"szQueryData\":\"2016-07-07 00:00:00\"," +
      "\"ulLogicFlag\":3},{\"szQueryColumn\":\"pass_time\",\"szQueryData\":\"2016-07-08 20:33:33\"," +
      "\"ulLogicFlag\":4},{\"szQueryColumn\":\"pass_time\",\"szQueryData\":\"\",\"ulLogicFlag\":7}," +
      "{\"szQueryColumn\":\"ul_gender\",\"szQueryData\":\"0\",\"ulLogicFlag\":0},{\"szQueryColumn\":\"ul_glasses\"," +
      "\"szQueryData\":\"0\",\"ulLogicFlag\":0},{\"szQueryColumn\":\"ul_age\",\"szQueryData\":\"0\"," +
      "\"ulLogicFlag\":3},{\"szQueryColumn\":\"ul_age\",\"szQueryData\":\"80\",\"ulLogicFlag\":4}," +
      "{\"szQueryColumn\":\"similarity\",\"szQueryData\":0,\"ulLogicFlag\":3},{\"szQueryColumn\":\"features\"," +
      "\"szQueryData\":\"0.047436,-0.081493,-0.201170,-0.131636,0.054726,-0.008160,-0.173765,0.000132,0.048224,0" +
      ".005493,0.055992,0.007887,-0.021601,0.057923,0.100048,-0.276222,-0.166797,0.039687,-0.005145,0.070250,0" +
      ".107689,0.069615,0.058260,-0.082034,0.018068,0.007724,0.059948,0.023604,0.022690,-0.024676,0.051446,0.082916,0" +
      ".142693,0.230417,0.131481,0.025718,-0.027427,0.000076,0.055268,0.153405,0.006165,0.013441,-0.106977,0.027991," +
      "-0.104288,0.119252,-0.012912,0.187834,-0.056052,0.017144,-0.004237,-0.060366,0.147220,0.006317,-0.017635,0" +
      ".113991,0.017141,0.055273,0.051664,-0.137105,0.062378,0.066726,-0.092695,-0.043695,-0.181893,-0.001527,0" +
      ".057410,-0.006668,-0.034863,-0.056698,0.026169,0.010402,0.008494,0.030023,-0.030060,0.054411,0.051314,0" +
      ".002083,-0.179153,-0.091932,0.078996,-0.140674,-0.009281,-0.233266,0.042282,0.072833,0.013907,0.237529,-0" +
      ".075210,0.031956,-0.025682,-0.009640,-0.019446,0.038620,0.067491,-0.090340,-0.070995,0.061904,-0.036664,0" +
      ".151684,0.011982,0.089324,0.073899,-0.082876,0.082935,0.010982,0.065859,0.093998,-0.113121,-0.166829,-0" +
      ".066244,0.115128,-0.054994,0.009681,0.051678,0.018899,-0.018702,-0.037896,0.091439,0.042253,-0.044349,-0" +
      ".063886,0.000433,0.038153,-0.067728,-0.026975,-0.150971,0.094982,0.000000,0.000000,0.000000,0.000000,0.000000," +
      "0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0" +
      ".000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0" +
      ".000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0" +
      ".000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000\"," +
      "\"ulLogicFlag\":0}]},\"stQueryPageInfo\":{\"ulPageRowNum\":10,\"ulPageFirstRowNumber\":0,\"bQueryCount\":true}}"
    //    parseJson((str).stripMargin, "faceImage")

    //parseWithJackson(str, "faceImage")

    val map = new util.HashMap[String, String]()
    map.put("abc", "123")
    val map2 = new util.HashMap[String, String]()
    map2.put("efg", "456")
    val list = new ListBuffer[util.HashMap[String, String]]
    list.add(map)
    list.add(map2)
    val jsonutil = new JsonUtils()
    jsonutil.put("key", list.toArray)
    println(jsonutil.toJson)


    //    println(renderJson(1, "message", 22, 22, null))
  }

}

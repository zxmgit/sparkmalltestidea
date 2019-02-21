package com.zxm.sparkmall.realtime.handler

import com.zxm.sparkmall.common.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods

/**
  * @author zxm
  * @create 2019/2/20 - 15:09
  */
object AreaTop3AdsHandler {
    def handler(dayCityAdsCountDstream:DStream[(String, Long)]): Unit ={
        // 1 ("date:area:city:ads", count)
        // 去掉city字段
        val dayAreaAdsCountDstream = dayCityAdsCountDstream.map { case (key, count) => {
            val keyArr = key.split(":")
            val day = keyArr(0)
            val area = keyArr(1)
            val adsId = keyArr(3)
            val keyNoCity = day + ":" + area + ":" + adsId
            (keyNoCity, count)
        }
        }.reduceByKey(_ + _)
        //("date:area:ads", count) -> (date,(area,(ads, count)) -> (date,iterable[(area,(ads, count)])
        val areaAdsCountGroupbyDayDstream = dayAreaAdsCountDstream.map { case (key, count) =>
            val keyArr = key.split(":")
            val day : String = keyArr(0)
            val area : String = keyArr(1)
            val adsId : String = keyArr(2)
            (day, (area, (adsId, count)))
        }.groupByKey
        //按地区分组
        val areaAdsTop3JsonCountGroupbyDayDstream = areaAdsCountGroupbyDayDstream.map { case (day, areaItr) =>
            val AdsCountGroupbyAreaMap = areaItr.groupBy { case (area, (ads, count)) => area }
            //(area, Itr(area, (ads, count))),把itr中的area去掉,排序,取前三,变json
            val adsTop3CountGroupbyAreaMap = AdsCountGroupbyAreaMap.map { case (area, areaAdsItr) =>
                val areaAdsTop3List = areaAdsItr.map { case (area, (ads, count)) => (ads, count) }.toList.sortWith(_._2 > _._2).take(3)
                val areaAdsTop3JsonString = JsonMethods.compact(JsonMethods.render(areaAdsTop3List))
                (area, areaAdsTop3JsonString)
            }
            (day, adsTop3CountGroupbyAreaMap)
        }


        //存入Redis中
        areaAdsTop3JsonCountGroupbyDayDstream.foreachRDD{rdd =>

            rdd.foreachPartition{ dayItr=>
                val jedis = RedisUtil.getJedisClient
                dayItr.foreach{case (day, areaMap) =>
                    import  collection.JavaConversions._
                    jedis.hmset("area_top3_ads:"+day,areaMap);
                }
                jedis.close()
            }
        }
    }
}
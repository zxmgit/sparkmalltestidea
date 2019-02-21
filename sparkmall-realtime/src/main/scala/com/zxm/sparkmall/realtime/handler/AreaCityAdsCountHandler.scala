package com.zxm.sparkmall.realtime.handler

import com.zxm.sparkmall.common.util.RedisUtil
import com.zxm.sparkmall.realtime.bean.AdsLog
import org.apache.spark.streaming.dstream.DStream

/**
  * @author zxm
  * @create 2019/2/20 - 10:24
  */
object AreaCityAdsCountHandler {
    def handler(adsLogDsteam: DStream[AdsLog]): DStream[(String, Long)] ={
        //1 将dstream整理为key,v
        val adsClickDstream = adsLogDsteam.map { adsLog =>
            val key = adsLog.getDate() + ":" + adsLog.area + ":" + adsLog.city + ":" + adsLog.adsId
            (key, 1L)
        }
        //2
        val adsClickCountDstream = adsClickDstream.updateStateByKey { (countSeq: Seq[Long], total: Option[Long]) =>
            //countSeq为每个key对应的序列
            val countSum = countSeq.sum
            //total为之前对每个key聚合的结果,在此基础上加下一个时间段的
            val curTotal = total.getOrElse(0L) + countSum
            Some(curTotal)
        }
        adsClickCountDstream.foreachRDD{rdd =>
            //3 保存到Redis
            rdd.foreachPartition{ adsClickCountItr =>
                val jedis = RedisUtil.getJedisClient
                adsClickCountItr.foreach{ case(key, count) =>
                    jedis.hset("date:area:city:ads", key, count.toString)
                }
                jedis.close()
            }
        }
        adsClickCountDstream
    }
}
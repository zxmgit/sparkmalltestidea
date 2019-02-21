package com.zxm.sparkmall.realtime.handler

import com.zxm.sparkmall.common.util.RedisUtil
import com.zxm.sparkmall.realtime.bean.AdsLog
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream

/**
  * @author zxm
  * @create 2019/2/19 - 16:41
  */
object  BlackListHandler {
    def handler(adsLogDsteam: DStream[AdsLog]) = {
        //统计每日每用户每广告的点击次数
        //某一时间       area city userid adid
        //1550573351293 华北 北京 1 2
        val clickCountPerDayUserAdDsteam = adsLogDsteam.map{adsLog => (adsLog.getDate() + "_" + adsLog.userId + "_" + adsLog.adsId, 1L)}.reduceByKey(_+_)


        clickCountPerDayUserAdDsteam.foreachRDD(rdd => {
//            val prop = PropertiesUtil.load("config.properties")

            rdd.foreachPartition(adsItr => {
                //建立Redis连接
//                val jedis = new Jedis(prop.getProperty("redis.host"), prop.getProperty("redis.port").toInt)//driver
                val jedis = RedisUtil.getJedisClient
                //redis结构: hash key:day field:userId_adId value:count
                adsItr.foreach{ case (logkey, count) =>
                    val logArr = logkey.split("_")
                    val day = logArr(0)
                    val userId = logArr(1)
                    val adId = logArr(2)
                    val key = "user_ads_click:" + day
                    //在上面已经聚合完成,但存储在Redis中还需要新的格式
                    jedis.hincrBy(key, userId + "_" + adId, count)

                    val curCount = jedis.hget(key, userId + "_" + adId)
                    if(curCount.toLong >= 100){
                        //每日每用户点击某广告超过100次的用户进入黑名单
                        //黑名单key的类型:只需要存储userId就行,所以用set
                        //string list set hash zset.
                        jedis.sadd("blackList", userId)
                    }
                }
                //使用连接池时,close将连接放回到池中,并不是丢失连接.
                jedis.close()
            })
        })
    }
    def check(sparkContext: SparkContext, adsLogDsteam: DStream[AdsLog]): DStream[AdsLog] ={
        val adsLogFilter = adsLogDsteam.transform { rdd =>
            //driver,每个时间间隔执行一次,确保取到blacklist实时更新的数据
//            val prop = PropertiesUtil.load("config.properties")
//            val jedis = new Jedis(prop.getProperty("redis.host"), prop.getProperty("redis.port").toInt)
            val jedis = RedisUtil.getJedisClient
            val blackListSet = jedis.smembers("blackList")
            val blackListBC = sparkContext.broadcast(blackListSet)
            rdd.filter { adslog =>
                println(blackListBC.value)
                !blackListBC.value.contains(adslog.userId)
            }
        }
        adsLogFilter
    }

}

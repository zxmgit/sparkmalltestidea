package com.zxm.sparkmall.realtime.app

import com.zxm.sparkmall.common.util.MyKafkaUtil
import com.zxm.sparkmall.realtime.bean.AdsLog
import com.zxm.sparkmall.realtime.handler.{AreaCityAdsCountHandler, AreaTop3AdsHandler, BlackListHandler}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 消费kafka数据,需要连接kafka的util
  *
  * @author zxm
  * @create 2019/2/19 - 16:08
  */
object RealTimeApp {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("realtime")
        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, Seconds(5))
        sc.setCheckpointDir("./checkpoint")

        val recordDsteam = MyKafkaUtil.getKafkaStream("ads_log", ssc)

        //某一时间       area city userid adid
        //1550573351293 华北 北京 1 2
        val adsLogDsteam = recordDsteam.map(_.value()).map { log =>
            val logArr = log.split(" ")
            AdsLog(logArr(0).toLong, logArr(1), logArr(2), logArr(3), logArr(4))
        }

        //需求三完成
        //Redis删除某个键的命令:del <key>
        val adsLogFilter = BlackListHandler.check(sc, adsLogDsteam)
        BlackListHandler.handler(adsLogFilter)

        //需求四
        //redis-cli --raw命令开启客户端,可以显示中文
        val dayCityAdsCountDstream = AreaCityAdsCountHandler.handler(adsLogFilter)

        //需求五
        AreaTop3AdsHandler.handler(dayCityAdsCountDstream)
        //启动接收器
        ssc.start()
        //阻塞main方法
        ssc.awaitTermination()
    }
}

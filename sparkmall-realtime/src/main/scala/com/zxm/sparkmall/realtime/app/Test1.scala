package com.zxm.sparkmall.realtime.app

import org.apache.kafka.common.serialization.StringDeserializer

/**
  * @author zxm
  * @create 2019/2/21 - 14:30
  */
object Test1 {
    def main(args: Array[String]): Unit = {
        val unit = classOf[StringDeserializer]
        println(unit)
    }
}

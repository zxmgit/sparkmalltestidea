package com.zxm.sparkmall.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

/**
  * @author zxm
  * @create 2019/2/19 - 19:54
  */
case class AdsLog(ts:Long, area:String, city:String, userId:String, adsId:String) {

    private val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

    def getDate(): String ={
        dateFormat.format(new Date(ts))
    }
}

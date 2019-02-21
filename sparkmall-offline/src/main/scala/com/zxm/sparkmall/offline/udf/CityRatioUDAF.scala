package com.zxm.sparkmall.offline.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap
import scala.math.Ordering

/**
  * @author zxm
  * @create 2019/2/19 - 10:53
  */
//class CityRatioUDAF extends UserDefinedAggregateFunction{
//    /**
//      * 定义输入类型
//      * @return
//      */
//    override def inputSchema: StructType = StructType(Array(StructField("city_name", StringType)))
//    //之所以是数组,因为根据area和click_product_id分组,那么一条area和click_product_id对应多个cityname数据,聚合时每一次聚合每个单元格(有多个cityname)的数据.
//
//    /**
//      * 定义存储类型 Map,Long
//      * @return
//      */
//    override def bufferSchema: StructType = StructType(Array(StructField("city_count",MapType(StringType,LongType)),StructField("total_count",LongType) ))
//
//    override def dataType: DataType = StringType
//
//    /**
//      * 相同的输入是否有相同的输出,如果一样就返回true
//      * @return
//      */
//    override def deterministic: Boolean = true
//
//    /**
//      * 存储的初始化
//      * @param buffer
//      */
//    override def initialize(buffer: MutableAggregationBuffer): Unit = {
//        buffer(0) = new HashMap[String, Long]
//        buffer(1) = 0L
//    }
//
//    /**
//      * 更新, 每到一条数据做一次更新,输入以什么格式更新到存储.executor
//      * @param buffer
//      * @param input
//      */
//    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
//        //getAs将数据结构化
//        val cityCountMap = buffer.getAs[HashMap[String, Long]](0)
//        val totalCount = buffer.getLong(1)
//        //city_ratio(city_name),将城市名传进来,进行加和
//        val cityName = input.getString(0)
//
//
//        buffer(0) = cityCountMap + (cityName -> (cityCountMap.getOrElse(cityName, 0L) + 1L))
//
//        buffer(1) = totalCount + 1L
//
//    }
//
//    /**
//      * 合并,每个分区处理完成汇总到driver时合并.
//      * @param buffer1
//      * @param buffer2
//      */
//    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
//        val cityCountMap1: Map[String, Long] = buffer1.getAs[Map[String,Long]](0)
//        val totalCount1: Long = buffer1.getLong(1)
//        val cityCountMap2: Map[String, Long] = buffer2.getAs[Map[String,Long]](0)
//        val totalCount2: Long = buffer2.getLong(1)
//
//        buffer1(0) = cityCountMap1.foldLeft(cityCountMap2){
//           case (cityCountMap2, (cityname, count)) =>
//                cityCountMap2 + (cityname -> (cityCountMap2.getOrElse(cityname, 0L) + count))
//        }
//
//        buffer1(1) = totalCount1 + totalCount2
//
//
////        buffer1(0) = cityCountMap1.foldLeft(cityCountMap2) { case (cityCountMap2, (cityName1, count1)) =>
////            cityCountMap2 + (cityName1 -> (cityCountMap2.getOrElse(cityName1, 0L) + count1))
////        }
////
////        buffer1(1)=totalCount1+totalCount2
//    }
//
//    /**
//      * 把存储中的数据展示出来
//      * @param buffer
//      * @return
//      */
//    override def evaluate(buffer: Row): Any = {
//        //每个地区每个广告点击所在的城市总数
//        val totalCount = buffer.getLong(1)
//        val cityCountMap = buffer.getAs[Map[String, Long]](0)
//
//        //备注为list
//        val cityRatioInfoList = new mutable.ListBuffer[CityRatioInfo]()
//        //1 计算百分比
//        val cityRatioList = cityCountMap.map { case (cityName, count) =>
//            val cityRatio = math.round(count.toDouble / totalCount * 1000) / 10D
//            CityRatioInfo(cityName, cityRatio)
//        }.toList
//        //2 排序取前2
//        var cityRatioTop2 = cityRatioInfoList.sortBy(_.cityRatio)(Ordering.Double.reverse).take(2)
////        cityRatioInfoList.sortWith(_.cityRatio > _.cityRatio)
//
//        //3 把其他计算出来
//        if (cityRatioInfoList.size > 2){
//            var otherRatio = 100D
//            cityRatioInfoList.foreach(cityRatioInfo => otherRatio -= cityRatioInfo.cityRatio)
//            cityRatioTop2 = cityRatioTop2 :+ CityRatioInfo("其他", otherRatio)
//        }
//
//        //4 拼接成string
//        cityRatioTop2.mkString(",")
//    }
//
//    case class CityRatioInfo(cityName:String, cityRatio:Double){
//        override def toString: String = {
//            cityName + ":" + cityRatio + "%"
//        }
//    }
//}

class CityRatioUDAF extends UserDefinedAggregateFunction{
    //定义输入 类型 String
    override def inputSchema: StructType =  StructType(Array(StructField("city_name",StringType)  ))
    //定义存储类型 类型 Map, Long
    override def bufferSchema: StructType = StructType(Array(StructField("city_count",MapType(StringType,LongType)),StructField("total_count",LongType) ))

    //定义 输出类型  String
    override def dataType: DataType = StringType

    //验证 是否相同的输入有相同的输出 如果一样就返回true
    override def deterministic: Boolean = true

    // 存储的初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0)=new HashMap[String,Long]
        buffer(1)=0L
    }

    // 更新 每到一条数据做一次更新  输入 加入存储
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        val cityCountMap: Map[String, Long] = buffer.getAs[Map[String,Long]](0)
        val totalCount: Long = buffer.getLong(1)
        val cityName: String = input.getString(0)

        buffer(0)=cityCountMap+(cityName->(cityCountMap.getOrElse(cityName,0L)+1L))
        buffer(1)=totalCount+1L

    }

    // 合并 每个分区处理完成 汇总到driver时进行合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        val cityCountMap1: Map[String, Long] = buffer1.getAs[Map[String,Long]](0)
        val totalCount1: Long = buffer1.getLong(1)
        val cityCountMap2: Map[String, Long] = buffer2.getAs[Map[String,Long]](0)
        val totalCount2: Long = buffer2.getLong(1)

        buffer1(0) = cityCountMap1.foldLeft(cityCountMap2) { case (cityCountMap2, (cityName1, count1)) =>
            cityCountMap2 + (cityName1 -> (cityCountMap2.getOrElse(cityName1, 0L) + count1))
        }

        buffer1(1)=totalCount1+totalCount2

    }

    // 把存储中的数据 展示出来
    override def evaluate(buffer: Row): Any = {
        val cityCountMap: Map[String, Long] = buffer.getAs[Map[String,Long]](0)
        val totalCount: Long = buffer.getLong(1)

        //1 计算百分比
        val cityRatioInfoList: List[CityRatioInfo] = cityCountMap.map { case (cityName, count) =>
            val cityRatio = math.round(count.toDouble / totalCount * 1000) / 10D
            CityRatioInfo(cityName, cityRatio)
        }.toList


        //2 排序截取  前二

        var cityRatioInfoTop2List: List[CityRatioInfo] = cityRatioInfoList.sortBy(_.cityRatio)(Ordering.Double.reverse).take(2)


        // 3  把其他计算出来
        if(cityRatioInfoList.size>2){
            var otherRatio=100D
            cityRatioInfoTop2List.foreach(cityRatioInfo=>otherRatio-=cityRatioInfo.cityRatio )
            otherRatio=math.round(otherRatio* 10) / 10D
            cityRatioInfoTop2List=  cityRatioInfoTop2List :+ CityRatioInfo("其他", otherRatio)
        }


        // 4 拼接成字符串
        cityRatioInfoTop2List.mkString(",")

    }


    case class CityRatioInfo(cityName:String,cityRatio:Double){
        override def toString: String ={
            cityName+":"+cityRatio+"%"
        }
    }
}

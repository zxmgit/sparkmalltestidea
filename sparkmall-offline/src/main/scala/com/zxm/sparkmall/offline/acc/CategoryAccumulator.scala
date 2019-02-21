package com.zxm.sparkmall.offline.acc

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * @author zxm
  * @create 2019/2/19 - 8:33
  */
class CategoryAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Long]]{

    var acc = new mutable.HashMap[String, Long]()

    override def isZero: Boolean = acc.isEmpty

    override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
        new CategoryAccumulator
    }

    override def reset(): Unit = {
        acc.clear()
    }

    override def add(key: String): Unit = {
        acc(key) = acc.getOrElse(key, 0L) + 1L
    }

    override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
        val otherMap = other.value
        acc = acc.foldLeft(otherMap){case (otherMap, (key, count))=>{
            otherMap(key) = otherMap.getOrElse(key, 0L) + count
            otherMap
        }}
    }

    override def value: mutable.HashMap[String, Long] = acc
}

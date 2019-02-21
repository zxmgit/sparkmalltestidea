package com.zxm.sparkmall.offline.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap

/**
  * @author zxm
  * @create 2019/2/21 - 12:25
  */
class CityRatio extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = StructType(Array(StructField("city_name", StringType)))

    override def bufferSchema: StructType = StructType(Array(StructField("city_count", MapType(StringType, LongType)), StructField("total_count", MapType(StringType, LongType))))

    override def dataType: DataType = StringType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = new HashMap[String, Long]
        buffer(1) = 1
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        val cityMap = buffer.getAs[Map[String, Long]](0)
        val totalCount = buffer.getString(1)
        val cityName = input.getString(0)
        buffer(0) = cityMap + (cityName -> (cityMap.getOrElse(cityName, 0L) + 1L))
        buffer(1) = totalCount + 1L
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = ???

    override def evaluate(buffer: Row): Any = ???
}

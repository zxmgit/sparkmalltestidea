package com.zxm.sparkmall.offline.app

import java.util.{Properties, UUID}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zxm.sparkmall.common.bean.UserVisitAction
import com.zxm.sparkmall.common.util.PropertiesUtil
import com.zxm.sparkmall.offline.bean.CategoryCount
import com.zxm.sparkmall.offline.handler.{CategoryCountHandler, CategoryTopSessionHandler}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @author zxm
  * @create 2019/2/19 - 10:10
  */


//    def main(args: Array[String]): Unit = {
//        JSON.parse("{\"ABC\":123}")
//    }

    object OfflineApp {


        def main(args: Array[String]): Unit = {
            val taskId: String = UUID.randomUUID().toString

            val sparkConf: SparkConf = new SparkConf().setAppName("sparkmall-offline").setMaster("local[*]")
            val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
            //    1\ 根据条件把hive中数据查询出来
            //    得 RDD[UserVisitAction]
            val userVisitActionRDD: RDD[UserVisitAction] = readUserVisitActionToRDD(sparkSession)

            //需求一
            val categoryCountList: List[CategoryCount] = CategoryCountHandler.handle(sparkSession,userVisitActionRDD,taskId)
            println("需求一完成！！")

            //需求二
            CategoryTopSessionHandler.handle(sparkSession,userVisitActionRDD,taskId,categoryCountList)
            println("需求二完成！！")
        }


        def readUserVisitActionToRDD(sparkSession: SparkSession): RDD[UserVisitAction] = {
            // json工具  fastjson  gson  jackson
            val properties: Properties = PropertiesUtil.load("conditions.properties")
            val conditionsJson: String = properties.getProperty("condition.params.json")
            val conditionJsonObj: JSONObject = JSON.parseObject(conditionsJson)
            val startDate: String = conditionJsonObj.getString("startDate")
            val endDate: String = conditionJsonObj.getString("endDate")
            val startAge: String = conditionJsonObj.getString("startAge")
            val endAge: String = conditionJsonObj.getString("endAge")

            var sql = new StringBuilder("select v.*  from user_visit_action v,user_info u  where  v.user_id=u.user_id")
            if (startDate.nonEmpty) {
                sql.append(" and date>='" + startDate + "'")
            }
            if (endDate.nonEmpty) {
                sql.append(" and date<='" + endDate  + "'")
            }
            if (startAge.nonEmpty) {
                sql.append(" and age>=" + startAge  )
            }
            if (endAge.nonEmpty) {
                sql.append(" and age<=" + endAge )
            }
            println(sql)

            sparkSession.sql("use sparkmall")
            import sparkSession.implicits._
            val rdd: RDD[UserVisitAction] = sparkSession.sql(sql.toString()).as[UserVisitAction].rdd

            rdd
        }

    }

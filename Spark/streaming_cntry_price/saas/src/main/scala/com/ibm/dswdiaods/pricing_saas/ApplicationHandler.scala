/*
Name: ApplicationHandler.scala
Description: provides 2 ways to run you application, local and cluster
Created by: Ricardo Maravilla <ricardo.maravilla@ibm.com>
Created Date: 2022/11/14
Modification:
    date        owner       description
    20230124    Ricardo     Added the multi topic part
*/
package com.ibm.dswdiaods.pricing_saas

import com.ibm.dswdiaods.pricing_saas.StreamHandlerSaaS.get_streaming_query

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import com.ibm.dswdia.core.Properties._
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.logging.log4j.{LogManager,Logger}

/**
 * Use this to test the app locally
 *
 * local[int with the number of exec]
 **/
object StreamingCntryPriceSaaSLocal extends App{
    val table_name = args(0)
    logger.info("Running on Local mode")
    val spark: SparkSession = SparkSession
            .builder
            .master("local[4]")
            .getOrCreate()
    Run.run(spark, table_name)
}


/**
  * Use this when submitting the app to a cluster with spark-submit
  **/
object StreamingCntryPriceSaaS extends App{
    val table_name = args(0)
    logger.info("Running on clustermode mode")
    val spark: SparkSession = SparkSession
            .builder
            .getOrCreate()
    Run.run(spark, table_name)
}

/**
  * Run object, here you specified how the app will start
  **/
object Run {
    val logger = LogManager.getLogger(this.getClass())

    def run(spark: SparkSession, args: String): Unit = {
        logger.info("######################## Streaming Cntry Price SaaS #########################")

        val streaming_query: List[DataStreamWriter[Row]] = get_streaming_query(spark, args)

        // Val with the BU execution
        val bu = streaming_query.apply(0).start
        // Val with the GPH execution
		val gph = streaming_query.apply(1).start

        // Wait until the BU finish
        bu.awaitTermination()

        sys.exit(0)
    }
}

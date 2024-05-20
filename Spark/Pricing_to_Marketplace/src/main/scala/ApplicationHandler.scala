/*
Name: ApplicationHandler.scala
Description: provides 2 ways to run you application, local and cluster
Created by: Ricardo Maravilla <ricardo.maravilla@ibm.com>
Created Date: 2023/01/22
Modification:
    date        owner       description
*/
//package com.ibm.dswdiaods.pricing_marketplace

import com.ibm.dswdia.core.Properties._
import MP.refeed_process

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.logging.log4j.{LogManager,Logger}

/**
 * Use this to test the app locally
 *
 **/
object StreamingPricingMarketplaceLocal extends App{
    logger.info("Running on Local mode")
    val spark: SparkSession = SparkSession
            .builder
            .master("local[1]")
            .appName("Marketplace")
            .getOrCreate()

    Run.run(spark, "")
}


/**
  * Use this when submitting the app to a cluster with spark-submit
  **/
object StreamingPricingMarketplace extends App{
    logger.info("Running on clustermode mode")
    val spark: SparkSession = SparkSession
            .builder
            .appName("Marketplace")
            .getOrCreate()

    Run.run(spark, args(0))
}

object Run {
    val logger = LogManager.getLogger(this.getClass())

    def run(spark: SparkSession, args: String): Unit = {

        if(!args.isEmpty()){
            logger.info("######################## Refeed Pricing to Marketplace #########################")
            val result = refeed_process(args.split(","))

            sys.exit(0)
        }else{
            import StreamHandler.get_streaming_query
            logger.info("######################## Streaming Pricing to Marketplace #########################")
            val streaming_query  = get_streaming_query(spark)

            val mp = streaming_query.start

            spark.streams.awaitAnyTermination()

		    sys.exit(0)
        }
    }
}


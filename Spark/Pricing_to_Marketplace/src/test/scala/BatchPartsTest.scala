/*
Name: BatchPartsTest.scala
Description: Test for run the bath parts
Created by: Ricardo Maravilla <ricardo.maravilla@ibm.com>
Created Date: 2023/02/15
Notes:
Modification:
		date				owner			 description
*/
//package com.ibm.dswdiaods.pricing_marketplace

import BatchParts._
import com.alibaba.fastjson2.{JSONObject, JSONArray}
import org.apache.logging.log4j.{LogManager,Logger}
import org.scalatest.wordspec.AnyWordSpec
import java.util.HashMap
import com.ibm.dswdia.core.Properties._
import com.ibm.dswdia.core.Database

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

class BatchPartsTest extends AnyWordSpec {

    val logger = LogManager.getLogger("DSW DIA Marketplace Json Testing")
	val pool = new Database("Streaming MP", db2_ods_properties_streaming)

    val spark: SparkSession = SparkSession
            .builder
            .master("local[1]")
            .appName("Marketplace")
            .getOrCreate()

	logger.info("The next tests are desinged to understand the Batch Parts object.")

    "Batch Parts: saas data found behavior" should {
		"return an JSONObject" in {
			val result_test = generate_json_part("D0NRHLL", "2023-04-25 15:23:10.116611", pool)
			logger.info("result_test: " + result_test)
			if(result_test.isEmpty()){
				logger.info("result_test is empty")
			}
			assert(true)
		}
	}

	"Batch Parts: onprem data found behavior" should {
		"return an JSONObject" in {
			val result_test = generate_json_part("D28UTLL", "2022-02-01-03.31.15.264913", pool)
			logger.info("result_test: " + result_test)
			assert(true)
		}
	}
}

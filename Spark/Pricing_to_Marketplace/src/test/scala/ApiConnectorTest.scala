/*
Name: ApiConnectorTest.scala
Description: Test for run queries on DB2
Created by: Ricardo Maravilla <ricardo.maravilla@ibm.com>
Created Date: 2023/02/15
Notes:
Modification:
		date				owner			 description
*/
//package com.ibm.dswdiaods.pricing_marketplace


import QueryData._
import JsonMaker._
import ApiConnector._
import BatchParts._

import com.alibaba.fastjson2.{JSONObject, JSONArray}
import org.apache.logging.log4j.{LogManager,Logger}
import org.scalatest.wordspec.AnyWordSpec
import java.util.HashMap
import com.ibm.dswdia.core.Properties._
import com.ibm.dswdia.core.Database
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

class ApiConnectorTest extends AnyWordSpec {

    val logger = LogManager.getLogger("DSW DIA Marketplace ApiConnector Testing")
	val pool = new Database("Streaming MP", db2_ods_properties_streaming)

	val spark: SparkSession = SparkSession
            .builder
            .master("local[1]")
            .appName("Marketplace")
            .getOrCreate()

	logger.info("The next tests are desinged to understand the Json Maker object.")

    "API Conn: send_data_saas data found behavior" should {
		"return an Boolean" in {
			var result_test = true
			query_data_saas("D009GZX", pool) match {
				case Right(data) =>	val json_result = generate_json_saas(data, pool)
									json_result match {
										case Right(json_message) =>	val result = send_json_doc(json_message)
										case Left(bool) => result_test = false
									}
				case Left(bool) =>  result_test = false
			}

			assert(result_test)
		}
	}

	"API Conn: send_data_onprem data found behavior" should {
		"return an Boolean" in {
			var result_test = true
			query_data_onprem("44T9049", pool) match {
				case Right(data) =>	val json_result = generate_json_onprem(data, pool)
									json_result match {
										case Right(json_message) =>	val result = send_json_doc(json_message)
										case Left(bool) => result_test = false
									}
				case Left(bool) =>  result_test = false
			}
			assert(result_test)
		}
	}

	"API Conn: send_data_saas batch data found behavior" should {
		"return an Boolean" in {
			val payload = generate_json_part("D009GZX", "2023-04-20-23.52.51.713090", pool)
			val result = send_json_doc_batch(payload)
			var result_test = true
			if(result < 0)
				result_test = false
			assert(result_test)
		}
	}

	"API Conn: send_data_onprem batch data found behavior" should {
		"return an Boolean" in {
			val payload = generate_json_part("D28UTLL", "2022-02-01-03.31.15.264913", pool)
			val result = send_json_doc_batch(payload)
			var result_test = true
			if(result < 0)
				result_test = false
			assert(result_test)
		}
	}

}

/*
Name: QueryDataTest.scala
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
import com.alibaba.fastjson2.{JSONObject, JSONArray}
import org.apache.logging.log4j.{LogManager,Logger}
import org.scalatest.wordspec.AnyWordSpec
import java.util.HashMap
import com.ibm.dswdia.core.Properties._
import com.ibm.dswdia.core.Database

class JsonMakerTest extends AnyWordSpec {

    val logger = LogManager.getLogger("DSW DIA Marketplace Json Testing")
	val pool = new Database("Streaming MP", db2_ods_properties_streaming)

	logger.info("The next tests are desinged to understand the Json Maker object.")

    "Json Maker: generate_json_saas data found behavior" should {
		"return an JSONObject" in {
			var result_test = true
			query_data_saas("D009GZX", pool) match {
				case Right(data) =>	val json_result = generate_json_saas(data, pool)
									json_result match {
										case Right(json_message) =>	logger.info("JSON message SaaS: " + json_message.toString())
										case Left(bool) => result_test = false
									}
				case Left(bool) =>  result_test = false
			}

			assert(result_test)
		}
	}

	"Json Maker: generate_json_onprem data found behavior" should {
		"return an JSONObject" in {
			var result_test = true
			query_data_onprem("44T9049", pool) match {
				case Right(data) =>	val json_result = generate_json_onprem(data, pool)
									json_result match {
										case Right(json_message) =>	logger.info("JSON message: " + json_message)
										case Left(bool) => result_test = false
									}
				case Left(bool) =>  result_test = false
			}
			assert(result_test)
		}
	}


}

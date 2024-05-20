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

import org.apache.logging.log4j.{LogManager,Logger}
import org.scalatest.wordspec.AnyWordSpec
import java.util.HashMap
import com.ibm.dswdia.core.Properties._
import com.ibm.dswdia.core.Database

class QueryDataTest extends AnyWordSpec {

    val logger = LogManager.getLogger("DSW DIA Marketpalce Query Testing")
	val pool = new Database("Streaming MP", db2_ods_properties_streaming)

	logger.info("The next tests are desinged to understand the Query Data object.")

	"Query Data: query_data_saas data found behavior" should {
		"return an Array[java.util.HashMap[String,String]]" in {
			var result_test = false
			query_data_saas("D009GZX", pool) match {
				case Right(data) =>	data.foreach(println)
									result_test = true
				case Left(bool) =>  result_test = false
			}
			assert(result_test)
		}
	}

	"Query Data: query_data_saas data not found behavior" should {
		"return an Boolean" in {
			var result_test = false
			query_data_saas("D009GZX1", pool) match {
				case Right(data) =>	data.foreach(println)
									result_test = true
				case Left(bool) =>  result_test = false
			}
			assert(result_test)
		}
	}

	"Query Data: query_data_onprem data found behavior" should {
		"return an Array[java.util.HashMap[String,String]]" in {
			var result_test = false
			query_data_onprem("44T9049", pool) match {
				case Right(data) =>	for (i <- 0 until data.length){
									logger.info("Query Data: " + data.apply(i))
								}
									result_test = true
				case Left(bool) =>  result_test = false
			}
			assert(result_test)
		}
	}

	"Query Data: query_data_onprem data not found behavior" should {
		"return an Boolean" in {
			var result_test = false
			query_data_onprem("44T9049Z", pool) match {
				case Right(data) =>	data.foreach(println)
									result_test = true
				case Left(bool) =>  result_test = false
			}
			assert(result_test)
		}
	}

}

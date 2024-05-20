/*
Name: MP.scala
Description: Methods to generate the JSON
Input:
Output: 
Created by: Ricardo Maravilla <ricardo.maravilla@ibm.com>
Created Date: 2023/06/08
Notes:
Modification:
    date        owner       description
*/

//package com.ibm.dswdiaods.pricing_marketplace

import QueryData._
import JsonMaker._
import ApiConnector._
import BatchParts._
import com.ibm.dswdia.core.Database
import com.ibm.dswdia.core.Properties.{dswdia_conf,db2_ods_properties_streaming}

import org.apache.logging.log4j.{LogManager,Logger}
import com.alibaba.fastjson2.{JSONObject, JSONArray}
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import java.util.TimeZone
import java.text.DecimalFormat
import org.apache.arrow.vector.util.JsonStringArrayList
import com.bettercloud.vault.json.JsonObject
import com.ibm.db2.cmx.runtime.Data
import org.apache.spark.sql.SparkSession

object MP {

    val logger: Logger = LogManager.getLogger(this.getClass())
    val topics_pricing = List("wwide_fnshd_good_sap","prod_publshg","cntry_price_drct","cntry_price_tierd_drct")
    val topics_batch = List("sw_prod_scw","pricing_tierd_ecom_scw")

    /**
	  *
	  * This method executes the steps in the main streaming process
      *
	  * @param part_num string with the part number of the record
      * @param topic the topic of the record
	  * @param pool database core pool
      * @param date the date of the record
	  *
	  * @return boolean
	*/
    def main_process(part_num: String, topic: String, pool: Database, date: String): Boolean = {
        var result = true
        var topic_process = topic.split('.')(2).toString()

				if (query_data_exists_saas(part_num, pool)){
					topic_process = "cntry_price_tierd_drct"
				}else if (query_data_exists_onprem(part_num, pool)){
					topic_process = "cntry_price_drct"
				}else {
				    logger.warn("Could not find part: " + part_num + " in any table, sending delete doc")
                    result = send_empty_process(part_num)
				}

			if(topic_process == "cntry_price_tierd_drct"){
				result = send_mp_process("saas", part_num, "STRM", pool)
			}else if(topic_process == "cntry_price_drct"){
				result = send_mp_process("onprem", part_num, "STRM", pool)
			}

        if (!result) {
            throw new Exception("Error connecting to Database or API, please check both and do the refeed in the refeed airflow app.")
            return false
        }

        result = send_batch_process(part_num, date, "STRM", pool)

        if (!result) {
            throw new Exception("Error connecting to Database or API, please check both and do the refeed in the refeed airflow app.")
            return false
        }

        return true
    }

    /**
	  *
	  * This method executes the steps in the refeed process
      *
	  * @param part_num an array string of the part number
	  *
	  * @return boolean
	*/
    def refeed_process(part_nums: Array[String]): Boolean = {
        var pool: Database = new Database("Marketplace_Refeed" ,db2_ods_properties_streaming)
        var result_parts = true
        var result_batch = true

        for(part_num <- part_nums) {
            logger.info("part_num: " + part_num)
            if (query_data_exists_saas(part_num, pool)){
				result_parts = send_mp_process("saas", part_num, "RFEED", pool)
			}else if (query_data_exists_onprem(part_num, pool)){
                result_parts = send_mp_process("onprem", part_num, "RFED", pool)
            }else {
				logger.error("Could not find part: " + part_num + " in any table, sending delete doc")
                result_parts = send_empty_process(part_num)
			}
            if (!result_parts) {
                throw new Exception("Error connecting to Database or API, please check both and do the refeed in the refeed airflow app.")
                return false
            }

            query_data_batch_date(part_num, pool) match {
                case Right(data) => result_batch = send_batch_process(part_num, data.apply(0).get("1"), "RFEED", pool)
                case Left(bool) =>  if(bool) logger.warn("Empty doc, process will continue")
                                    result_batch = bool
            }
            if (!result_batch) {
                throw new Exception("Error connecting to Database or API, please check both and do the refeed in the refeed airflow app.")
                return false
            }
        }

        return true
    }

    /**
	  *
	  * This method executes the steps to send the parts json
      *
      * @param data_type string saas or onprem
	  * @param part_num string with the part number of the record
      * @param type_data string with the kind of data
	  * @param pool database core pool
	  *
	  * @return boolean
	*/
    def send_mp_process(data_type: String, part_num: String, type_data: String, pool: Database): Boolean = {
        var result = true
        if(data_type == "saas"){
            query_data_saas(part_num, pool) match {
					case Right(data) =>	val json_result = generate_json_saas(data, pool)
										json_result match {
											case Right(json_message) =>	val result_doc = send_json_doc_load_tries(json_message, "Info")
                                                                        if(result_doc == -1) result = false
											case Left(bool) => result = false
										}
					case Left(bool) =>  result = send_empty_process(part_num)
                    }
        }else{
            query_data_onprem(part_num, pool) match {
					case Right(data) =>	val json_result = generate_json_onprem(data, pool)
										json_result match {
											case Right(json_message) =>	val result_doc = send_json_doc_load_tries(json_message, "Info")
                                                                        if(result_doc == -1) result = false
											case Left(bool) => result =  false
										}
					case Left(bool) =>  result = send_empty_process(part_num)
                    }
        }

        if(!result) return false

        var recon = query_data_recon(part_num, "INFO", type_data, pool)
        return true
    }

    /**
	  *
	  * This method executes the steps to send the prices json
      *
	  * @param part_num string with the part number of the record
      * @param date string with the date of the record
      * @param type_data string with the kind of data
	  * @param pool database core pool
	  *
	  * @return boolean
	*/
    def send_batch_process(part_num: String, date: String, type_data: String, pool: Database): Boolean = {

        val doc = generate_json_part(part_num, date, pool)

        if(doc.isEmpty()){
            logger.warn("Empty doc, process will continue")
            return true
        }

        val result = send_json_doc_load_tries(doc , "Parts")

        if (result == -1) {
            logger.error("Error sending empty process to the API")
            return false
        }

        var recon = query_data_recon(part_num, "PARTS", type_data, pool)
        return true
    }

    /**
	  *
	  * This method executes the steps to send the delete part json
      *
	  * @param part_num string with the part number of the record
	  *
	  * @return boolean
	*/
    def send_empty_process(part_num: String): Boolean = {
        var doc = generete_json_empty(part_num)
        val result = send_json_doc_load_tries(doc, "Info")

        if(result == -1){
            logger.error("Error sending empty process to the API")
            return false
        }

        return true
    }


}

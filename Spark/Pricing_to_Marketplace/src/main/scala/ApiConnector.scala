/*
Name: ApiConnector.scala
Description: Methods to connect and send data to the api
Input:
Output: Json DOC
Created by: Ricardo Maravilla <ricardo.maravilla@ibm.com>
Created Date: 2023/04/10
Notes:
Modification:
    date        owner       description
*/

//package com.ibm.dswdiaods.pricing_marketplace

import org.apache.logging.log4j.{LogManager,Logger}
import com.alibaba.fastjson2.{JSONObject, JSONArray}
import requests.{TimeoutException,RequestFailedException,InvalidCertException,UnknownHostException}
import scala.util.Properties.envOrElse


object ApiConnector {

    val logger: Logger = LogManager.getLogger(this.getClass())

    val parts_url = envOrElse("PARTS_API_URL","").toString().replace("\n", "")
    val parts_token = envOrElse("PARTS_API_TOKEN","").toString().replace("\n", "")

    val batch_url = envOrElse("BATCH_API_URL","").toString().replace("\n", "")
    val batch_token = envOrElse("BATCH_API_TOKEN","").toString().replace("\n", "")

    /**
	  *
	  * This method sends the doc json to the api.
	  *
	  * @param doc JSON object containing the data
	  *
	  * @return integer if 200 to 206 is ok, other value is error
	*/
    def send_json_doc(doc: JSONObject): Integer = {
        try{
            val doc_final = new JSONObject()
            doc_final.put("SCW_ODS_PRICE_LIST",new JSONArray(doc))
            val resp = requests.post(
                parts_url,
                data=(doc_final.toString()),
                headers =Map("Authorization" -> s"Bearer ${parts_token}", "Content-Type" -> "application/json","Accept" -> "*/*")
            )

            val response_status_code = resp.statusCode

            if (Array(200, 204).contains(response_status_code)){
                logger.info(s"The request payload was loaded properly, status_code: $response_status_code")
                return response_status_code
            }
            else {
                logger.error(s"There is an error in request method, data was not loaded, status code: $response_status_code")    
                return -1
            }
        }
        catch{
            case e: Exception => logger.error(s"There is an error in request method, data was not loaded: $e")
            return -1

        }
    }

    /**
	  *
	  * This method sends the doc json to the api batch.
	  *
	  * @param doc JSON object containing the data
	  *
	  * @return integer if 200 to 206 is ok, other value is error
	*/
    def send_json_doc_batch(doc: JSONObject): Integer = {
        try{
            val resp = requests.post(
                batch_url,
                data=(doc.toString()),
                headers =Map("Authorization" -> s"Bearer ${batch_token}", "Content-Type" -> "application/json","Accept" -> "*/*")
            )

            val response_status_code = resp.statusCode

            if (Array(200, 204).contains(response_status_code)){
                logger.info(s"The request payload was loaded properly, status_code: $response_status_code")
                return response_status_code
            }
            else {
                logger.error(s"There is an error in request method, data was not loaded, status code: $response_status_code")    
                return -1
            }
        }
        catch{
            case e: Exception => logger.error(s"There is an error in request method, data was not loaded: $e")
            return -1

        }
    }

    /**
    * Retries a failed part treetimes wating 3 seconds between retries
    *
    * @param doc
    * @param doc_type
    * @return numeric response
    */
    def send_json_doc_load_tries (doc: JSONObject, doc_type: String): Integer= {
        var retry_it = 0
        var resp = -1
        while (retry_it <= 3 && resp <= 0) { 
        retry_it+=1
        if(doc_type == "Info"){
            resp = send_json_doc(doc)
        }else{
            resp = send_json_doc_batch(doc)
        }
        if (retry_it > 1 && resp <= 0){
            logger.warn(s"It was not possible load batch retry number: ${retry_it-1}")
            Thread.sleep(3000)  //wait 3 seconds
        }
        }
        return resp
    }

}

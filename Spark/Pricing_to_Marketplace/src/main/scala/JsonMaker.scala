/*
Name: JsonMaker.scala
Description: Methods to generate the JSON
Input:
Output: JSON value
Created by: Ricardo Maravilla <ricardo.maravilla@ibm.com>
Created Date: 2023/02/01
Notes:
Modification:
    date        owner       description
*/

//package com.ibm.dswdiaods.pricing_marketplace

import Redis._
import QueryData._
import com.ibm.dswdia.core.Database

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
object JsonMaker {
	/**
	  *
	  * This method creates an json object with the cntry_code and the iso_currency_code group
	  * in one part_num for SaaS. The data came from the DB with an select query only one part_num.
	  *
	  * @param data Array[java.util.HashMap[String,String]] with all the data for part_num
	  * @param start_date String with the start date of the newest part_num
	  *
	  * @return left boolean with false, right json object with the info
	*/
	val logger: Logger = LogManager.getLogger(this.getClass())

    def generate_json_saas(data: Array[java.util.HashMap[String,String]], pool: Database): Either[Boolean,JSONObject] = {

		// var init
		var doc = new JSONObject()
		val part_num = data.apply(0).get("PART_NUM")
		var pricng_tier_mdl = ""
		var publshd_price_durtn_code = ""

		var filter = true
		val date = timezone_est()

		// Filter to check if the part exists in the prod_publshg table
		if(!filter && !(query_prod_publish(part_num, pool))){
			logger.error("For SaaS, part_num " + part_num + " is not found in dimnsn index")
			Left (false)
		}else {
			query_data_wwide(part_num, pool) match {
								// Get the pricng_tier_mdl and the publshd_price from the database
								case Right(data) =>	pricng_tier_mdl = data.apply(0).get("PRICNG_TIER_MDL")
													publshd_price_durtn_code = data.apply(0).get("PUBLSHD_PRICE_DURTN_CODE")
								case Left(bool) =>  logger.error("Error getting data from wwide.")
													return Left(false)
			}

			doc.put("SCW_ODS_PART_NUMBER", part_num)
			doc.put("SCW_ODS_PRICING_MODEL", pricng_tier_mdl)
			doc.put("SCW_ODS_PRICING_INDICATOR", publshd_price_durtn_code)
			doc.put("SCW_ODS_MARK_4_DELETE", 0)
			doc.put("ODS_EXTRACT_DATE", date)
			doc.put("PRICE_START_DATE", data.apply(0).get("PRICE_START_DATE"))


			// Start all the objects and the first codes
			var priceList = new JSONArray()
			var df1 = new DecimalFormat("#.000")
			var df2 = new DecimalFormat("#.0000")
			var last_cntry_code = data.apply(0).get("CNTRY_CODE")
			var last_iso_currncy_code = data.apply(0).get("ISO_CURRNCY_CODE")
			var partPriceList = new JSONArray()
			var scaleQtyPrice = new JSONArray()
			var json = new JSONObject()
			var price = new JSONObject()
			var partPrice = new JSONObject()

			// Main for for all the recods per part_num
			for(i <- 0 until data.length){
				if(data.apply(i).get("ISO_CURRNCY_CODE") != last_iso_currncy_code || data.apply(i).get("CNTRY_CODE") != last_cntry_code){
					partPrice.put("CURRENCY", last_iso_currncy_code)
					partPrice.put("SCALE_QTY_PRICE", scaleQtyPrice)
					partPriceList.add(partPrice)
					partPrice = new JSONObject()
					scaleQtyPrice = new JSONArray()
				}
				if(data.apply(i).get("CNTRY_CODE") != last_cntry_code){
					price.put("COUNTRY_CODE", last_cntry_code)
					price.put("PART_PRICE", partPriceList)
					priceList.add(price)
					partPriceList = new JSONArray()
					price = new JSONObject()
				}

				// This block needs to run every iteration
				json.put("QTY", df1.format(data.apply(i).get("TIERD_SCALE_QTY").toFloat))
				json.put("PRICE", df2.format(data.apply(i).get("SVP_LEVEL_B").toFloat))
				scaleQtyPrice.add(json)
				json = new JSONObject()

				last_cntry_code = data.apply(i).get("CNTRY_CODE")
				last_iso_currncy_code = data.apply(i).get("ISO_CURRNCY_CODE")

			}

			// This block needs to run to add the last record of the data
			partPrice.put("CURRENCY", last_iso_currncy_code)
			partPrice.put("SCALE_QTY_PRICE", scaleQtyPrice)
			partPriceList.add(partPrice)
			price.put("COUNTRY_CODE", last_cntry_code)
			price.put("PART_PRICE", partPriceList)
			priceList.add(price)

			doc.put("PRICE_LIST", priceList)
			Right (doc)
		}
    }

	/**
	  *
	  * This method creates an json object with the cntry_code and the iso_currency_code group
	  * in one part_num for Onprem. The data came from the DB with an select query only one part_num.
	  *
	  * @param data Array[java.util.HashMap[String,String]] with all the data for part_num
	  * @param start_date String with the start date of the newest part_num
	  *
	  * @return left boolean with false, right json object with the info
	*/
    def generate_json_onprem(data: Array[java.util.HashMap[String,String]], pool: Database): Either[Boolean,JSONObject] = {

		// var init
		var doc = new JSONObject()
		val part_num = data.apply(0).get("PART_NUM")

		var filter = true
		val date = timezone_est()

		// Filter to check if the part exists in the prod_publshg table
		if(!filter && !(query_prod_publish(part_num, pool))){
			logger.error("For SaaS, part_num " + part_num + " is not found in dimnsn index")
			Left (false)
		}else {
			doc.put("SCW_ODS_PART_NUMBER", part_num)
			doc.put("SCW_ODS_PRICING_MODEL", "")
			doc.put("SCW_ODS_PRICING_INDICATOR", "")
			doc.put("SCW_ODS_MARK_4_DELETE", 0)
			doc.put("ODS_EXTRACT_DATE", date)
			doc.put("PRICE_START_DATE", data.apply(0).get("PRICE_START_DATE"))

			// Start all the objects and the first codes
			var priceList = new JSONArray()
			var df2 = new DecimalFormat("#.0000")
			var last_cntry_code = data.apply(0).get("CNTRY_CODE")
			var last_iso_currncy_code = data.apply(0).get("ISO_CURRNCY_CODE")
			var partPriceList = new JSONArray()
			var scaleQtyPrice = new JSONArray()
			var json = new JSONObject()
			var price = new JSONObject()
			var partPrice = new JSONObject()

			// Main for for all the recods per part_num
			for(i <- 0 until data.length){
				if(data.apply(i).get("ISO_CURRNCY_CODE") != last_iso_currncy_code || data.apply(i).get("CNTRY_CODE") != last_cntry_code){
					partPrice.put("CURRENCY", last_iso_currncy_code)
					partPrice.put("SCALE_QTY_PRICE", scaleQtyPrice)
					partPriceList.add(partPrice)
					partPrice = new JSONObject()
					scaleQtyPrice = new JSONArray()
				}
				if(data.apply(i).get("CNTRY_CODE") != last_cntry_code){
					price.put("COUNTRY_CODE", last_cntry_code)
					price.put("PART_PRICE", partPriceList)
					priceList.add(price)
					partPriceList = new JSONArray()
					price = new JSONObject()
				}

				// This block needs to run every iteration
				json.put("QTY",("1.000"))
				json.put("PRICE", df2.format(data.apply(i).get("SVP_LEVEL_B").toFloat))
				scaleQtyPrice.add(json)
				json = new JSONObject()

				last_cntry_code = data.apply(i).get("CNTRY_CODE")
				last_iso_currncy_code = data.apply(i).get("ISO_CURRNCY_CODE")

			}

			// This block needs to run to add the last record of the data
			partPrice.put("CURRENCY", last_iso_currncy_code)
			partPrice.put("SCALE_QTY_PRICE", scaleQtyPrice)
			partPriceList.add(partPrice)
			price.put("COUNTRY_CODE", last_cntry_code)
			price.put("PART_PRICE", partPriceList)
			priceList.add(price)

			doc.put("PRICE_LIST", priceList)
			Right (doc)
		}
	}

	/**
	  *
	  * This method generete the empty delete doc
	  *
	  * @return a json doc
	*/
	def generete_json_empty(part_num: String): JSONObject = {
		var empty_doc = new JSONObject()
        empty_doc.put("SCW_ODS_PART_NUMBER", part_num)
        empty_doc.put("SCW_ODS_MARK_4_DELETE", 1)
        return empty_doc
	}
	/**
	  *
	  * This method gives the current date with the format
	  * yyyy-MM-dd'T'HH: mm: ss.SSSSSS
	  *
	  * @return a str with the date
	*/
	def timezone_est(): String = {
		var calendar = Calendar.getInstance()
		calendar.setTime(new Date())
		var sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH: mm: ss.SSSSSS")

		//Here you say to java the initial timezone. This is the secret
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"))


		return sdf.format(calendar.getTime())
	}

}

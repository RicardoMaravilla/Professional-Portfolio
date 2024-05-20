/*
Name: BatchParts.scala
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
import com.ibm.dswdia.core.Properties._

import org.apache.logging.log4j.{LogManager,Logger}
import org.apache.spark.sql._
import com.alibaba.fastjson2.{JSONObject, JSONArray}
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import java.util.TimeZone
import java.text.DecimalFormat
import org.apache.arrow.vector.util.JsonStringArrayList
import com.bettercloud.vault.json.JsonObject
import com.ibm.db2.cmx.runtime.Data
import scala.io.Source


object BatchParts {

    val logger: Logger = LogManager.getLogger(this.getClass())

    /**
	* generetes the json doc using a query, only one part with the information is send
	*
	* @param part_num a string with the number of part
    * @param date a string with the last date
    * @param pool a database pool
    *
	* @return an JSON object with the information, empty if there is no information
	*/
    def generate_json_part(part_num: String, date: String, pool: Database): JSONObject = {

        var doc = new JSONObject()
        var ext_query = query_reader("marketplaceQuery.sql") //this query contains parts to load

        ext_query = ext_query
                    //.replace("AND PTES.ODS_MOD_DATE >= ?", "AND PTES.ODS_MOD_DATE >= " + "'" + date + "'" )
                    .replace("AND PTES.PART_NUM IN [?]", "AND PTES.PART_NUM IN " + "'" + part_num + "'" )

        val result = pool.run(ext_query)

        if(result.isEmpty) {
            logger.error("Couldn't find data for the part number " + part_num)
            return doc
        }

        doc.put("itemType",result.apply(0).get("itemType"))
        doc.put("chargeMetric",result.apply(0).get("chargeMetric"))
        doc.put("licenseTerm",result.apply(0).get("licenseTerm"))
        doc.put("saasRenwlMdlCodeDscr",result.apply(0).get("saasRenwlMdlCodeDscr"))
        doc.put("description",result.apply(0).get("description"))
        doc.put("pid",result.apply(0).get("pid"))
        doc.put("slaPart",result.apply(0).get("slaPart"))
        doc.put("priceStartDate",result.apply(0).get("priceStartDate"))
        doc.put("licenseType",result.apply(0).get("licenseType"))
        doc.put("chargeableComponent",result.apply(0).get("chargeableComponent"))
        doc.put("revenueStreamCodeDscr",result.apply(0).get("revenueStreamCodeDscr"))
        doc.put("sapMatlTypeCodeDscr",result.apply(0).get("sapMatlTypeCodeDscr"))
        doc.put("touName",result.apply(0).get("touName"))
        doc.put("sapMatlTypeCode",result.apply(0).get("sapMatlTypeCode"))
        doc.put("chargeableComponentDscr",result.apply(0).get("chargeableComponentDscr"))
        doc.put("swURL",result.apply(0).get("swURL"))
        doc.put("monthlyLicenseFlag",result.apply(0).get("monthlyLicenseFlag").toInt)
        doc.put("saasRenewal",result.apply(0).get("saasRenewal"))
        doc.put("dswPartType",result.apply(0).get("dswPartType"))
        doc.put("revenueStreamCode",result.apply(0).get("revenueStreamCode"))
        doc.put("partShortDscr",result.apply(0).get("partShortDscr"))
        doc.put("utl30Code",result.apply(0).get("utl30Code"))
        doc.put("audienceMask",result.apply(0).get("audienceMask").toInt)
        doc.put("touURL",result.apply(0).get("touURL"))
        doc.put("saasServiceProviderCode",result.apply(0).get("saasServiceProviderCode"))
        doc.put("name",result.apply(0).get("name"))
        doc.put("partNumber",result.apply(0).get("partNumber"))
        doc.put("serviceDescription",result.apply(0).get("serviceDescription"))
        doc.put("subscriptionId",result.apply(0).get("subscriptionId"))
        doc.put("licKeyFlag",result.apply(0).get("licKeyFlag").toInt)
        doc.put("status",result.apply(0).get("status"))

        return  doc
        }

    /**
	* Read text files from resources path.
	*
	* @param str_path
	* @return
	*/
    def query_reader(str_path: String): String = {

        val readmeText : Iterator[String] = Source.fromResource(str_path).getLines
        var query = ""
        for(line <- readmeText) {query = query + line + "\n"}
        query
    }

}

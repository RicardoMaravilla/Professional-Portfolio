/*
Name: QueryData.scala
Description: Methods to Query the Data from the database
Input:
Output: Data values
Created by: Ricardo Maravilla <ricardo.maravilla@ibm.com>
Created Date: 2023/02/01
Notes:
Modification:
    date        owner       description
*/

//package com.ibm.dswdiaods.pricing_marketplace

import org.apache.logging.log4j.{LogManager,Logger}
import com.ibm.dswdia.core.Database
import com.ibm.dswdia.core.Properties
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.text.SimpleDateFormat
import java.util.Calendar

object QueryData {

	val logger: Logger = LogManager.getLogger(this.getClass())

    val cal = Calendar.getInstance()
    val date =cal.get(Calendar.DATE)
    val Year =cal.get(Calendar.YEAR)

    /**
	  *
	  * This method executes a query to get information for the json of the table
	  * WWPP2.CNTRY_PRICE_TIERD_DRCT.
	  *
	  * @param part_num string with the part number of the record
	  * @param pool database core pool
	  *
	  * @return left boolean with false, right hashamp with the result
	*/
    def query_data_saas(part_num: String, pool: Database): Either[Boolean,Array[java.util.HashMap[String,String]]] = {

        val query = "SELECT PART_NUM, PRICE_START_DATE, CNTRY_CODE, ISO_CURRNCY_CODE, TIERD_SCALE_QTY, SVP_LEVEL_B " +
                    "FROM WWPP2.CNTRY_PRICE_TIERD_DRCT WHERE PART_NUM='" + part_num + "' AND PRICE_START_DATE <= '" +
                    Year +"-12-31' ORDER BY CNTRY_CODE"

        val result = pool.run(query)

        if(result.isEmpty){
            logger.error("Not data found for part number " + part_num)
            Left (false)
        }else
            Right (result)
    }

    /**
	  *
	  * This method executes a query to get information for the json of the table
	  * WWPP2.CNTRY_PRICE_DRCT.
	  *
	  * @param part_num string with the part number of the record
	  * @param pool database core pool
	  *
	  * @return left boolean with false, right hashamp with the result
	*/
    def query_data_onprem(part_num: String, pool: Database): Either[Boolean,Array[java.util.HashMap[String,String]]] = {

        val query = "SELECT PART_NUM, PRICE_START_DATE, CNTRY_CODE, ISO_CURRNCY_CODE, SVP_LEVEL_B " +
                    "FROM WWPP2.CNTRY_PRICE_DRCT WHERE PART_NUM='" + part_num + "' AND PRICE_START_DATE <= '" +
                    Year +"-12-31' ORDER BY CNTRY_CODE"

        val result = pool.run(query)

        if(result.isEmpty){
            logger.error("Not data found for part number " + part_num)
            Left (false)
        }else
            Right (result)
    }

    /**
	  *
	  * This method executes a query to get information that if the part_num
	  * exists on the table WWPP2.PROD_PUBLSHG.
	  *
	  * @param part_num string with the part number of the record
	  * @param pool database core pool
	  *
	  * @return boolean with the result
	*/
    def query_prod_publish(part_num: String, pool: Database): Boolean = {

        val query = "SELECT DISTINCT PART_NUM " +
                    "FROM WWPP2.PROD_PUBLSHG WHERE PART_NUM='" + part_num + "'"

        val result = pool.run(query)

        if(result.isEmpty){
            logger.error("Not PROD publish found for part_num " + part_num)
            return false
        }

        true
    }

    /**
	  *
	  * This method executes a query to get information for the json of the table
	  * WWPP2.WWIDE_FNSHD_GOOD_SAP.
	  *
	  * @param part_num string with the part number of the record
	  * @param pool database core pool
	  *
	  * @return left boolean with false, right hashamp with the result
	*/
    def query_data_wwide(part_num: String, pool: Database): Either[Boolean,Array[java.util.HashMap[String,String]]] = {

        val query = "SELECT PART_NUM, PRICNG_TIER_MDL, PUBLSHD_PRICE_DURTN_CODE " +
                    "FROM WWPP2.WWIDE_FNSHD_GOOD_SAP WHERE PART_NUM='" + part_num + "'"

        val result = pool.run(query)

        if(result.isEmpty){
            logger.error("Not WWIDE found for part_num " + part_num)
            Left (false)
        }else
            Right (result)
    }

    /**
	  *
	  * This method executes a query to get information for the json of the table
	  * FFXT1.PRICING_TIERD_ECOM_SCW
	  *
	  * @param part_num string with the part number of the record
	  * @param pool database core pool
	  *
	  * @return left boolean with false, right hashamp with the result
	*/
    def query_data_batch_saas(part_num: String, date:String, pool: Database): Either[Boolean,Array[java.util.HashMap[String,String]]] = {

        val query = "SELECT * " +
                    "FROM FFXT1.PRICING_TIERD_ECOM_SCW  WHERE PART_NUM='" + part_num + "' AND ODS_MOD_DATE >='" + date + "'"

        val result = pool.run(query)

        if(result.isEmpty){
            logger.error("Not Pricing Tierd Ecom for part_num " + part_num)
            Left (false)
        }else
            Right (result)
    }

    /**
	  *
	  * This method executes a query to get information for the json of the table
	  * FFXT1.SW_PROD_SCW
	  *
	  * @param part_num string with the part number of the record
	  * @param pool database core pool
	  *
	  * @return left boolean with false, right hashamp with the result
	*/
    def query_data_batch_onprem(part_num: String, date:String, pool: Database): Either[Boolean,Array[java.util.HashMap[String,String]]] = {

        val query = "SELECT * " +
                    "FROM FFXT1.SW_PROD_SCW  WHERE PART_NUM='" + part_num + "' AND ODS_MOD_DATE >='" + date + "'"

        val result = pool.run(query)

        if(result.isEmpty){
            logger.error("Not SW PROD SCW for part_num " + part_num)
            Left (false)
        }else
            Right (result)
    }


    /**
	  *
	  * This method executes a query to determine if the part is onprem
	  *
	  * @param part_num string with the part number of the record
	  * @param pool database core pool
	  *
	  * @return left boolean with false, right hashamp with the result
	*/
    def query_data_exists_onprem(part_num: String, pool: Database): Boolean = {

        val query = "SELECT 1" +
                    " FROM WWPP2.CNTRY_PRICE_DRCT WHERE PART_NUM='" + part_num + "'"

        val result = pool.run(query)

        if(result.isEmpty){
            logger.error("Not part onprem part_num " + part_num)
            return false
        }
            return true
    }

    /**
	  *
	  * This method executes a query to determine if the part is onprem
	  *
	  * @param part_num string with the part number of the record
	  * @param pool database core pool
	  *
	  * @return left boolean with false, right hashamp with the result
	*/
    def query_data_exists_saas(part_num: String, pool: Database): Boolean = {

        val query = "SELECT 1" +
                    " FROM WWPP2.CNTRY_PRICE_TIERD_DRCT WHERE PART_NUM='" + part_num + "'"

        val result = pool.run(query)

        if(result.isEmpty){
            logger.error("Not part saas part_num " + part_num)
            return false
        }

        return true
    }

    /**
	  *
	  * This method executes a query to get the ods_mod_date from the batch tables
      *
	  * @param part_num string with the part number of the record
	  * @param pool database core pool
	  *
	  * @return left boolean with false, right hashamp with the result
	*/
    def query_data_batch_date(part_num: String, pool: Database): Either[Boolean,Array[java.util.HashMap[String,String]]] = {

        var query = ""
        var flag = false

        if(query_data_exists_saas(part_num, pool)){
            query = "SELECT MAX(ODS_MOD_DATE) FROM FFXT1.PRICING_TIERD_ECOM_SCW WHERE PART_NUM='" + part_num + "' AND CNTRY_CODE ='USA'"
        }else if(query_data_exists_onprem(part_num, pool)){
            query = "SELECT MAX(ODS_MOD_DATE) FROM FFXT1.SW_PROD_SCW WHERE PART_NUM='" + part_num + "' AND CNTRY_CODE ='USA'"
        }else{
            logger.error("PART " + part_num + " doesnt exists in any table.")
            flag = true
        }

        if(flag){
            Left (true)
        }else{

            val result = pool.run(query)

            if(result.isEmpty){
                logger.error("Not batch found for part_num " + part_num)
                Left (false)
            }else
                Right (result)
        }
    }

    /**
	  *
	  * This method executes a query to insert/update recon table
      *
	  * @param part_num string with the part number of the record
      * @param process string with the process of the part
      * @param type_data string with the type of the part
	  * @param pool database core pool
	  *
	  * @return left boolean with false, right array with the result
	*/
    def query_data_recon(part_num: String, process: String, type_data: String, pool: Database): Either[Boolean,Array[java.util.HashMap[String,String]]] = {

        val current_timestamp = get_current_timestamp()
        val query_exists = "SELECT 1" +
                            " FROM FFXT1.MARKETPLACE_RECON WHERE PART_NUM='" + part_num + "'" +
                            " AND PROCESS='" + process + "'"

        var query = ""

        val result = pool.run(query_exists)

        if(result.isEmpty){
            query = "INSERT INTO FFXT1.MARKETPLACE_RECON (PART_NUM, PROCESS, TYPE_DATA, ODS_ADD_DATE, ODS_MOD_DATE) " +
                    "VALUES ('" + part_num + "', '" + process + "', '" + type_data + "', '" + current_timestamp + "', '" + current_timestamp + "')"
        }else{
            query = "UPDATE FFXT1.MARKETPLACE_RECON SET TYPE_DATA = '" + type_data + "', ODS_MOD_DATE = '" + current_timestamp + "'" +
                    " WHERE PART_NUM = '" + part_num + "' AND PROCESS = '" + process + "'"
        }

        val result_query = pool.run(query)
        if(result_query.isEmpty){
                Left(false)
        }

        Right (result)
    }

    /**
	  * Get current timestamp
	  *
	  * @return String
	  */
	def get_current_timestamp(): String = {
		
		val current_timestamp = DateTimeFormatter
						.ofPattern("YYYY-MM-dd-HH.mm.ss.SSSSSS")
						.format(LocalDateTime.now)
		return current_timestamp
	}
}

/*
Name: PartInfo.scala
Description: Logic to handle the pricing to marketplace feed
Input: Kafka CDC information
Output: JSON value
Created by: Ricardo Maravilla <ricardo.maravilla@ibm.com>
Created Date: 2023/01/27
Notes:
Modification:
    date        owner       description
*/
//package com.ibm.dswdiaods.pricing_marketplace

import com.ibm.dswdia.core.Database
import com.ibm.dswdia.core.Properties.{db2_ods_properties_streaming}

import java.sql.Types
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.text.SimpleDateFormat
import scala.util.{Failure, Success}
import org.apache.logging.log4j.{LogManager,Logger}
import org.apache.spark.sql.execution.columnar.INT
import com.ibm.db2.cmx.runtime.Data

object PartInfo {
	/**
	  *
	  * @param 
	  */

	val logger: Logger = LogManager.getLogger(this.getClass())

    def wwdie_fnshd_good_sap(raw_record: org.apache.spark.sql.Row): Boolean = {

        val primary_keys: Array[String] = Array(
			raw_record.getAs[String]("PART_NUM").replace(" ", "")
		)

        val record: Map[String, Any] = Map(
			"PART_NUM" 				                -> raw_record.getAs[String]("PART_NUM"),
			"GL_ACCT_ASSGNMT_GRP_CODE" 			    -> raw_record.getAs[String]("GL_ACCT_ASSGNMT_GRP_CODE"),
			"SAP_ITEM_CAT_GRP_CODE" 	            -> raw_record.getAs[String]("SAP_ITEM_CAT_GRP_CODE").replace(" ", ""),
			"SAP_MATL_GRP" 			                -> raw_record.getAs[String]("SAP_MATL_GRP"),
			"SAP_MATL_PRICING_GRP_1" 			    -> raw_record.getAs[String]("SAP_MATL_PRICING_GRP_1"),
			"SAP_MATL_PRICING_GRP_2" 				-> raw_record.getAs[Double]("SAP_MATL_PRICING_GRP_2"),
			"SAP_MATL_PRICING_GRP_3" 				-> raw_record.getAs[Double]("SAP_MATL_PRICING_GRP_3"),
			"SAP_MATL_PRICING_GRP_4" 				-> raw_record.getAs[Double]("SAP_MATL_PRICING_GRP_4"),
			"SAP_MATL_PRICING_GRP_5" 				-> raw_record.getAs[Double]("SAP_MATL_PRICING_GRP_5"),
			"SAP_MATL_TYPE_CODE" 				    -> raw_record.getAs[Double]("SAP_MATL_TYPE_CODE"),
			"SAP_SALES_FLAG" 				        -> raw_record.getAs[Double]("SAP_SALES_FLAG"),
			"SAP_SALES_STAT_CODE" 				    -> raw_record.getAs[Double]("SAP_SALES_STAT_CODE"),
			"PRICNG_TIER_QTY_MESUR" 				-> raw_record.getAs[Double]("PRICNG_TIER_QTY_MESUR"),
			"PRICNG_TIER_MDL" 				        -> raw_record.getAs[Double]("PRICNG_TIER_MDL"),
			"PROVISNG_HOLD_DTL" 				    -> raw_record.getAs[Double]("PROVISNG_HOLD_DTL"),
			"PUBLSHD_PRICE_DURTN_CODE" 				-> raw_record.getAs[Double]("PUBLSHD_PRICE_DURTN_CODE"),
			"SAAS_RENWL_MDL_CODE" 				    -> raw_record.getAs[Double]("SAAS_RENWL_MDL_CODE"),
			"BILLG_UPFRNT_FLAG" 				    -> raw_record.getAs[Double]("BILLG_UPFRNT_FLAG"),
			"BILLG_MTHLY_FLAG" 				        -> raw_record.getAs[Double]("BILLG_MTHLY_FLAG"),
            "BILLG_QTRLY_FLAG" 				        -> raw_record.getAs[Double]("BILLG_QTRLY_FLAG"),
            "BILLG_ANL_FLAG" 				        -> raw_record.getAs[Double]("BILLG_ANL_FLAG"),
            "SER_NUM_PRFL" 				            -> raw_record.getAs[Double]("SER_NUM_PRFL"),
            "BILLG_EVENT_FLAG" 				        -> raw_record.getAs[Double]("BILLG_EVENT_FLAG")
		)

        logger.info("RECORD: " + record)

        return true
    }

	def prod_publshg(raw_record: org.apache.spark.sql.Row): Boolean = {

        val primary_keys: Array[String] = Array(
			raw_record.getAs[String]("PART_NUM").replace(" ", ""),
			raw_record.getAs[String]("FULFLLMT_SITE_CODE").replace(" ", "")
		)

        val record: Map[String, Any] = Map(
			"PART_NUM" 				                -> raw_record.getAs[String]("PART_NUM"),
			"FULFLLMT_SITE_CODE" 			    	-> raw_record.getAs[String]("FULFLLMT_SITE_CODE"),
			"PROD_PUBLSHG_CAT_MASK" 	            -> raw_record.getAs[String]("PROD_PUBLSHG_CAT_MASK").replace(" ", ""),
			"PROD_PUBLSHG_AUD_MASK" 			    -> raw_record.getAs[String]("PROD_PUBLSHG_AUD_MASK"),
			"ADD_DATE" 			    				-> raw_record.getAs[String]("ADD_DATE"),
			"ADD_BY_USER_NAME" 						-> raw_record.getAs[Double]("ADD_BY_USER_NAME"),
			"MOD_DATE" 								-> raw_record.getAs[Double]("MOD_DATE"),
			"MOD_BY_USER_NAME" 						-> raw_record.getAs[Double]("MOD_BY_USER_NAME")
		)

        logger.info("RECORD: " + record)

        return true
    }

	def cntry_price_tierd_drct(raw_record: org.apache.spark.sql.Row): Boolean = {

        val primary_keys: Array[String] = Array(
			raw_record.getAs[String]("PART_NUM").replace(" ", ""),
			raw_record.getAs[String]("CNTRY_CODE").replace(" ", ""),
			raw_record.getAs[String]("ISO_CURRNCY_CODE").replace(" ", ""),
			raw_record.getAs[String]("SAP_DISTRIBTN_CHNL_CODE").replace(" ", ""),
			raw_record.getAs[String]("TIERD_SCALE_QTY").replace(" ", ""),
			raw_record.getAs[String]("PRICE_START_DATE").replace(" ", "")
		)

        val record: Map[String, Any] = Map(
			"PART_NUM" 				                -> raw_record.getAs[String]("PART_NUM"),
			"CNTRY_CODE" 			    			-> raw_record.getAs[String]("CNTRY_CODE"),
			"ISO_CURRNCY_CODE" 	            		-> raw_record.getAs[String]("ISO_CURRNCY_CODE").replace(" ", ""),
			"SAP_DISTRIBTN_CHNL_CODE" 			    -> raw_record.getAs[String]("SAP_DISTRIBTN_CHNL_CODE"),
			"TIERD_SCALE_QTY" 			    		-> raw_record.getAs[String]("TIERD_SCALE_QTY"),
			"PRICE_START_DATE" 						-> raw_record.getAs[Double]("PRICE_START_DATE"),
			"PRICE_END_DATE" 						-> raw_record.getAs[Double]("PRICE_END_DATE"),
			"SRP_PRICE" 							-> raw_record.getAs[Double]("SRP_PRICE"),
			"SVP_LEVEL_A" 							-> raw_record.getAs[Double]("SVP_LEVEL_A"),
			"SVP_LEVEL_B" 				    		-> raw_record.getAs[Double]("SVP_LEVEL_B"),
			"SVP_LEVEL_C" 				        	-> raw_record.getAs[Double]("SVP_LEVEL_C"),
			"SVP_LEVEL_D" 				   			-> raw_record.getAs[Double]("SVP_LEVEL_D"),
			"SVP_LEVEL_E" 							-> raw_record.getAs[Double]("SVP_LEVEL_E"),
			"SVP_LEVEL_F" 				        	-> raw_record.getAs[Double]("SVP_LEVEL_F"),
			"SVP_LEVEL_G" 				    		-> raw_record.getAs[Double]("SVP_LEVEL_G"),
			"SVP_LEVEL_H" 							-> raw_record.getAs[Double]("SVP_LEVEL_H"),
			"SVP_LEVEL_I" 				    		-> raw_record.getAs[Double]("SVP_LEVEL_I"),
			"SVP_LEVEL_J" 				    		-> raw_record.getAs[Double]("SVP_LEVEL_J"),
			"SVP_LEVEL_ED" 				        	-> raw_record.getAs[Double]("SVP_LEVEL_ED"),
            "SVP_LEVEL_GV" 				        	-> raw_record.getAs[Double]("SVP_LEVEL_GV"),
            "SAP_EXTRCT_DATE" 				        -> raw_record.getAs[Double]("SAP_EXTRCT_DATE"),
            "ADD_BY_USER_NAME" 				        -> raw_record.getAs[Double]("ADD_BY_USER_NAME"),
            "SAP_ODS_ADD_DATE" 				        -> raw_record.getAs[Double]("SAP_ODS_ADD_DATE"),
			"MOD_BY_USER_NAME" 				        -> raw_record.getAs[Double]("MOD_BY_USER_NAME"),
            "SAP_ODS_MOD_DATE" 				        -> raw_record.getAs[Double]("SAP_ODS_MOD_DATE"),
			"GPH_IND" 				            	-> raw_record.getAs[Double]("GPH_IND")
		)

        logger.info("RECORD: " + record)

        return true
    }

	def cntry_price_drct(raw_record: org.apache.spark.sql.Row): Boolean = {

        val primary_keys: Array[String] = Array(
			raw_record.getAs[String]("PART_NUM").replace(" ", ""),
			raw_record.getAs[String]("CNTRY_CODE").replace(" ", ""),
			raw_record.getAs[String]("ISO_CURRNCY_CODE").replace(" ", ""),
			raw_record.getAs[String]("SAP_DISTRIBTN_CHNL_CODE").replace(" ", ""),
			raw_record.getAs[String]("PRICE_START_DATE").replace(" ", "")
		)

        val record: Map[String, Any] = Map(
			"PART_NUM" 				                -> raw_record.getAs[String]("PART_NUM"),
			"CNTRY_CODE" 			    			-> raw_record.getAs[String]("CNTRY_CODE"),
			"ISO_CURRNCY_CODE" 	            		-> raw_record.getAs[String]("ISO_CURRNCY_CODE").replace(" ", ""),
			"SAP_DISTRIBTN_CHNL_CODE" 			    -> raw_record.getAs[String]("SAP_DISTRIBTN_CHNL_CODE"),
			"PRICE_START_DATE" 						-> raw_record.getAs[Double]("PRICE_START_DATE"),
			"PRICE_END_DATE" 						-> raw_record.getAs[Double]("PRICE_END_DATE"),
			"SRP_PRICE" 							-> raw_record.getAs[Double]("SRP_PRICE"),
			"SVP_LEVEL_A" 							-> raw_record.getAs[Double]("SVP_LEVEL_A"),
			"SVP_LEVEL_B" 				    		-> raw_record.getAs[Double]("SVP_LEVEL_B"),
			"SVP_LEVEL_C" 				        	-> raw_record.getAs[Double]("SVP_LEVEL_C"),
			"SVP_LEVEL_D" 				   			-> raw_record.getAs[Double]("SVP_LEVEL_D"),
			"SVP_LEVEL_E" 							-> raw_record.getAs[Double]("SVP_LEVEL_E"),
			"SVP_LEVEL_F" 				        	-> raw_record.getAs[Double]("SVP_LEVEL_F"),
			"SVP_LEVEL_G" 				    		-> raw_record.getAs[Double]("SVP_LEVEL_G"),
			"SVP_LEVEL_H" 							-> raw_record.getAs[Double]("SVP_LEVEL_H"),
			"SVP_LEVEL_I" 				    		-> raw_record.getAs[Double]("SVP_LEVEL_I"),
			"SVP_LEVEL_J" 				    		-> raw_record.getAs[Double]("SVP_LEVEL_J"),
			"SVP_LEVEL_ED" 				        	-> raw_record.getAs[Double]("SVP_LEVEL_ED"),
            "SVP_LEVEL_GV" 				        	-> raw_record.getAs[Double]("SVP_LEVEL_GV"),
            "SAP_EXTRCT_DATE" 				        -> raw_record.getAs[Double]("SAP_EXTRCT_DATE"),
            "ADD_BY_USER_NAME" 				        -> raw_record.getAs[Double]("ADD_BY_USER_NAME"),
            "SAP_ODS_ADD_DATE" 				        -> raw_record.getAs[Double]("SAP_ODS_ADD_DATE"),
			"MOD_BY_USER_NAME" 				        -> raw_record.getAs[Double]("MOD_BY_USER_NAME"),
            "SAP_ODS_MOD_DATE" 				        -> raw_record.getAs[Double]("SAP_ODS_MOD_DATE"),
			"GPH_IND" 				            	-> raw_record.getAs[Double]("GPH_IND")
		)

        logger.info("RECORD: " + record)

        return true
    }

}
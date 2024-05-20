/*
Name: OnPrem.scala
Description: Logic to handle OnPrem data part information
Created by: Alvaro Gonzalez <alvaro.glez@ibm.com>
Created Date: 2021/06/14
Notes:
Modification:
    date        owner       description
	20221210 	Ricardo		Onprem modification to finish the logic
	20230123	Ricardo		GPH logic added
*/
package com.ibm.dswdiaods.pricing_onprem

import com.ibm.dswdia.core.Database
import com.ibm.dswdia.core.Properties.{db2_ods_properties_streaming}
import OnPrem.insert_record
import Redis.onprem_primary_keys_check
import ApiConnector._

import com.opencsv.CSVWriter
import play.api.libs.json.{JsNull, JsObject, JsValue}
import java.io.{BufferedWriter, FileWriter}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

import java.sql.Types
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.text.SimpleDateFormat
import scala.util.{Failure, Success}
import org.apache.logging.log4j.{LogManager,Logger}
import org.apache.spark.sql.execution.columnar.INT
import com.ibm.db2.cmx.runtime.Data
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.ContentTypes
import java.io._


object OnPrem {
	/**
	  * Validates data incoming from Kafka, when data is valid
	  * this is loaded to base tables in ODS: wwpp1.cntry_price
	  * and wwpp1.cntry_price_tierd.
	  * When data is not valid this is loaded to error tables:
	  * wwpp1.cntry_price_err and wwpp1.cntry_price_err
	  * as well as utol.error_log
	  *
	  * If the data have an start date > than the actual date
	  * is going to be inserted in the wwpp1.futr_cntry_price
	  * except if it is an GPH record.
	  *
	  * @param <code>raw_record</code> an org.apache.spark.sql.Row containing
	  * pricing data from Kafka
	  *
	  * @param  <code>table_name</code> a String containing the table name
	  *
	  * @param pool Core Database object
	  *
	  * @return Boolean -> True for normal exec, False if an error happen
	  */

	val logger: Logger = LogManager.getLogger(this.getClass())

	def cntry_price_upsert(raw_record: org.apache.spark.sql.Row, table_name: String, pool: Database): Boolean = {
		var topic = raw_record.getAs[String]("topic").replace(".", " ").split(" ")

		val base_table = table_name
		var error_table = s"${base_table}_err"

		val futr_table = s"futr_${base_table}"

		val primary_keys: Array[String] = Array(
			raw_record.getAs[String]("PART_NUM").replace(" ", ""),
			raw_record.getAs[String]("CNTRY_CODE"),
			raw_record.getAs[String]("ISO_CURRNCY_CODE"),
			raw_record.getAs[String]("SAP_DISTRIBTN_CHNL_CODE").replace(" ", ""),
			raw_record.getAs[String]("PRICE_START_DATE").replace(" ", "")
		)

		var sap_extrct_date = raw_record.getAs[String]("SAP_EXTRCT_DATE")
		sap_extrct_date = sap_extrct_date.replace(" ", "-") // replace space to dash
												// from '2021-05-04 05:04:07.241965000000'
												// to   '2021-05-04-05:04:07.241965000000'
		sap_extrct_date = sap_extrct_date.replace(":", ".") // replace colon to dash
												// from '2021-05-04 05:04:07.241965000000'
												// to   '2021-05-04-05.04.07.241965000000'
		sap_extrct_date = sap_extrct_date.dropRight(6) // delete extra zeros added as miliseconds
												// from '2021-05-04-05.04.07.241965000000'
												// to   '2021-05-04-05.04.07.241965'

		val record: Map[String, Any] = Map(
			"part_num" 					-> raw_record.getAs[String]("PART_NUM").replace(" ", ""),
			"cntry_code" 				-> raw_record.getAs[String]("CNTRY_CODE"),
			"iso_currncy_code" 			-> raw_record.getAs[String]("ISO_CURRNCY_CODE"),
			"sap_distribtn_chnl_code" 	-> raw_record.getAs[String]("SAP_DISTRIBTN_CHNL_CODE").replace(" ", ""),
			"price_start_date" 			-> raw_record.getAs[String]("PRICE_START_DATE"),
			"price_end_date" 			-> raw_record.getAs[String]("PRICE_END_DATE"),
			"srp_price" 				-> raw_record.getAs[Double]("SRP_PRICE"),
			"svp_level_a" 				-> raw_record.getAs[Double]("SVP_LEVEL_A"),
			"svp_level_b" 				-> raw_record.getAs[Double]("SVP_LEVEL_B"),
			"svp_level_c" 				-> raw_record.getAs[Double]("SVP_LEVEL_C"),
			"svp_level_d" 				-> raw_record.getAs[Double]("SVP_LEVEL_D"),
			"svp_level_e" 				-> raw_record.getAs[Double]("SVP_LEVEL_E"),
			"svp_level_f" 				-> raw_record.getAs[Double]("SVP_LEVEL_F"),
			"svp_level_g" 				-> raw_record.getAs[Double]("SVP_LEVEL_G"),
			"svp_level_h" 				-> raw_record.getAs[Double]("SVP_LEVEL_H"),
			"svp_level_i" 				-> raw_record.getAs[Double]("SVP_LEVEL_I"),
			"svp_level_j" 				-> raw_record.getAs[Double]("SVP_LEVEL_J"),
			"svp_level_ed" 				-> raw_record.getAs[Double]("SVP_LEVEL_ED"),
			"svp_level_gv" 				-> raw_record.getAs[Double]("SVP_LEVEL_GV"),
			"gph_status" 				-> raw_record.getAs[Int]("GPH_STATUS"),
			"sap_extrct_date" 			-> sap_extrct_date
		)

		// Step 1: Validate existing part number in error table
		val err_seq_num: String = get_err_seq_num(error_table, primary_keys, pool)

		if (err_seq_num != ""){
			delete_from_error_log(err_seq_num, pool)
			delete_from_table(error_table, primary_keys, pool)
		}

		// Step 2: Validate primary keys
		if (!validate_primary_keys(primary_keys, base_table, record, pool)){
			return false
		}

		// Step 3: Validate if current price does exist, if so, update its end date to
		// be equal to incoming start date - 1 day

		if (is_update(base_table, primary_keys, pool)){
			update_record(base_table, record, pool)
		} else {
			val current_timestamp = DateTimeFormatter.ofPattern("YYYY-MM-dd").format(LocalDateTime.now())
			val dateformat = new SimpleDateFormat("yyyy-MM-dd")
			val today_date = dateformat.parse(current_timestamp)
			// If the start_date is greater than the local date the record is going to be inserted into the futr table
			// If it is an GPH record is going to be inserted in the bu table
			if(dateformat.parse(raw_record.getAs[String]("PRICE_START_DATE")).compareTo(today_date) >  0 && raw_record.getAs[String]("GPH_STATUS") == '0'){
				if (is_update(futr_table, primary_keys, pool)){
					update_record(futr_table, record, pool)
				}else {
					insert_record(futr_table, record, pool)
				}
			}else{
				insert_record(base_table, record, pool)
			}
		}

		return true
	}


	/**
	  * Validates data incoming from Kafka, when data is valid
	  * this is loaded to base tables in ODS: wwpp1.cntry_price
	  * and wwpp1.cntry_price_tierd.
	  * When data is not valid this is loaded to error tables:
	  * wwpp1.cntry_price_err and wwpp1.cntry_price_err
	  * as well as utol.error_log
	  *
	  * @param <code>record</code> an org.apache.spark.sql.Row containing
	  * pricing data from Kafka
	  *
	  * @param <code>table_name</code> an string containing
	  * the table name
	  *
	  * @param pool Core Database object
	  *
	  * @return Boolean
	  */
	def cntry_price_delete(raw_record: org.apache.spark.sql.Row, table_name: String, pool: Database): Boolean = {
		var topic = raw_record.getAs[String]("topic").replace(".", " ").split(" ")
		val base_table = table_name

		val primary_keys: Array[String] = Array(
			raw_record.getAs[String]("PART_NUM").replace(" ", ""),
			raw_record.getAs[String]("CNTRY_CODE"),
			raw_record.getAs[String]("ISO_CURRNCY_CODE"),
			raw_record.getAs[String]("SAP_DISTRIBTN_CHNL_CODE").replace(" ", ""),
			raw_record.getAs[String]("PRICE_START_DATE")
		)

		val affected_rows = delete_from_table(base_table , primary_keys, pool)

		if (affected_rows < 0){
			logger.info(s"There was a problem with the delete.")
			return false
		}

		logger.info(s"${primary_keys(0)} was deleted properly.")

		return true
	}

	/**
	  * Get error sequence number from error table
	  *
	  * @param table String with the table name
	  * @param parameters Array[String] with the PK value
	  * @param pool Core Database object
	  *
	  * @return String
	  */
	def get_err_seq_num(table: String, parameters: Array[String], pool: Database): String = {

		val query = s"""SELECT err_seq_num
						FROM   wwpp1.${table}
						WHERE  part_num                = '${parameters(0)}'
						AND    cntry_code              = '${parameters(1)}'
						AND    iso_currncy_code        = '${parameters(2)}'
						AND    sap_distribtn_chnl_code = '${parameters(3)}'
						AND    price_start_date        = '${parameters(4)}'"""


		val result = pool.run(query)

		if(result.isEmpty){return ""}

		var err_seq_num: String = result.apply(0).get("ERR_SEQ_NUM").toString()

		return err_seq_num
	}

	/**
	  * Delete record in the error log table
	  *
	  * @param err_seq_num String with the num of error
	  * @param pool Core Database object
	  *
	  * @return None
	  */
	def delete_from_error_log(err_seq_num: String, pool: Database): Unit = {
		val delete = s"DELETE FROM utol.error_log WHERE err_seq_num = $err_seq_num"
		val result = pool.run(delete)
		val affected_rows: Int = result.apply(0).get("Affected rows").toInt
	}

	/**
	  * Delete record from given table
	  *
	  * @param table String with the table name
	  * @param parameters Array[String] with the PK value
	  * @param pool Core Database object
	  *
	  * @return Int
	  */
	def delete_from_table(table: String, parameters: Array[String], pool: Database): Int = {

		val delete: String = s"""DELETE FROM wwpp1.${table}
								WHERE  part_num                = '${parameters(0)}'
								AND    cntry_code              = '${parameters(1)}'
								AND    iso_currncy_code        = '${parameters(2)}'
								AND    sap_distribtn_chnl_code = '${parameters(3)}'
								AND    price_start_date        = '${parameters(4)}'"""

		val result = pool.run(delete)
		val affected_rows: Int = result.apply(0).get("Affected rows").toInt

		return affected_rows
	}

	/**
	 * Validate primary keys
	 * First validate if the record is the redis key
	 * And if there is not, then validate if the record is in the table
	 * @param primary_keys Array[String] with the PK value
	 * @param base_table String with the table name (base)
	 * @param record Map[String, any] with the values to be validate
	 * @param pool Core Database object
	 *
	 * @return Boolean
	 */
	def validate_primary_keys(primary_keys: Array[String], base_table: String, record: Map[String, Any], pool: Database): Boolean = {
		// REDIS validation will be used for MVP2
		val redis_response = onprem_primary_keys_check(primary_keys)

		// Validate incoming part number against WWPP2.WWIDE_FNSHD_PART
		if (redis_response(0) == false){
			if (!validate_part_number(primary_keys(0), pool)){
				val col_name = "part_num"
				val error_description = "part_num is missing in wwpp2.wwide_fnshd_part"

				logger.error(s"Part: ${primary_keys(0)}, ${error_description}")
				set_invalid_part(col_name, base_table, error_description, record, pool)

				return false
			}
		}

		// Validate incoming country code against SHAR2.CODE_DSCR
		if (redis_response(1) == false){
			if (!validate_cntry_code(primary_keys(1), pool)){
				val col_name = "cntry_code"
				val error_description = "cntry_code is missing in shar2.code_dscr"

				logger.error(s"Part: ${primary_keys(0)}, ${error_description}")
				set_invalid_part(col_name, base_table, error_description, record, pool)

				return false
			}
		}

		// Validate incoming iso currncy code against SHAR2.CODE_DSCR
		if (redis_response(2) == false){
			if (!validate_iso_currency_code(primary_keys(2), pool)){
				val col_name = "iso_currncy_code"
				val error_description = "iso_currncy_code is missing in shar2.code_dscr"

				logger.error(s"Part: ${primary_keys(0)}, ${error_description}")
				set_invalid_part(col_name, base_table, error_description, record, pool)

				return false
			}
		}

		// Validate incoming sap distribution channel code against SHAR2.CODE_DSCR
		if (redis_response(3) == false){
			if (!validate_sap_distribtn_chnl_code(primary_keys(3), pool)){
				val col_name = "sap_distribtn_chnl_code"
				val error_description = "sap_distribtn_chnl_code is missing in shar2.code_dscr"

				logger.error(s"Part: ${primary_keys(0)}, ${error_description}")
				set_invalid_part(col_name, base_table, error_description, record, pool)

				return false
			}
		}

		return true
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

	/**
	  * Check if is valid the record
	  *
	  * @param query String with the select statement
	  * @param pool Core Database object
	  *
	  * @return Boolean
	  */
	def is_valid(query: String, pool: Database): Boolean = {
		val result = pool.run(query)

		if(result.isEmpty){ return false }

		return true
	}

	/**
	  * Validate part number
	  *
	  * @param part_num String with the value to be validated
	  * @param pool Core Database object
	  *
	  * @return Boolean
	  */
	def validate_part_number(part_num: String, pool: Database): Boolean = {
		val query: String = s"""SELECT 1
								FROM WWPP2.WWIDE_FNSHD_PART
								WHERE part_num = '${part_num}'"""

		return is_valid(query, pool)
	}

	/**
	  * Validate country code
	  *
	  * @param cntry_code String with the value to be validated
	  * @param pool Core Database object
	  *
	  * @return Boolean
	  */
	def validate_cntry_code(cntry_code: String, pool: Database): Boolean = {
		val query: String = s"""SELECT 1
								FROM  shar2.code_dscr
								WHERE col_name = 'cntry_code'
								AND   code = '${cntry_code}'"""

		return is_valid(query, pool)
	}

	/**
	  * Validate iso currency code
	  *
	  * @param iso_currncy_code String with the value to be validated
	  * @param pool Core Database object
	  *
	  * @return Boolean
	  */
	def validate_iso_currency_code(iso_currncy_code: String, pool: Database): Boolean = {
		val query: String = s"""SELECT 1
								FROM shar2.code_dscr
								WHERE col_name = 'iso_currncy_code'
								AND   code = '${iso_currncy_code}'"""

		return is_valid(query, pool)
	}

	/**
	  * Validate SAP distribution channel code
	  *
	  * @param sap_distribtn_chnl_code String with the value to be validated
	  * @param pool Core Database object
	  *
	  * @return Boolean
	  */
	def validate_sap_distribtn_chnl_code(sap_distribtn_chnl_code: String, pool: Database): Boolean = {
		val query: String = s"""SELECT 1
								FROM  shar2.code_dscr
								WHERE col_name = 'sap_distribtn_chnl_code'
								AND   code = '${sap_distribtn_chnl_code}'"""

		return is_valid(query, pool)
	}

	/**
	  * Insert record in error log table with the sp
	  *
	  * @param parameters Array[Any] with the PK values
	  * @param pool Core Database object
	  *
	  * @return None
	  */
	def insert_error_log(parameters: Array[Any], pool: Database): String = {
		var statement = "CALL utol.dp_insrt_error_log(p_error_date=>?, p_table_name=>?, p_col_name=>?, p_error_dscr=>?, p_err_seq_num=>?, o_status=>?)"
		val conn = pool.ds.getConnection()

		val call_statement = conn.prepareCall(statement)

		call_statement.setString("p_error_date", parameters(0).toString())
		call_statement.setString("p_table_name", parameters(1).toString().split("\\.")(1)) //takes just the name of table 
		call_statement.setString("p_col_name", parameters(2).toString())
		call_statement.setString("p_error_dscr", parameters(3).toString())
		call_statement.setInt("p_err_seq_num", parameters(4).asInstanceOf[Int])

		call_statement.registerOutParameter("o_status", Types.VARCHAR)
		call_statement.executeUpdate()
		return call_statement.getString("p_err_seq_num")
	}

	/**
		* Insert record in the error table in case that one of the PK are missing.
		*
		* @param table String with the name of the table
		* @param record Map[String, Any] with the values to be inserted.
		* @param err_seq_num String with the err num generated
		* @param pool Core Database object
		*
		* @return Int
		*/
	def insert_record_err(table: String, record: Map[String, Any], err_seq_num: BigInt, pool: Database): Int = {
		var affected_rows = -1
		val current_timestamp = get_current_timestamp()

		val insert = s"""INSERT INTO wwpp1.${table}_err (
							part_num,
							cntry_code,
							iso_currncy_code,
							sap_distribtn_chnl_code,
							price_start_date,
							price_end_date,
							srp_price,
							svp_level_a,
							svp_level_b,
							svp_level_c,
							svp_level_d,
							svp_level_e,
							svp_level_f,
							svp_level_g,
							svp_level_h,
							svp_level_i,
							svp_level_j,
							svp_level_ed,
							svp_level_gv,
							sap_extrct_date,
							sap_ods_add_date,
							sap_ods_mod_date,
							err_seq_num
						) VALUES (
							'${record("part_num")}',
							'${record("cntry_code")}',
							'${record("iso_currncy_code")}',
							'${record("sap_distribtn_chnl_code")}',
							'${record("price_start_date")}',
							'${record("price_end_date")}',
							${record("srp_price")},
							${record("svp_level_a")},
							${record("svp_level_b")},
							${record("svp_level_c")},
							${record("svp_level_d")},
							${record("svp_level_e")},
							${record("svp_level_f")},
							${record("svp_level_g")},
							${record("svp_level_h")},
							${record("svp_level_i")},
							${record("svp_level_j")},
							${record("svp_level_ed")},
							${record("svp_level_gv")},
							'${record("sap_extrct_date")}',
							'${current_timestamp}',
							'${current_timestamp}',
							${err_seq_num}
						)"""

		val result = pool.run(insert)
		if(!result.isEmpty){affected_rows = result.apply(0).get("Affected rows").toInt}

		if (affected_rows < 0){
			val message = s"""Problem with wwpp1.${table} insert for:
			part_num                  	= '${record("part_num")}'
			and cntry_code              = '${record("cntry_code")}'
			and iso_currncy_code        = '${record("iso_currncy_code")}'
			and sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
			and price_start_date        = '${record("price_start_date")}'
			"""

			logger.error(message)

			return -1
		}

		logger.info(s"Part: ${record("part_num")} succesfully inserted in the Err table")
		return affected_rows
	}

	/**
	  * Prepare error record for error log and generate the err_seq_num
	  *
	  * @param col_name String with the name of the column missed
	  * @param base_table String with the table of the process
	  * @param error_description String with the description of the error
	  * @param record Map[String, Any] with the values
	  * @param pool Core Database object
	  *
	  * @return None
	  */
	def set_invalid_part(col_name: String, base_table: String, error_description: String, record: Map[String, Any], pool: Database): Unit = {
		val error_id = 0
		val current_timestamp = get_current_timestamp().replace(" ", "-").replace(":", ".")
		val sp_parameters = Array[Any](
			current_timestamp,
			base_table,
			col_name,
			error_description,
			error_id
		)

		insert_error_log(sp_parameters, pool)

		val query = s"""SELECT err_seq_num
						FROM   utol.error_log
						WHERE  error_date	= '${current_timestamp}'
						AND    table_name	= '${base_table}'
						AND    error_dscr	= '${error_description}'"""


		val result = pool.run(query)

		var err_seq_num: BigInt = BigInt(result.apply(0).get("ERR_SEQ_NUM"))

		insert_record_err(base_table, record, err_seq_num, pool)
	}

	/**
	  * Validate if record exists on given table
	  *
	  * @param table String with the table name
	  * @param parameters Array[String] with the PK values
	  * @param pool Core Database object
	  *
	  * @return Boolean
	  */
	def is_update(table: String, parameters: Array[String], pool: Database): Boolean = {
		val query: String = s"""SELECT 1
								FROM  wwpp1.${table}
								WHERE part_num                = '${parameters(0)}'
								AND   cntry_code              = '${parameters(1)}'
								AND   iso_currncy_code        = '${parameters(2)}'
								AND   sap_distribtn_chnl_code = '${parameters(3)}'
								AND   price_start_date        = '${parameters(4)}'"""

		return is_valid(query, pool)
	}

	/**
	  * Insert record in given table, main method of the process
	  *
	  * @param table String with the table name
	  * @param record Map[String, Any] wit the values to be inserted
	  * @param pool Core Database object
	  *
	  * @return Int
	  */
	def insert_record(table: String, record: Map[String, Any], pool: Database): Int = {
		var affected_rows = -1
		val current_timestamp = get_current_timestamp()

		val insert = s"""INSERT INTO wwpp1.${table} (
							part_num,
							cntry_code,
							iso_currncy_code,
							sap_distribtn_chnl_code,
							price_start_date,
							price_end_date,
							srp_price,
							svp_level_a,
							svp_level_b,
							svp_level_c,
							svp_level_d,
							svp_level_e,
							svp_level_f,
							svp_level_g,
							svp_level_h,
							svp_level_i,
							svp_level_j,
							svp_level_ed,
							svp_level_gv,
							sap_extrct_date,
							sap_ods_add_date,
							sap_ods_mod_date,
							gph_ind
						) VALUES (
							'${record("part_num")}',
							'${record("cntry_code")}',
							'${record("iso_currncy_code")}',
							'${record("sap_distribtn_chnl_code")}',
							'${record("price_start_date")}',
							'${record("price_end_date")}',
							${record("srp_price")},
							${record("svp_level_a")},
							${record("svp_level_b")},
							${record("svp_level_c")},
							${record("svp_level_d")},
							${record("svp_level_e")},
							${record("svp_level_f")},
							${record("svp_level_g")},
							${record("svp_level_h")},
							${record("svp_level_i")},
							${record("svp_level_j")},
							${record("svp_level_ed")},
							${record("svp_level_gv")},
							'${record("sap_extrct_date")}',
							'${current_timestamp}',
							'${current_timestamp}',
							${record("gph_status")}
						)"""

		val result = pool.run(insert)
		if(!result.isEmpty){affected_rows = result.apply(0).get("Affected rows").toInt}

		if (affected_rows < 0){
			val message = s"""Problem with wwpp1.${table} insert for:
			part_num                  	= '${record("part_num")}'
			and cntry_code              = '${record("cntry_code")}'
			and iso_currncy_code        = '${record("iso_currncy_code")}'
			and sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
			and price_start_date        = '${record("price_start_date")}'
			"""

			logger.error(message)

			val col_name = "ROW"
			val error_description = "Error: Insert failed"

			set_invalid_part(col_name, table, error_description, record, pool)

			return -1
		}

		// Invalidate records with price_start_date greater than the currently loaded, we want to maintain only the lastest price cable feed as valid,
		// if any price cable was previously loaded its being dimissed, since biz can make price changes because of marketplace they want to keep only latest feed sent.

		val delete = s"""DELETE FROM wwpp1.${table}
							WHERE part_num              = '${record("part_num")}'
							and cntry_code              = '${record("cntry_code")}'
							and iso_currncy_code        = '${record("iso_currncy_code")}'
							and sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
							and price_start_date 		> '${record("price_start_date")}'
							"""

		var affected_rows_delete = -1
		val result_delete = pool.run(delete)
		if(!result_delete.isEmpty){affected_rows_delete = result_delete(0).get("Affected rows").toInt}

		if (affected_rows_delete > 0){
			val message = s"""Record deleted wwpp1.${table} for:
								part_num                  	= '${record("part_num")}'
								and cntry_code              = '${record("cntry_code")}'
								and iso_currncy_code        = '${record("iso_currncy_code")}'
								and sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
								and price_start_date        > '${record("price_start_date")}'"""

			//logger.info(message)
		}

		// if the price_start_date is older than current date, we want to keep 
		// ONLY the record being inserted and expire/delete the records with price start date older

		val delete_older = s"""DELETE FROM wwpp1.${table}
							WHERE part_num              = '${record("part_num")}'
							and cntry_code              = '${record("cntry_code")}'
							and iso_currncy_code        = '${record("iso_currncy_code")}'
							and sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
							and price_start_date 		< '${record("price_start_date")}'
							and price_start_date 		< CURRENT DATE
							"""

		var affected_rows_delete_older = -1
		val result_delete_older = pool.run(delete_older)
		if(!result_delete_older.isEmpty){affected_rows_delete_older = result_delete_older(0).get("Affected rows").toInt}

		if (affected_rows_delete_older > 0){
			val message_older = s"""Record deleted wwpp1.${table} for:
								part_num              = '${record("part_num")}'
								and cntry_code              = '${record("cntry_code")}'
								and iso_currncy_code        = '${record("iso_currncy_code")}'
								and sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
								and price_start_date 		< '${record("price_start_date")}'
								and price_start_date 		< CURRENT DATE'"""

			//logger.info(message_older)
		}

		logger.info(s"Part: ${record("part_num")} succesfully inserted into wwpp1.${table}")
		return affected_rows
	}

	/**
	  * Update record in given table, secondary method of the process
	  *
	  * @param table String with the table name
	  * @param record Map[String, Any] with the values to be inserted
	  * @param pool Core Database object
	  *
	  * @return Int
	  */
	def update_record(table: String, record: Map[String, Any], pool: Database): Int = {
		var affected_rows = -1
		val update = s"""UPDATE wwpp1.${table}
						SET price_end_date          	= '${record("price_end_date")}',
							srp_price               	= ${record("srp_price")},
							svp_level_a             	= ${record("svp_level_a")},
							svp_level_b             	= ${record("svp_level_b")},
							svp_level_c             	= ${record("svp_level_c")},
							svp_level_d             	= ${record("svp_level_d")},
							svp_level_e             	= ${record("svp_level_e")},
							svp_level_f             	= ${record("svp_level_f")},
							svp_level_g             	= ${record("svp_level_g")},
							svp_level_h             	= ${record("svp_level_h")},
							svp_level_i             	= ${record("svp_level_i")},
							svp_level_j             	= ${record("svp_level_j")},
							svp_level_ed            	= ${record("svp_level_ed")},
							svp_level_gv            	= ${record("svp_level_gv")},
							sap_extrct_date         	= '${record("sap_extrct_date")}',
							mod_by_user_name        	= 'ODSOPER',
							sap_ods_mod_date        	= '${get_current_timestamp()}',
							gph_ind            			= ${record("gph_status")}
						WHERE part_num                  = '${record("part_num")}'
							and cntry_code              = '${record("cntry_code")}'
							and iso_currncy_code        = '${record("iso_currncy_code")}'
							and sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
							and price_start_date 		= '${record("price_start_date")}'
							and sap_extrct_date 		< '${record("sap_extrct_date")}'
							"""

		val result = pool.run(update)
		if(!result.isEmpty){affected_rows = result.apply(0).get("Affected rows").toInt}

		//logger.info("---- Aff ROWS update -----:" + affected_rows)

		if (affected_rows < 0){
			val message = s"""Problem with wwpp1.${table} update for:
								part_num                  	= '${record("part_num")}'
								and cntry_code              = '${record("cntry_code")}'
								and iso_currncy_code        = '${record("iso_currncy_code")}'
								and sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
								and price_start_date        = '${record("price_start_date")}'"""

			logger.error(message)

			val col_name = "ROW"
			val error_description = "Error: Update failed"

			set_invalid_part(col_name, table, error_description, record, pool)

			return -1
		}
		// Invalidate records with price_start_date greater than the currently loaded, we want to maintain only the lastest price cable feed as valid,
		// if any price cable was previously loaded its being dimissed, since biz can make price changes because of marketplace they want to keep only latest feed sent.

		val delete = s"""DELETE FROM wwpp1.${table}
							WHERE part_num              = '${record("part_num")}'
							and cntry_code              = '${record("cntry_code")}'
							and iso_currncy_code        = '${record("iso_currncy_code")}'
							and sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
							and price_start_date 		> '${record("price_start_date")}'
							"""

		var affected_rows_delete = -1
		val result_delete = pool.run(delete)
		if(!result_delete.isEmpty){affected_rows_delete = result_delete(0).get("Affected rows").toInt}

		if (affected_rows_delete > 0){
			val message = s"""Record deleted wwpp1.${table} for:
								part_num                  	= '${record("part_num")}'
								and cntry_code              = '${record("cntry_code")}'
								and iso_currncy_code        = '${record("iso_currncy_code")}'
								and sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
								and price_start_date        > '${record("price_start_date")}'"""

			//logger.info(message)
		}

		logger.info(s"Part: ${record("part_num")} succesfully updated in wwpp1.${table}")
		return affected_rows
	}

	//----------------------------------------------------------------//
	def futr_pricing_csv_send(pool: Database, price_start_date: String): Boolean = {
		val query = "SELECT * FROM WWPP1.FUTR_CNTRY_PRICE WHERE PRICE_START_DATE >= " + price_start_date
		val result = pool.run(query)

		val csv_result = saveToCSV(result)

		if(!csv_result){
			logger.error("Failed to save/write the csv result.")
			return false
		}

		val http_result = send_json_doc("tmp/csv/futr_pricing/")

		if(http_result < 0){
			logger.error("Failed to send the csv result to the api.")
			return false
		}

		return true
	}

	def saveToCSV(str: Seq[JsValue]): Boolean = {
		val outputFile = new BufferedWriter(new FileWriter("Result.csv"))
		val csvWriter = new CSVWriter(outputFile)
		val data = str.head
		data match {
			case JsObject(fields) => {
			var listOfRecords = new ListBuffer[Array[String]]()
			//val csvFields = Array("Open","High","Low","Close","Volume")
			//listOfRecords += csvFields
			fields.values.foreach(value => {
				val jsObject = value.as[JsObject]
				val nameList = List(jsObject("1. open").toString, jsObject("2. high").toString, jsObject("3. low").toString, jsObject("4. close").toString, jsObject("5. volume").toString)
				listOfRecords += Array(nameList.toString)
				csvWriter.writeAll(listOfRecords.toList.asJava)
				println("Written!")
				outputFile.close()
			})
			}
			case JsNull => println("Null")
							return false
		}

		return true
	}

	def writeToFile(content: String, campaignId: String): File = {
		val tempFolder = System.getProperty("tmp/csv/futr_pricing/")
		val file = new File(s"$tempFolder/retailers_$campaignId.csv")
		val bw = new BufferedWriter(new FileWriter(file))
		bw.write(content)
		bw.close()
		file
	}

}

/*
Name: SaaS.scala (before OnPrem)
Description: Logic to handle SaaS data part information
Created by: Alvaro Gonzalez <alvaro.glez@ibm.com>
Created Date: 2021/06/14
Notes:
Modification:
    date        owner       description
2021-09-21 Octavio Sanchez Adaptation for SaaS
2023-01-25 Ricardo Maravilla GPH and core Added
*/

package com.ibm.dswdiaods.pricing_saas

import com.ibm.dswdiaods.pricing_onprem.Redis.saas_primary_keys_check

import com.ibm.dswdia.core.Database
import com.ibm.dswdia.core.Properties.{db2_ods_properties_streaming}

import java.sql.Types
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.text.SimpleDateFormat
import scala.util.{Failure, Success}
import org.apache.logging.log4j.{LogManager,Logger}
import java.time.LocalDate

object SaaS {
	val logger: Logger = LogManager.getLogger(this.getClass())

	/**
	  * Validates data incoming from Kafka, when data is valid
	  * this is loaded to base tables in ODS: wwpp1.cntry_price_tierd
	  * When data is not valid this is loaded to error tables:
	  * wwpp1.cntry_price_err and utol.error_log
	  *
	  * @param <code>raw_record</code> an org.apache.spark.sql.Row containing
	  * pricing data from Kafka
	  *
	  * @param  <code>table_name</code> a String containing the table name
	  * @param pool
	  *
	  * @return Boolean
	  */
	def cntry_price_upsert(table_name: String, raw_record: org.apache.spark.sql.Row, pool: Database): Boolean = {
		val topic = raw_record.getAs[String]("topic").split("\\.")

		val base_table = s"wwpp1.${table_name}"
		val error_table = s"wwpp1.${table_name}_err"

		val primary_keys: Array[String] = Array(
			raw_record.getAs[String]("PART_NUM").replace(" ", ""),
			raw_record.getAs[String]("CNTRY_CODE"),
			raw_record.getAs[String]("ISO_CURRNCY_CODE"),
			raw_record.getAs[String]("SAP_DISTRIBTN_CHNL_CODE").replace(" ", ""),
			raw_record.getAs[String]("TIERD_SCALE_QTY").replace(" ", ""),
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
		val current_timestamp = get_current_timestamp()

        val record: Map[String, String] = Map(
			"part_num" 					-> raw_record.getAs[String]("PART_NUM").replace(" ", ""),
			"cntry_code" 				-> raw_record.getAs[String]("CNTRY_CODE"),
			"iso_currncy_code" 			-> raw_record.getAs[String]("ISO_CURRNCY_CODE"),
			"sap_distribtn_chnl_code" 	-> raw_record.getAs[String]("SAP_DISTRIBTN_CHNL_CODE").replace(" ", ""),
			"tierd_scale_qty"           -> raw_record.getAs[String]("TIERD_SCALE_QTY").replace(" ", ""),
			"price_start_date" 			-> raw_record.getAs[String]("PRICE_START_DATE").replace(" ", ""),
			"price_end_date" 			-> raw_record.getAs[String]("PRICE_END_DATE").replace(" ", ""),
			"srp_price" 				-> raw_record.getAs[String]("SRP_PRICE").replace(" ", ""),
			"svp_level_a" 				-> raw_record.getAs[String]("SVP_LEVEL_A").replace(" ", ""),
			"svp_level_b" 				-> raw_record.getAs[String]("SVP_LEVEL_B").replace(" ", ""),
			"svp_level_c" 				-> raw_record.getAs[String]("SVP_LEVEL_C").replace(" ", ""),
			"svp_level_d" 				-> raw_record.getAs[String]("SVP_LEVEL_D").replace(" ", ""),
			"svp_level_e" 				-> raw_record.getAs[String]("SVP_LEVEL_E").replace(" ", ""),
			"svp_level_f" 				-> raw_record.getAs[String]("SVP_LEVEL_F").replace(" ", ""),
			"svp_level_g" 				-> raw_record.getAs[String]("SVP_LEVEL_G").replace(" ", ""),
			"svp_level_h" 				-> raw_record.getAs[String]("SVP_LEVEL_H").replace(" ", ""),
			"svp_level_i" 				-> raw_record.getAs[String]("SVP_LEVEL_I").replace(" ", ""),
			"svp_level_j" 				-> raw_record.getAs[String]("SVP_LEVEL_J").replace(" ", ""),
			"svp_level_ed" 				-> raw_record.getAs[String]("SVP_LEVEL_ED").replace(" ", ""),
			"svp_level_gv" 				-> raw_record.getAs[String]("SVP_LEVEL_GV").replace(" ", ""),
			"gph_status" 				-> raw_record.getAs[String]("GPH_STATUS"),
			"sap_extrct_date" 			-> sap_extrct_date
		)

		// Step 1: Validate existing part number in error table
		val err_seq_num: String = get_err_seq_num(error_table, record, pool)

		if (err_seq_num != ""){
			delete_from_error_log(err_seq_num, pool)
			delete_error_record(error_table, record, pool)
		}

		// Step 2: Validate primary keys
		if (!validate_primary_keys(primary_keys, base_table, record, pool)){
			return false
		}

		// Step 3: Evaluate upsert
		if (is_update(base_table, record, pool)){
			update_record(base_table, current_timestamp, record, pool)
		}
		else {
			insert_record(base_table, current_timestamp, record, pool)
			update_end_date(base_table, current_timestamp, record, pool)
		}

		return true
	}

	/**
	  * Validates data incoming from Kafka, when data is valid
	  * this is loaded to base table in ODS: wwpp1.cntry_price_tierd
	  * When data is not valid this is loaded to error table
	  * wwpp1.cntry_price_tierd_err as well as utol.error_log
	  *
	  * @param <code>table_name</code> an String with the main table of
	  * the process
	  *
	  * @param <code>record</code> an org.apache.spark.sql.Row containing
	  * pricing data from Kafka
	  *
	  * @return Boolean
	  */
	def cntry_price_delete(table_name: String, raw_record: org.apache.spark.sql.Row, pool: Database): Boolean = {
		val base_table = s"WWPP1.${table_name}"

		val record_pk: Map[String, String] = Map(
			"part_num" 					-> raw_record.getAs[String]("PART_NUM").replace(" ", ""),
			"cntry_code" 				-> raw_record.getAs[String]("CNTRY_CODE"),
			"iso_currncy_code" 			-> raw_record.getAs[String]("ISO_CURRNCY_CODE"),
			"sap_distribtn_chnl_code" 	-> raw_record.getAs[String]("SAP_DISTRIBTN_CHNL_CODE").replace(" ", ""),
			"tierd_scale_qty"           -> raw_record.getAs[String]("TIERD_SCALE_QTY").replace(" ", ""),
			"price_start_date" 			-> raw_record.getAs[String]("PRICE_START_DATE").replace(" ", "")
		)

		val affected_rows = delete_from_table(base_table, record_pk, pool)

		if (affected_rows < 0){
			logger.info(s"There was a problem with the delete.")
			return false
		}

        logger.info(s"${raw_record.getAs[String]("PART_NUM").replace(" ", "")} was deleted properly, rows affected: ${affected_rows} ")

		return true
	}

	/**
	  * Get error sequence number from error table
	  *
	  * @param table String with the target table
	  * @param record Map[String, String] with the PK values
	  * @param poll Core Database object
	  *
	  * @return String
	  */
	def get_err_seq_num(table: String, record: Map[String, String], pool: Database): String = {
		val query = s"""SELECT err_seq_num
						FROM   ${table}
						WHERE  part_num                = '${record("part_num")}'
						AND    cntry_code              = '${record("cntry_code")}'
						AND    iso_currncy_code        = '${record("iso_currncy_code")}'
						AND    sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
						AND    tierd_scale_qty         = ${record("tierd_scale_qty")}"""

		val result = pool.run(query)
		if(result.isEmpty){return ""}

		var err_seq_num: String = result.apply(0).get("ERR_SEQ_NUM").toString()

		return err_seq_num
	}

	/**
	  * Delete record in the error log table in UTOL
	  *
	  * @param err_seq_num String with the err num
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
	  * @param table String with the target table
	  * @param record Map[String, String] with the PK values
	  * @param pool Core Database object
	  *
	  * @return Int
	  */
	def delete_from_table(table: String, record: Map[String, String], pool: Database): Int = {
		val delete: String = s"""DELETE FROM ${table}
								WHERE  part_num                = '${record("part_num")}'
								AND    cntry_code              = '${record("cntry_code")}'
								AND    iso_currncy_code        = '${record("iso_currncy_code")}'
								AND    sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
								AND    tierd_scale_qty         = '${record("tierd_scale_qty")}'
								AND    price_start_date        = '${record("price_start_date")}'"""

		val result = pool.run(delete)
		val affected_rows: Int = result.apply(0).get("Affected rows").toInt
		affected_rows match {
			case i if (i < 0) => {

				val message = s"""Problem with ${table} Delete for:
								part_num                  	= '${record("part_num")}'
								and cntry_code              = '${record("cntry_code")}'
								and iso_currncy_code        = '${record("iso_currncy_code")}'
								and sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
								and tierd_scale_qty         = '${record("tierd_scale_qty")}'
								and price_start_date        = '${record("price_start_date")}'"""

				logger.error(message)

				val col_name = "ROW"
				val error_description = "Error: Delete record failed"
				set_invalid_part(col_name, table, error_description, record, pool)
				return -1
			}
			case i if (i == 0) =>{
                logger.error(s"Part: ${record("part_num")} is not on ${table} , rows affected: ${i} ")
			    return i
			}
			case i if (i > 0) =>{
                logger.info(s"Part: ${record("part_num")} was deleted properly from ${table}, rows affected: ${i} ")
				return i
			}
	    }
		return affected_rows
	}

	/**
	 * Validate primary keys in Redis, if is not in Redis we
	 * check on the database
	 *
	 * @param primary_keys Array[String] with the PK
	 * @param base_table String with the name of the base table of the process
	 * @param record Map[String, String] with the values to be validated
	 * @param pool Core Database object
	 *
	 * @return Boolean
	 */
	def validate_primary_keys(primary_keys: Array[String], base_table: String, record: Map[String, String], pool: Database): Boolean = {
		//REDIS validation will be used for MVP2
		val redis_response = saas_primary_keys_check(primary_keys)

		//Validate incoming part number against WWPP2.WWIDE_FNSHD_PART
		if(redis_response(0) == false){
			if(!validate_part_number(primary_keys(0), pool)){
				val col_name = "part_num"
				val error_description = "part_num is missing in wwpp2.wwide_fnshd_part"

				logger.error(s"Part: ${primary_keys(0)}, ${error_description}")
				set_invalid_part(col_name, base_table, error_description, record, pool)

				return false
			}
		}

		//validate incoming country cose against SHAR2.CODE_DSCR
		if(redis_response(1) == false){
			if(!validate_part_number(primary_keys(1), pool)){
				val col_name = "cntry_code"
				val error_description = "cntry_code is missing in shar2.code_dscr"

				logger.error(s"Part: ${primary_keys(1)}, ${error_description}")
				set_invalid_part(col_name, base_table, error_description, record, pool)

				return false
			}
		}

		//validate incoming country cose against SHAR2.CODE_DSCR
		if(redis_response(2) == false){
			if(!validate_part_number(primary_keys(2), pool)){
				val col_name = "iso_currncy_code"
				val error_description = "iso_currncy_code is missing in shar2.code_dscr"

				logger.error(s"Part: ${primary_keys(2)}, ${error_description}")
				set_invalid_part(col_name, base_table, error_description, record, pool)

				return false
			}
		}

		//validate incoming country cose against SHAR2.CODE_DSCR
		if(redis_response(3) == false){
			if(!validate_part_number(primary_keys(3), pool)){
				val col_name = "sap_distribtn_chnl_code"
				val error_description = "sap_distribtn_chnl_code is missing in shar2.code_dscr"

				logger.error(s"Part: ${primary_keys(3)}, ${error_description}")
				set_invalid_part(col_name, base_table, error_description, record, pool)

				return false
			}
		}

		return true
	}

	/**
	  * Validate part number
	  *
	  * @param part_num String with the value of the record
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
	  * @param cntry_code String with the value of the record
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
	  * @param iso_currncy_code String with the value of the record
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
	  * @param sap_distribtn_chnl_code String with the value of the record
	  * @param pool Core Database object
	  *
	  * @return Boolean
	  */
	def validate_sap_distribtn_chnl_code(sap_distribtn_chnl_code: String, pool: Database) : Boolean = {
		val query: String = s"""SELECT 1
								FROM  shar2.code_dscr
								WHERE col_name = 'sap_distribtn_chnl_code'
								AND   code = '${sap_distribtn_chnl_code}'"""

		return is_valid(query, pool)
	}

	/**
	  * Validate price_start_date < price_end_date
	  *
	  * @param price_start_date String with the start date value of the record
	  * @param price_end_date String with the end date value of the record
	  *
	  * @return Boolean
	  */
	def validate_end_after_start_date(price_start_date: String, price_end_date: String): Boolean = {

		lazy val start_date = LocalDate.parse(price_start_date)
		lazy val end_date = LocalDate.parse(price_end_date)
		if(end_date.isAfter(start_date)) return true

		return false
	}

	/**
	  * Validate price_end_date > current_date
	  *
	  * @param price_end_date String with the end date of the record
	  *
	  * @return Boolean
	  */
	def validate_end_date_is_future(price_end_date: String): Boolean = {

		lazy val current_date = LocalDate.now
		lazy val end_date = LocalDate.parse(price_end_date)
		if(end_date.isAfter(current_date)) return true

		return false
	}

	/**
	  * Insert record in error log table
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
	  * Prepare error record for error log and get the err_seq_num
	  *
	  * @param col_name String with the value of the column with the error
	  * @param base_table String with the name of the base table
	  * @param error_description String with the description of the error
	  * @param record Map[String, String] with the values
	  * @param pool Core Database object
	  *
	  * @return None
	  */
	def set_invalid_part(col_name: String, base_table: String, error_description: String, record: Map[String, String], pool: Database): Unit = {
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
						AND    table_name	= '${base_table.split("\\.")(1)}'
						AND    error_dscr	= '${error_description}'"""


		val result = pool.run(query)

		var err_seq_num: BigInt = BigInt(result.apply(0).get("ERR_SEQ_NUM"))

		insert_record_err(base_table, record, err_seq_num, pool)

	}

	/**
	  * Validate if record exists on given table
	  *
	  * @param table String with the name of the target table
	  * @param record Map[String, String] with the values of the record
	  * @param pool Core Database object
	  *
	  * @return Boolean
	  */
	def is_update(table: String, record: Map[String, String], pool: Database): Boolean = {
		val query: String = s"""SELECT 1
								FROM  ${table}
								WHERE part_num                = '${record("part_num")}'
								AND   cntry_code              = '${record("cntry_code")}'
								AND   iso_currncy_code        = '${record("iso_currncy_code")}'
								AND   sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
								AND   tierd_scale_qty         = '${record("tierd_scale_qty")}'
						        AND   price_start_date        = '${record("price_start_date")}'""" 

		return is_valid(query, pool)
	}

	/**
	  * Insert record in given table
	  *
	  * @param table String with the name of the target table
	  * @param current_timestamp String with the timestamp at this moment
	  * @param record Map[String, String] with the values of the record
	  * @param pool Core Database object
	  *
	  * @return Int
	  */
	def insert_record(table: String, current_timestamp: String, record: Map[String, String], pool: Database): Int = {

		val insert = s"""INSERT INTO ${table} (
							part_num,
							cntry_code,
							iso_currncy_code,
							sap_distribtn_chnl_code,
							tierd_scale_qty,
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
							${record("tierd_scale_qty")},
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

		var affected_rows = -1
		val result = pool.run(insert)
		if(!result.isEmpty){affected_rows = result.apply(0).get("Affected rows").toInt}

		if (affected_rows < 0){
			val message = s"""Problem with ${table} insert for:
			part_num                  	= '${record("part_num")}'
			and cntry_code              = '${record("cntry_code")}'
			and iso_currncy_code        = '${record("iso_currncy_code")}'
			and sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
			and tierd_scale_qty         = '${record("tierd_scale_qty")}'
			and price_start_date        = '${record("price_start_date")}'
			"""

			logger.error(message)

			val col_name = "ROW"
			val error_description = "Error: Insert failed"

			set_invalid_part(col_name, table, error_description, record, pool)

			return -1
		}

		// Invalidate records with price_start_date greater than the currently loaded, we want to maintain only the lastest price cable feed as valid, if any price cable was previously loaded its being dimissed, since biz can make price changes because of marketplace they want to keep only latest feed sent.
        // a trigger will log this delete trasaction on wwpp1.cntry_price_tierd_dlt_hist table

		val delete: String = s"""DELETE FROM ${table}
								WHERE  part_num                = '${record("part_num")}'
								AND    cntry_code              = '${record("cntry_code")}'
								AND    iso_currncy_code        = '${record("iso_currncy_code")}'
								AND    sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
								AND    tierd_scale_qty         = '${record("tierd_scale_qty")}'
								AND    price_start_date        > '${record("price_start_date")}'"""

		var affected_rows_delete = -1
		val result_delete = pool.run(delete)
		if(!result_delete.isEmpty){affected_rows_delete = result_delete(0).get("Affected rows").toInt}

		if (affected_rows_delete < 0){

			val message_delete = s"""Problem with ${table} Delete for:
								part_num                  	= '${record("part_num")}'
								and cntry_code              = '${record("cntry_code")}'
								and iso_currncy_code        = '${record("iso_currncy_code")}'
								and sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
								and tierd_scale_qty         = '${record("tierd_scale_qty")}'
								and price_start_date        > '${record("price_start_date")}'"""

			logger.error(message_delete)
		}
		// if the price_start_date is older than current date, we want to keep ONLY the record being inserted and expire/delete the records with price start date older
        //a trigger will log this delete trasaction on wwpp1.cntry_price_tierd_dlt_hist table

		if(record("price_start_date") <= get_current_timestamp()){
			val delete_older: String = s"""DELETE FROM ${table}
								WHERE  part_num                = '${record("part_num")}'
								AND    cntry_code              = '${record("cntry_code")}'
								AND    iso_currncy_code        = '${record("iso_currncy_code")}'
								AND    sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
								AND    tierd_scale_qty         = '${record("tierd_scale_qty")}'
								AND    price_start_date        < '${record("price_start_date")}'
								AND    price_start_date        < CURRENT DATE"""

			var affected_rows_delete_older = -1
			val result_delete_older = pool.run(delete_older)
			if(!result_delete_older.isEmpty){affected_rows_delete_older = result_delete_older(0).get("Affected rows").toInt}

			if (affected_rows_delete_older < 0){

				val message_older = s"""Problem with ${table} Delete for:
									part_num                  	= '${record("part_num")}'
									and cntry_code              = '${record("cntry_code")}'
									and iso_currncy_code        = '${record("iso_currncy_code")}'
									and sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
									and tierd_scale_qty         = '${record("tierd_scale_qty")}'
									and price_start_date        < '${record("price_start_date")}'
									and price_start_date        < CURRENT DATE"""

				logger.error(message_older)
			}
		}

		// if INSERTed just executed, created a overlapped record update previously existing records with price end date = new records price start day - one day.. if records exists udpate will work, otherwise, update will update zero records, what would be ok.
		val current_timestamp_new = get_current_timestamp()
		val update_overlapped: String = s"""UPDATE(
												SELECT PRICE_END_DATE,SAP_ODS_MOD_DATE,ROW_NUMBER () OVER (PARTITION BY part_num, cntry_code,iso_currncy_code, sap_distribtn_chnl_code,tierd_scale_qty ORDER BY price_start_date desc) AS row_id
												FROM ${table} 
												WHERE  part_num                = '${record("part_num")}' 
												AND    cntry_code              = '${record("cntry_code")}' 
												AND    iso_currncy_code        = '${record("iso_currncy_code")}' 
												AND    sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}' 
												AND    tierd_scale_qty         = ${record("tierd_scale_qty")} 
											) 
											SET PRICE_END_DATE   = (date(${record("price_start_date")})-1 DAY), 
											SAP_ODS_MOD_DATE = '${current_timestamp_new}' 
											WHERE row_id=2
								"""

		var affected_rows_overlapped = -1
		val result_rows_overlapped = pool.run(update_overlapped)
		if(!result_rows_overlapped.isEmpty){affected_rows_overlapped = result_rows_overlapped(0).get("Affected rows").toInt}
		if (affected_rows_overlapped < 0){

			val message_overlapped = s"""UPDATE(SELECT PRICE_END_DATE,SAP_ODS_MOD_DATE,ROW_NUMBER () OVER (PARTITION BY part_num, cntry_code,iso_currncy_code, sap_distribtn_chnl_code,tierd_scale_qty ORDER BY price_start_date desc) AS row_id
								FROM ${table}
								WHERE  part_num                = '${record("part_num")}'
								AND    cntry_code              = '${record("cntry_code")}'
								AND    iso_currncy_code        = '${record("iso_currncy_code")}'
								AND    sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
								AND    tierd_scale_qty         = '${record("tierd_scale_qty")}'
								) 
								SET PRICE_END_DATE   = (date('${record("price_start_date")}') - 1 DAY), 
                            	SAP_ODS_MOD_DATE = '${current_timestamp}' + 1 MICROSECOND
								WHERE row_id=1'"""
			logger.error(message_overlapped)
			}

		logger.info(s"Part: ${record("part_num")} succesfully inserted")
		return affected_rows
	}

    /**
	  * Change end date for recently inserted records (almost the same key except by start date)
	  *
	  * @param table String with the name of the target table
	  * @param current_timestamp String with the timestamp at this moment
	  * @param record Map[String, String] with the values of the record
	  * @param pool Core Database object
	  *
	  * @return Int
	  */
	def update_end_date(table: String, current_timestamp: String, record: Map[String, String], pool: Database): Int = {
		val update_end = s"""MERGE INTO ${table} cpt
                               USING(
                                 SELECT  
                                   PART_NUM,
                               	   CNTRY_CODE,
                               	   ISO_CURRNCY_CODE,
                               	   SAP_DISTRIBTN_CHNL_CODE, 
                               	   TIERD_SCALE_QTY,
                                   PRICE_START_DATE,
                                   PRICE_END_DATE,
                                   (
                                     Lead(PRICE_start_DATE) over (  
                                   
                                        PARTITION BY PART_NUM, 
                                                     CNTRY_CODE, 
                                                     ISO_CURRNCY_CODE, 
                                                     SAP_DISTRIBTN_CHNL_CODE, 
                                                     TIERD_SCALE_QTY
                                        ORDER BY 
                                                     PRICE_START_DATE
                                     ) -1 DAY
                                   
                                   ) AS CALCULATED_END_DATE
                                    
                                 FROM ${table}
                                 WHERE 
                               	   PART_NUM = '${record("part_num")}' AND
                               	   CNTRY_CODE = '${record("cntry_code")}' AND
                               	   ISO_CURRNCY_CODE = '${record("iso_currncy_code")}' AND
                               	   SAP_DISTRIBTN_CHNL_CODE = '${record("sap_distribtn_chnl_code")}' AND 
                               	   TIERD_SCALE_QTY = '${record("tierd_scale_qty")}'
                               ) cpt1                           
                               ON 
                               cpt.PART_NUM = cpt1.PART_NUM AND
                               cpt.CNTRY_CODE = cpt1.CNTRY_CODE AND
                               cpt.ISO_CURRNCY_CODE = cpt1.ISO_CURRNCY_CODE AND
                               cpt.SAP_DISTRIBTN_CHNL_CODE = cpt1.SAP_DISTRIBTN_CHNL_CODE AND
                               cpt.TIERD_SCALE_QTY = cpt1.TIERD_SCALE_QTY AND
                               cpt.PRICE_START_DATE = cpt1.PRICE_START_DATE AND
                               cpt.PRICE_END_DATE > cpt1.CALCULATED_END_DATE
                               
                               WHEN MATCHED THEN UPDATE SET 
                               PRICE_END_DATE = CALCULATED_END_DATE,
                               SAP_ODS_MOD_DATE = cast('${current_timestamp}' AS TIMESTAMP) +  1 MICROSECOND,
                               MOD_BY_USER_NAME = 'ODSOPER'"""

		var affected_rows = -1
		val result = pool.run(update_end)
		if(!result.isEmpty){affected_rows = result.apply(0).get("Affected rows").toInt}

		affected_rows match {
			case i if (i < 0) => {
				val message = s"""Problem with ${table} update for:
								part_num                  	= '${record("part_num")}'
								and cntry_code              = '${record("cntry_code")}'
								and iso_currncy_code        = '${record("iso_currncy_code")}'
								and sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
								and tierd_scale_qty         = '${record("tierd_scale_qty")}'"""

				logger.error(message)

				val col_name = "ROW"
				val error_description = "Error: Update end date failed"
				set_invalid_part(col_name, table, error_description, record, pool)
				
				return -1
			}
			case i if (i == 0) =>{
				logger.debug(s"Part: ${record("part_num")} has not end dates for update")
				return i
			} 
			case i if (i > 0) =>{
				logger.info(s"Part: ${record("part_num")} end dates was updated for ${i} records")
				return i
			} 
		}
		return affected_rows
	}

	/**
	  * Update record in given table
	  *
	  * @param table String with the name of the target table
	  * @param current_timestamp String with the timestamp at this moment
	  * @param record Map[String, String] with the values of the record
	  * @param pool Core Database object
	  *
	  * @return Int
	  */
	def update_record(table: String, current_timestamp: String, record: Map[String, String], pool: Database): Int = {
		val update = s"""UPDATE ${table}
						SET 
							price_end_date          	= '${record("price_end_date")}',
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
							sap_ods_mod_date        	= '${current_timestamp}',
							gph_ind            			= ${record("gph_status")}
						WHERE part_num                  = '${record("part_num")}'
							and cntry_code              = '${record("cntry_code")}'
							and iso_currncy_code        = '${record("iso_currncy_code")}'
							and sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
							and tierd_scale_qty         = '${record("tierd_scale_qty")}'
							and price_start_date        = '${record("price_start_date")}' 
							and sap_extrct_date 		< '${record("sap_extrct_date")}'"""

		var affected_rows = -1
		val result = pool.run(update)
		if(!result.isEmpty){affected_rows = result.apply(0).get("Affected rows").toInt}

		if (affected_rows < 0){
			val message = s"""Problem with ${table} update for:
								part_num                  	= '${record("part_num")}'
								and cntry_code              = '${record("cntry_code")}'
								and iso_currncy_code        = '${record("iso_currncy_code")}'
								and sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
								and tierd_scale_qty         = '${record("tierd_scale_qty")}'
								and price_start_date        = '${record("price_start_date")}'"""

			logger.error(message)

			val col_name = "ROW"
			val error_description = "Error: Update failed"

			set_invalid_part(col_name, table, error_description, record, pool)

			return -1
		}

		// Invalidate records with price_start_date greater than the currently loaded, 
		// we want to maintain only the lastest price cable feed as valid, 
		// if any price cable was previously loaded its being dimissed, 
		// since biz can make price changes because of marketplace they want to keep only latest feed sent.
        // a trigger will log this delete trasaction on wwpp1.cntry_price_tierd_dlt_hist table

		val delete: String = s"""DELETE FROM ${table}
								WHERE  part_num                = '${record("part_num")}'
								AND    cntry_code              = '${record("cntry_code")}'
								AND    iso_currncy_code        = '${record("iso_currncy_code")}'
								AND    sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
								AND    tierd_scale_qty         = '${record("tierd_scale_qty")}'
								AND    price_start_date        > '${record("price_start_date")}'"""

		var affected_rows_delete = -1
		val result_delete = pool.run(delete)
		if(!result_delete.isEmpty){affected_rows_delete = result.apply(0).get("Affected rows").toInt}

		if (affected_rows_delete < 0){
			val message_delete = s"""Problem with ${table} Delete for:
								part_num                  	= '${record("part_num")}'
								and cntry_code              = '${record("cntry_code")}'
								and iso_currncy_code        = '${record("iso_currncy_code")}'
								and sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
								and tierd_scale_qty         = '${record("tierd_scale_qty")}'
								and price_start_date        = '${record("price_start_date")}'"""

			logger.error(message_delete)
	    }

		logger.info(s"Part: ${record("part_num")} succesfully updated")
		return affected_rows
	}

	/**
	  * Insert record in error table
	  *
	  * @param error_table String with the name of the err table
	  * @param current_timestamp String with the timestamp at the moment
	  * @param err_seq_num String with the number of error
	  * @param record Map[String, String] with the values of the record
	  * @param pool Core Database object
	  *
	  * @return Int
	  */
	def insert_error_record(error_table: String, current_timestamp: String, err_seq_num: String ,record: Map[String, String], pool: Database): Int = {
		val insert = s"""INSERT INTO ${error_table} (
							part_num,
							cntry_code,
							iso_currncy_code,
							sap_distribtn_chnl_code,
							tierd_scale_qty,
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
							${record("tierd_scale_qty")},
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
							'${err_seq_num}'
						)"""

		var affected_rows = -1
		val result = pool.run(insert)
		if(!result.isEmpty){affected_rows = result.apply(0).get("Affected rows").toInt}

		if (affected_rows < 0){
			val message = s"""Problem with ${error_table} insert for:
			part_num                  	= '${record("part_num")}'
			and cntry_code              = '${record("cntry_code")}'
			and iso_currncy_code        = '${record("iso_currncy_code")}'
			and sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
			and tierd_scale_qty         = '${record("tierd_scale_qty")}'
			and price_start_date        = '${record("price_start_date")}'
			"""

			logger.error(s"On error table: ${message}")

			return -1
		}
		else {

		}
		logger.info(s"Part: ${record("part_num")} succesfully inserted on error table")
		return affected_rows
	}

	/**
	  * Insert record in error table
	  *
	  * @param table String with the name of the target table
	  * @param record Map[String, String] with the values of the record
	  * @param pool Core Database object
	  *
	  * @return None
	  */
	def delete_error_record(table: String, record: Map[String, String], pool:Database): Unit = {
		val delete: String = s"""DELETE FROM ${table}
								WHERE  part_num                = '${record("part_num")}'
								AND    cntry_code              = '${record("cntry_code")}'
								AND    iso_currncy_code        = '${record("iso_currncy_code")}'
								AND    sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
								AND    tierd_scale_qty         = '${record("tierd_scale_qty")}'"""

		var affected_rows_delete = -1
		val result_delete = pool.run(delete)
		if(!result_delete.isEmpty){affected_rows_delete = result_delete(0).get("Affected rows").toInt}

		if (affected_rows_delete < 0){
			val message = s"""Problem with ${table} Delete for:
								part_num                  	= '${record("part_num")}'
								and cntry_code              = '${record("cntry_code")}'
								and iso_currncy_code        = '${record("iso_currncy_code")}'
								and sap_distribtn_chnl_code = '${record("sap_distribtn_chnl_code")}'
								and tierd_scale_qty         = '${record("tierd_scale_qty")}'"""

			logger.error(message)
		}
	}

	/**
	  * Check if the record is valid
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
		* Insert record in the error table
		*
		* @param table String with the name of the target table
		* @param record Map[String, Any] with the values of the record
		* @param err_seq_num String with the number of error
		* @param pool Core Database object
		*
		* @return Int
	*/
	def insert_record_err(table: String, record: Map[String, Any], err_seq_num: BigInt, pool: Database): Int = {
		var affected_rows = -1
		val current_timestamp = get_current_timestamp()

		val insert = s"""INSERT INTO ${table}_err (
							part_num,
							cntry_code,
							iso_currncy_code,
							sap_distribtn_chnl_code,
							tierd_scale_qty,
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
							'${record("tierd_scale_qty")}',
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
}
/*
Name: Sink.scala
Description: Create a Sink class for multiples streams
Created by: Ricardo Maravilla <ricardo.maravilla@ibm.com>
Created Date: 2023/06/01
Modification:
    date        owner                   description
*/

package com.ibm.dswdiaods.pricing_saas

import com.ibm.dswdia.core.Properties.Properties
import com.ibm.dswdia.core.{Cloudant, Database, KafkaWriter}
import com.ibm.dswdia.core.Utils.{email_alert, parse_json}
import com.ibm.dswdiaods.pricing_saas.SaaS._
import Exception._

import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.logging.log4j.LogManager
import org.apache.spark.SparkException

import org.apache.spark.sql.streaming.StreamingQueryListener
import java.sql.Timestamp


class Sink (spark: SparkSession, properties: Properties, table_name: String) extends ForeachWriter[Row]{

	val logger = LogManager.getLogger(this.getClass())
	var part_num = ""
	var date = ""

	// Records that the column A_ENTTYP could have
	val put = "PT"
	val update_before = "UB"
	val update_after = "UP"
	val delete = "DL"
	val refresh_record = "RR"

	val upsert_values: Array[String] = Array(
		put,
		update_after,
		update_before,
		refresh_record
	)

	logger.info("Setting config")
	var pool: Database = _

	override def open(partition_id: Long, version: Long): Boolean = {
        Class.forName(properties.getProperty("driver"))
        pool = new Database("Marketplace" ,properties)
        true
    }

    override def process(record: org.apache.spark.sql.Row): Unit = {
		if (upsert_values contains record.getAs[String]("A_ENTTYP")) {
			cntry_price_upsert(table_name, record, pool)
			} else {
			cntry_price_delete(table_name, record, pool)
		}
	}

	override def close(error_or_null: Throwable): Unit = {
		pool.close
		if (error_or_null != null) {
			logger.error(s"Trigger notification to DSW DIA ODS Intrefaces squad about error: ${error_or_null}")
			logger.error("preparing error mail")
			val halt = new ApplicationHalt(error_or_null, "Marketplace", part_num)
			halt.send_error_mail
			throw error_or_null
		}
	}
}


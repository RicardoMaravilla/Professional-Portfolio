/*
Name: Sink.scala
Description: Create a Sink class for multiples streams
Created by: Ricardo Maravilla <ricardo.maravilla@ibm.com>
Created Date: 2023/06/01
Modification:
    date        owner                   description
*/

import com.ibm.dswdia.core.Properties.Properties
import com.ibm.dswdia.core.{Cloudant, Database, KafkaWriter}
import com.ibm.dswdia.core.Utils.{email_alert, parse_json}
import Exception._
import QueryData._
import JsonMaker._
import ApiConnector._
import MP._

import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.logging.log4j.LogManager
import org.apache.spark.SparkException

import org.apache.spark.sql.streaming.StreamingQueryListener
import java.sql.Timestamp


class Sink (spark: SparkSession, properties: Properties) extends ForeachWriter[Row]{

	val logger = LogManager.getLogger(this.getClass())
	var part_num = ""
	var date = ""
	logger.info("Setting config")
	var pool: Database = _

	override def open(partition_id: Long, version: Long): Boolean = {
        Class.forName(properties.getProperty("driver"))
        pool = new Database("Marketplace" ,properties)
        true
    }

    override def process(record: org.apache.spark.sql.Row): Unit = {
		part_num = record.getAs[String]("PART_NUM").replace(" ","")
		date = record.getAs[Timestamp]("MAX_SAP_ODS_TIME").toString()
		logger.info("Record: " + record)
		main_process(part_num, record.getAs[String]("topic"), pool, date)
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
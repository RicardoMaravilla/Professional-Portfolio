/*
Name: StreamHandler.scala
Description: Stream handler to connect to kafka topics and retrieve messages
Created by: Alvaro Gonzalez <alvaro.glez@ibm.com>
Created Date: 2021/05/07
Notes:
	1.- Document reference for entry type: https://www.ibm.com/support/pages/filtering-what-are-all-enttyp-values
Modification:
    date        owner       description
	20210908	Alvaro		Add environment variable to set checkpoint path
	20221114	Ricardo		New structure with core added
	20230120	Ricardo		GPH Logic Added
*/

package com.ibm.dswdiaods.pricing_onprem

import com.ibm.dswdia.core.Database
import com.ibm.dswdia.core.Properties.{kafka_spark_properties_read, dswdia_conf,db2_ods_properties_streaming}
import OnPrem.{cntry_price_upsert, cntry_price_delete}

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import scala.util.Properties.envOrElse
import scala.collection.JavaConverters._
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import za.co.absa.abris.config.{AbrisConfig, FromAvroConfig}
import za.co.absa.abris.avro.functions.from_avro

object StreamHandler {
	val logger: Logger = LogManager.getLogger(this.getClass())
	val app_name = "Streaming_Onprem"
	//?Kafka read configurations
	logger.info("Create kafka configuration")
	val topic = envOrElse("KAFKA_TOPICS","").toString().replace("\n", "")

	// Topic for the BU Process
	val bu_topic = topic.split(",")(0)
	// Topic for the GPH Process
	val gph_topic = topic.split(",")(1)

	// Kafka options creation
	//kafka_spark_properties_read.put("startingOffsets" ,"latest")
	kafka_spark_properties_read.put("startingOffsets" ,"earliest")
	kafka_spark_properties_read.put("failOnDataLoss" , "true")
	kafka_spark_properties_read.put("groupIdPrefix" , app_name)

	val streaming_options_bu = kafka_spark_properties_read.asScala
	val streaming_options_gph = streaming_options_bu.clone()

	// Suscribe to the bu topic
	streaming_options_bu+=("subscribe" ->bu_topic)
	// Suscribe to the gph topic
    streaming_options_gph+=("subscribe" ->gph_topic)

	val environment = dswdia_conf.getString("bigdata.environment")

	// Checkpoint location for each topic
	val checkpoint_path = dswdia_conf.getString("spark.checkpoint.path")
	val checkpoint_path_bu = s"${checkpoint_path}/streaming_cntry_price_onprem/${environment}/bu"
	val checkpoint_path_gph = s"${checkpoint_path}/streaming_cntry_price_onprem/${environment}/gph"


	val schema_registry_config_bu = Map[String, String](
			"schema.registry.url" -> envOrElse("SCHEMA_REGISTRY_URL","").toString().replace("\n", "")
		)

	val schema_registry_config_gph = Map[String, String](
			"schema.registry.url" -> envOrElse("SCHEMA_REGISTRY_URL","").toString().replace("\n", "")
		)

	/**
	  * Method to process every row in the kafka topic
	  *
	  * @param <code>spark</code> an SparkSeesion with the config to
	  * be used
	  *
	  * @param <code>args</code> an String with the value given in the args of the exec
	  *
	  * @return List[DataStreamWriter[Row]] with the execution per topic
	  */
	def get_streaming_query(spark: SparkSession,args: String): List[DataStreamWriter[Row]] = {

		import spark.implicits._
		val logger: Logger = LogManager.getLogger(this.getClass())

		logger.info("Create abris configuration for BU")
		val from_avro_config_bu: FromAvroConfig = AbrisConfig
			.fromConfluentAvro
			.downloadReaderSchemaByLatestVersion
			.andTopicNameStrategy(bu_topic)
			.usingSchemaRegistry(schema_registry_config_bu)

		logger.info("Create abris configuration for GPH")
		val from_avro_config_gph: FromAvroConfig = AbrisConfig
			.fromConfluentAvro
			.downloadReaderSchemaByLatestVersion
			.andTopicNameStrategy(gph_topic)
			.usingSchemaRegistry(schema_registry_config_gph)

		logger.info("Create 'inputDF' dataframe for BU")
		val inputDF_bu = spark
			.readStream
			.format("kafka")
			.options(streaming_options_bu)
			.load()

		logger.info("Create 'inputDF' dataframe for GPH")
		val inputDF_gph = spark
			.readStream
			.format("kafka")
			.options(streaming_options_gph)
			.load()


		logger.info("Create 'raw_data' dataframe for BU")
		val raw_data_bu = inputDF_bu.select(
			from_avro(col("value"), from_avro_config_bu).as("data"),
			col("topic")
		)

		logger.info("Create 'raw_data' dataframe for GPH")
		val raw_data_gph = inputDF_gph.select(
			from_avro(col("value"), from_avro_config_gph).as("data"),
			col("topic")
		)

		logger.info("Create 'data' dataframe for BU")
		val data_bu = raw_data_bu.select(
			"topic",
			"data.PART_NUM",
			"data.CNTRY_CODE",
			"data.ISO_CURRNCY_CODE",
			"data.SAP_DISTRIBTN_CHNL_CODE",
			"data.PRICE_START_DATE",
			"data.PRICE_END_DATE",
			"data.SRP_PRICE",
			"data.SVP_LEVEL_A",
			"data.SVP_LEVEL_B",
			"data.SVP_LEVEL_C",
			"data.SVP_LEVEL_D",
			"data.SVP_LEVEL_E",
			"data.SVP_LEVEL_F",
			"data.SVP_LEVEL_G",
			"data.SVP_LEVEL_H",
			"data.SVP_LEVEL_I",
			"data.SVP_LEVEL_J",
			"data.SVP_LEVEL_ED",
			"data.SVP_LEVEL_GV",
			"data.SAP_EXTRCT_DATE",
			"data.GPH_STATUS",
			"data.A_ENTTYP"
		)

		logger.info("Create 'data' dataframe for GPH")
		val data_gph = raw_data_gph.select(
			"topic",
			"data.PART_NUM",
			"data.CNTRY_CODE",
			"data.ISO_CURRNCY_CODE",
			"data.SAP_DISTRIBTN_CHNL_CODE",
			"data.PRICE_START_DATE",
			"data.PRICE_END_DATE",
			"data.SRP_PRICE",
			"data.SVP_LEVEL_A",
			"data.SVP_LEVEL_B",
			"data.SVP_LEVEL_C",
			"data.SVP_LEVEL_D",
			"data.SVP_LEVEL_E",
			"data.SVP_LEVEL_F",
			"data.SVP_LEVEL_G",
			"data.SVP_LEVEL_H",
			"data.SVP_LEVEL_I",
			"data.SVP_LEVEL_J",
			"data.SVP_LEVEL_ED",
			"data.SVP_LEVEL_GV",
			"data.SAP_EXTRCT_DATE",
			"data.GPH_STATUS",
			"data.A_ENTTYP"
		)

		data_bu.printSchema()
		data_gph.printSchema()

		val table_name = args.toString().split(" ")(1)

		logger.info("Start streaming cntry price query")
		logger.info(s"Table name: ${table_name}")
		logger.info("VERSION: 2.0")

        val writer = new Sink(spark, properties = db2_ods_properties_streaming, table_name)

		// Query to process the data from the BU topic
		val query_bu = data_bu
			.writeStream
			.foreach(writer)
			.option("checkpointLocation", checkpoint_path_bu)
			.queryName("pricing_onprem")

	/*	val query_bu = data_bu.writeStream.foreach(
			new ForeachWriter[org.apache.spark.sql.Row] {
				var pool: Database = _

				def open(partition_id: Long, version: Long): Boolean = {
					pool = new Database(app_name, db2_ods_properties_streaming)
					return true
				}

				def process(record: org.apache.spark.sql.Row): Unit = {
					if (upsert_values contains record.getAs[String]("A_ENTTYP")) {
						cntry_price_upsert(record, table_name, pool)
					} else {
						cntry_price_delete(record, table_name, pool)
					}
				}

				def close(error_or_null: Throwable): Unit = {
					//
					pool.close()
				}
			}
			).option("checkpointLocation", checkpoint_path_bu)*/

		// Query to process the data from the GPH topic
		val query_gph = data_gph
			.writeStream
			.foreach(writer)
			.option("checkpointLocation", checkpoint_path_gph)
			.queryName("pricing_onprem_gph")

	/*	val query_gph = data_gph.writeStream.foreach(
			new ForeachWriter[org.apache.spark.sql.Row] {
				var pool: Database = _

				def open(partition_id: Long, version: Long): Boolean = {
					pool = new Database(app_name, db2_ods_properties_streaming)
					return true
				}

				def process(record: org.apache.spark.sql.Row): Unit = {
					if (upsert_values contains record.getAs[String]("A_ENTTYP")) {
						cntry_price_upsert(record, table_name, pool)
					} else {
						cntry_price_delete(record, table_name, pool)
					}
				}

				def close(error_or_null: Throwable): Unit = {
					//
					pool.close()
				}
			}
			).option("checkpointLocation", checkpoint_path_gph) */

		//Return value
		List(query_bu,query_gph)

	}

}
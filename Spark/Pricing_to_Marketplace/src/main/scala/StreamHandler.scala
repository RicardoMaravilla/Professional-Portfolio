/*
Name: StreamHandlerOnPrem.scala
Description: Stream handler to connect to kafka topics and retrieve messages
Created by: Ricardo Maravilla <ricardo.maravilla@ibm.com>
Created Date: 2023/01/22
Notes:
	1.- Document reference for entry type: https://www.ibm.com/support/pages/filtering-what-are-all-enttyp-values
Modification:
    date        owner       description
*/

//package com.ibm.dswdiaods.pricing_marketplace

import PartInfo.{wwdie_fnshd_good_sap, prod_publshg, cntry_price_drct, cntry_price_tierd_drct}
import QueryData._
import JsonMaker._
import ApiConnector._

import com.ibm.dswdia.core.Database
import com.ibm.dswdia.core.Properties.{kafka_spark_properties_read, dswdia_conf,db2_ods_properties_streaming}
import com.ibm.dswdia.core.Utils.{email_alert, parse_json}

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
import com.ibm.db2.jcc.am.p

object StreamHandler {
	val logger: Logger = LogManager.getLogger(this.getClass())
	val app_name = "Marketplace"
	//?Kafka read configurations
	logger.info("Create kafka configuration")
	val topic = envOrElse("KAFKA_TOPICS","").toString().replace("\n", "")

	val wwide_topic = topic.split(",")(0)
	val publshg_topic = topic.split(",")(1)
	val onprem_topic = topic.split(",")(2)
	val saas_topic = topic.split(",")(3)
	val batch_saas_topic = topic.split(",")(4)
	val batch_onprem_topic = topic.split(",")(5)


	// Kafka options creation
	//kafka_spark_properties_read.put("startingOffsets" ,"latest")
	kafka_spark_properties_read.put("startingOffsets" ,"earliest")
	kafka_spark_properties_read.put("failOnDataLoss" , "false")
	kafka_spark_properties_read.put("groupIdPrefix" , app_name)

	val streaming_options_wwide = kafka_spark_properties_read.asScala
    val streaming_options_publshg = streaming_options_wwide.clone()
    val streaming_options_onprem = streaming_options_wwide.clone()
    val streaming_options_saas = streaming_options_wwide.clone()
	val streaming_options_batch_saas = streaming_options_wwide.clone()
	val streaming_options_batch_onprem = streaming_options_wwide.clone()

	// Suscribe to the bu topic
	streaming_options_wwide+=("subscribe" ->wwide_topic)
    streaming_options_publshg+=("subscribe" ->publshg_topic)
	streaming_options_onprem+=("subscribe" ->onprem_topic)
	streaming_options_saas+=("subscribe" ->saas_topic)
	streaming_options_batch_saas+=("subscribe" ->batch_saas_topic)
	streaming_options_batch_onprem+=("subscribe" ->batch_onprem_topic)


	val environment = dswdia_conf.getString("bigdata.environment")

	var checkpoint_path = dswdia_conf.getString("spark.checkpoint.path")
	checkpoint_path = s"${checkpoint_path}/streaming_pricing_marketplace/${environment}/mp"

	val schema_registry_config = Map[String, String](
			"schema.registry.url" -> envOrElse("SCHEMA_REGISTRY_URL","").toString().replace("\n", "")
		)

	/**
	  *
	  * This method process every row of the topic and will send the json doc.
	  *
	  * @param spark Sparksession instance
	  *
	  * @return left boolean with false, right json object with the info
	*/
	def get_streaming_query(spark: SparkSession): DataStreamWriter[Row] = {

		import spark.implicits._
		val logger: Logger = LogManager.getLogger(this.getClass())

		logger.info("Create abris configuration")
		val from_avro_config_wwide: FromAvroConfig = AbrisConfig
			.fromConfluentAvro
			.downloadReaderSchemaByLatestVersion
			.andTopicNameStrategy(wwide_topic)
			.usingSchemaRegistry(schema_registry_config)

		val from_avro_config_publshg: FromAvroConfig = AbrisConfig
			.fromConfluentAvro
			.downloadReaderSchemaByLatestVersion
			.andTopicNameStrategy(publshg_topic)
			.usingSchemaRegistry(schema_registry_config)

		val from_avro_config_onprem: FromAvroConfig = AbrisConfig
			.fromConfluentAvro
			.downloadReaderSchemaByLatestVersion
			.andTopicNameStrategy(onprem_topic)
			.usingSchemaRegistry(schema_registry_config)

		val from_avro_config_saas: FromAvroConfig = AbrisConfig
			.fromConfluentAvro
			.downloadReaderSchemaByLatestVersion
			.andTopicNameStrategy(saas_topic)
			.usingSchemaRegistry(schema_registry_config)

		val from_avro_config_batch_saas: FromAvroConfig = AbrisConfig
			.fromConfluentAvro
			.downloadReaderSchemaByLatestVersion
			.andTopicNameStrategy(batch_saas_topic)
			.usingSchemaRegistry(schema_registry_config)

		val from_avro_config_batch_onprem: FromAvroConfig = AbrisConfig
			.fromConfluentAvro
			.downloadReaderSchemaByLatestVersion
			.andTopicNameStrategy(batch_onprem_topic)
			.usingSchemaRegistry(schema_registry_config)

		logger.info("Create 'inputDF' dataframe")

		val inputDF_wwide = spark
			.readStream
			.format("kafka")
			.options(streaming_options_wwide)
			.load()

		val inputDF_publshg = spark
			.readStream
			.format("kafka")
			.options(streaming_options_publshg)
			.load()

		val inputDF_onprem = spark
			.readStream
			.format("kafka")
			.options(streaming_options_onprem)
			.load()

		val inputDF_saas = spark
			.readStream
			.format("kafka")
			.options(streaming_options_saas)
			.load()

		val inputDF_batch_saas = spark
			.readStream
			.format("kafka")
			.options(streaming_options_batch_saas)
			.load()

		val inputDF_batch_onprem = spark
			.readStream
			.format("kafka")
			.options(streaming_options_batch_onprem)
			.load()

		logger.info("Create 'raw_data' dataframe")

		val raw_data_wwide = inputDF_wwide.select(
			from_avro(col("value"), from_avro_config_wwide).as("data"),
			col("topic")
		)

		val raw_data_publshg = inputDF_publshg.select(
			from_avro(col("value"), from_avro_config_publshg).as("data"),
			col("topic")
		)

		val raw_data_onprem = inputDF_onprem.select(
			from_avro(col("value"), from_avro_config_onprem).as("data"),
			col("topic")
		)

		val raw_data_saas = inputDF_saas.select(
			from_avro(col("value"), from_avro_config_saas).as("data"),
			col("topic")
		)

		val raw_data_batch_saas = inputDF_batch_saas.select(
			from_avro(col("value"), from_avro_config_batch_saas).as("data"),
			col("topic")
		)

		val raw_data_batch_onprem = inputDF_batch_onprem.select(
			from_avro(col("value"), from_avro_config_batch_onprem).as("data"),
			col("topic")
		)

		logger.info("Create 'data' dataframe")

		val data_wwide = raw_data_wwide.select(
		"topic",
		"data.PART_NUM",
        "data.A_ENTTYP",
        "data.A_CCID",
        "data.A_TIMSTAMP",
        "data.A_USER",
        "data.A_CCID",
		).withColumn("SAP_ODS_TIME", to_timestamp(col("A_TIMSTAMP")))

		data_wwide.printSchema()

		val data_publshg = raw_data_publshg.select(
		"topic",
		"data.PART_NUM",
        "data.A_ENTTYP",
        "data.A_CCID",
        "data.A_TIMSTAMP",
        "data.A_USER",
        "data.A_CCID",
		).withColumn("SAP_ODS_TIME", to_timestamp(col("A_TIMSTAMP")))

		data_publshg.printSchema()

		val data_onprem = raw_data_onprem.select(
		"topic",
		"data.PART_NUM",
		"data.SAP_ODS_MOD_DATE",
        "data.A_ENTTYP",
        "data.A_CCID",
        //"data.A_TIMSTAMP",
        "data.A_USER",
        "data.A_CCID",
		).withColumn("SAP_ODS_TIME", to_timestamp(col("SAP_ODS_MOD_DATE")))

		data_onprem.printSchema()

		val data_saas = raw_data_saas.select(
		"topic",
		"data.PART_NUM",
		"data.SAP_ODS_MOD_DATE",
        "data.A_ENTTYP",
        "data.A_CCID",
        //"data.A_TIMSTAMP",
        "data.A_USER",
        "data.A_CCID",
		).withColumn("SAP_ODS_TIME", to_timestamp(col("SAP_ODS_MOD_DATE")))

		data_saas.printSchema()

		val data_batch_saas = raw_data_batch_saas.select(
		"topic",
		"data.PART_NUM",
		"data.ODS_MOD_DATE",
        "data.A_ENTTYP",
        "data.A_CCID",
        //"data.A_TIMSTAMP",
        "data.A_USER",
        "data.A_CCID",
		).withColumn("SAP_ODS_TIME", to_timestamp(col("ODS_MOD_DATE")))

		data_batch_saas.printSchema()

		val data_batch_onprem = raw_data_batch_onprem.select(
		"topic",
		"data.PART_NUM",
		"data.ODS_MOD_DATE",
        "data.A_ENTTYP",
        "data.A_CCID",
        //"data.A_TIMSTAMP",
        "data.A_USER",
        "data.A_CCID",
		).withColumn("SAP_ODS_TIME", to_timestamp(col("ODS_MOD_DATE")))

		data_batch_onprem.printSchema()

		val one_queue = data_onprem
			.union(data_saas)
			.union(data_publshg)
			.union(data_wwide)
			.union(data_batch_saas)
			.union(data_batch_onprem)
			.withWatermark("SAP_ODS_TIME", "20 minutes")
			.groupBy(col("topic"), col("PART_NUM"), window(col("SAP_ODS_TIME"), "10 minutes"))
			.agg(max(col("SAP_ODS_TIME")).as("MAX_SAP_ODS_TIME"))
			.filter(col("MAX_SAP_ODS_TIME") > "2023-08-20 00:00") // final filter 2023-06-01 00:00

		one_queue.printSchema()

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

		// val table_name = args.toString().split(" ")(1)

		logger.info("Start streaming pricing to marketplace query")
		logger.info("VERSION: 1.2")

        val writer = new Sink(spark, properties = db2_ods_properties_streaming)

		logger.debug("Create streaming queries")
		val one_query = one_queue
			.writeStream
			.foreach(writer)
			.option("checkpointLocation", checkpoint_path)
			.queryName("MP")

		one_query

	}

}
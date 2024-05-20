/*
Name: Redis.scala
Description: Logic to use redis to make comparations. (JEDIS)
Created by: Ricardo Maravilla <ricardo.maravilla@ibm.com>
Created Date: 2022/07/25
Notes:
Modification:
    date        owner       description
20230125        Ricardo     Core Added
*/

package com.ibm.dswdiaods.pricing_onprem

import redis.clients.jedis._
import scala.util.Properties.envOrElse
import com.ibm.dswdia.core.Properties._
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.apache.logging.log4j.{LogManager,Logger}

object Redis {

  val logger: Logger = LogManager.getLogger(this.getClass())

  var client = redis_pool()
	val redis_topic = envOrElse("REDIS_TOPIC","").toString().replace("\n", "")

  /**
  * Only one conn check
  */
  def get_redis_client(): JedisPooled = {
    if(client == null){
        client = redis_pool()
    }
    client
  }

  /**
  * Create a connection pool
  */
	def redis_pool(): JedisPooled = {

      //val url = s"rediss://${secrets_streaming_map.get("redis_username")}:${secrets_streaming_map.get("redis_password")}@${dswdia_conf.getString("redis.dswdiacloud.host")}:${dswdia_conf.getString("redis.dswdiacloud.port")}"
      val url= ""
      val pool = new JedisPooled(url)
      return pool
	}

/**
  * Method to check the 4 PK in redis for saas logic.
  * @param primary_keys Array[String] with the values to validate
  *
  * @return Vector -> with the response of the topic (true,false)
  */
  def saas_primary_keys_check(primary_keys: Array[String]): scala.collection.immutable.Vector[Any] = {

    client = get_redis_client()

		var future_response = Vector[Any] (
			client.sismember(redis_topic + ":part_num", primary_keys(0)),
			client.sismember(redis_topic + ":cntry_code", primary_keys(1)),
			client.sismember(redis_topic + ":iso_currncy_code", primary_keys(2)),
			client.sismember(redis_topic + ":sap_distribtn_chnl_code", primary_keys(3))
    )

		future_response
	}
}

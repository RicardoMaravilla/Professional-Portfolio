/*
	Name: Redis.scala
	Description: Basic operations to Redis Database
	Created by: Octavio Sanchez <octavio.sanchez@ibm.com>
	Created Date: 2022/08/01
	Modification:
		date				owner			 description
*/
//package com.ibm.dswdiaods.pricing_marketplace

import redis.clients.jedis._
import scala.util.Properties.envOrElse
import com.ibm.dswdia.core.Properties._
import org.apache.logging.log4j.{LogManager,Logger}

object Redis {
  	val logger = LogManager.getLogger(this.getClass())
	val redis_topic = envOrElse("REDIS_TOPIC","").toString().replace("\n", "")

	def redis_pool(): JedisPooled = {

        //val url = s"rediss://${secrets_streaming_map.get("redis_username")}:${secrets_streaming_map.get("redis_password")}@${dswdia_conf.getString("redis.dswdiacloud.host")}:${dswdia_conf.getString("redis.dswdiacloud.port")}"
		val url= ""
		val pool = new JedisPooled(url)
		return pool
	}

	/**
	  *
	  * This method tells if the part num exists on the redis cache.
	  *
	  * @param part_num string with the part number of the record
	  *
	  * @return boolean with the result
	*/
    def get_prod_publshg(part_num: String): Boolean = {
        val pool = redis_pool()

		var response = pool.sismember(redis_topic + ":part_num", part_num)

		response
	}
}

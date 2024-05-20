/*
Name: RedistTest.scala
Description: test for extract data with redis pool
Created by: Octavio Sanchez <octavio.sanchez@ibm.com>
Created Date: 2022/07/22
Notes:
Modification:
    date        owner       description
*/
package com.ibm.dswdiaods.pricing_onprem

import org.scalatest.funsuite.AnyFunSuite
import Redis.redis_pool
import scala.util.Properties.envOrElse
import org.apache.log4j.Logger
class RedisTest extends AnyFunSuite {
    //System.setProperty("javax.net.ssl.trustStore", envOrElse("SSL_KEY",""))
    val logger = Logger.getLogger(this.getClass())
    val pool = redis_pool().keys("*")
    test("redis_test: Get keys"){assert(pool.isEmpty == false)}

}

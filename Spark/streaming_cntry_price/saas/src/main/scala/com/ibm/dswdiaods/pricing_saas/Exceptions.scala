/*
Name: Exceptions.scala
Description: Handle on exception actions
Created by: Octavio Sanchez <Octavio Sanchez@ibm.com>
Created Date: 2023/05/20
Modification:
    date        owner                   description
*/

package com.ibm.dswdiaods.pricing_saas

import com.ibm.dswdia.core.Properties.dswdia_conf
import com.ibm.dswdia.core.Utils.email_alert
import java.io.{StringWriter, PrintWriter}
import scala.collection.mutable.{Map => MutableMap}

class ApplicationHalt(t: Throwable, app_name: String, part_num: String) {

	val mailto = dswdia_conf.getString("dswdia.ods.alerts")
	val mailfrom = dswdia_conf.getString("dswdia.ods.interfaces")
	val environment = dswdia_conf.getString("bigdata.environment")
	val mailConfig = MutableMap("from" -> mailfrom, "to" -> mailto, "subject" -> s"Streaming: ${app_name}")

	def send_error_mail(): Unit = {
		var mailBody = new StringWriter
		t.printStackTrace(new PrintWriter(mailBody))
		//send mail
		email_alert(
			mailfrom,
			mailto,
			s"Application: ${mailConfig("subject")} fails on ${environment} environment. The application is Down!",
			s"<b> PART_NUM that needs to be refeeded: ${part_num} </b><b> [${environment}] The application: ${mailConfig("subject")} has thrown an exception: </b><br> ${mailBody.toString().replaceAll(" at ", " at <br>").replaceAll("Caused by: ", "<br>Caused by: <br>")}",
		)
	}
}

/*
Name: OnPrem_insert_futr.scala
Description: Scenario 1 - Logic to test if the data are inserted and updated in the correct table.
  - insert
  - update
  - before
      record non-exists in base table
  - after
      record exists in base table
Created by: Ricardo Maravilla <ricardo.maravilla@ibm.com>
Created Date: 2021/10/18
Notes:
Modification:
    date        owner       description
*/

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
// import Database.{delete_record, execute_dml, is_valid, look_record, select => dbselect}

import java.sql.ResultSet

class OnPrem_load_cntry_price extends AnyFunSuite {
/*
    // Schema with the columns names from cntry price table
    val schema: StructType = StructType(Array(
      StructField("topic", StringType),
      StructField("PART_NUM", StringType),
      StructField("CNTRY_CODE", StringType),
      StructField("ISO_CURRNCY_CODE", StringType),
      StructField("SAP_DISTRIBTN_CHNL_CODE", StringType),
      StructField("PRICE_START_DATE", StringType),
      StructField("PRICE_END_DATE", StringType),
      StructField("SRP_PRICE", StringType),
      StructField("SVP_LEVEL_A", StringType),
      StructField("SVP_LEVEL_B", StringType),
      StructField("SVP_LEVEL_C", StringType),
      StructField("SVP_LEVEL_D", StringType),
      StructField("SVP_LEVEL_E", StringType),
      StructField("SVP_LEVEL_F", StringType),
      StructField("SVP_LEVEL_G", StringType),
      StructField("SVP_LEVEL_H", StringType),
      StructField("SVP_LEVEL_I", StringType),
      StructField("SVP_LEVEL_J", StringType),
      StructField("SVP_LEVEL_ED", StringType),
      StructField("SVP_LEVEL_GV", StringType),
      StructField("SAP_EXTRCT_DATE", StringType)
    ))

    // Row with the date for the columns specified in the schema to be inserted
    // The record date in the line 55 has to be less than the current date and greater than the date in the line 56.
    val row_insert = new GenericRowWithSchema(Array("odsvt.slt0.l_cntry_price", "D29NMLL", "MDV",
      "USD", "A",
      "2021-06-22",
      "2021-12-31",
      "96250.0000", "96250.0000","96250.0000",
      "0.0000", "84700.0000", "83451.5000",
      "82681.5000", "82197.5000", "81812.5000",
      "76807.5000", "76615.0000", "38500.0000",
      "81812.5000", "2021-09-15-00.17.01.010004"), schema)

    println("Row: " + row_insert)

    // Row with the date for the columns specified in the schema to be inserted
    // The record in the line 69 was changed for the update.
    val row_update = new GenericRowWithSchema(Array("odsvt.slt0.l_cntry_price", "D29NMLL", "MDV",
      "USD", "A",
      "2021-08-22",
      "2021-12-31",
      "66666.1111", "96250.0000","96250.0000",
      "0.0000", "84700.0000", "83451.5000",
      "82681.5000", "82197.5000", "81812.5000",
      "76807.5000", "76615.0000", "38500.0000",
      "81812.5000", "2021-09-30-00.17.01.010004"), schema)

    println("Row: " + row_update)

    /**
      * Execute the test in the file and method that we specified in assert,
      * assert validates the result.
      *
      *@param testName -> string with the name that we are going to give
      *                    to the test.
      */

    test("OnPrem Load Cntry Price Test - Scenario 1 - Insert"){

        val del = delete_record("cntry_price_slt_test", row_insert)

        // Delay to check the DB
        Thread.sleep(20000)

        assert(OnPrem.cntry_price_upsert(row_insert))

        val rs = look_record("cntry_price_slt_test", row_insert)

    }

    // Delay to check the DB
    Thread.sleep(20000)

    test("OnPrem Load Cntry Price Test - Scenario 1 - Update"){

        // Query to find the record inserted previously
        val query_select = (s"""SELECT 1
                      FROM  wwpp1.cntry_price_slt_test
                      WHERE part_num                = '${row_insert.getAs[String]("PART_NUM").replace(" ", "")}'
                      AND   cntry_code              = '${row_insert.getAs[String]("CNTRY_CODE").replace(" ", "")}'
                      AND   iso_currncy_code        = '${row_insert.getAs[String]("ISO_CURRNCY_CODE").replace(" ", "")}'
                      AND   sap_distribtn_chnl_code = '${row_insert.getAs[String]("SAP_DISTRIBTN_CHNL_CODE").replace(" ", "")}'""")

        // Look for the record that was inserted
        val result: Boolean = is_valid(query_select)

        if(result){
          println("Record for the update is in the table.....")

          assert(OnPrem.cntry_price_upsert(row_update))

          // Check if the record is in the table
          val result_check_record: ResultSet = dbselect(query_select)
          println("Record: " + result_check_record)
        }else {
          println("Record is NOT in the table.....")
        }

      println("Exit.....")
    }
*/
}

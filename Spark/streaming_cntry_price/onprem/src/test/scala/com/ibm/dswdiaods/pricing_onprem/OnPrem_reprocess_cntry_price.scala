/*
Name: OnPrem_load_error_cntry_price.scala
Description: Scenario 3 - Logic to test if the data are reprocessed in the error table to be inserted in the cntry_price
table.
  - before
      record exists in error table
      record non-exists in base table
  - after
      record non-exists in error table
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
//import Database.{delete_record, look_record, select => dbselect}

class OnPrem_reprocess_cntry_price extends AnyFunSuite {
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

    // Row with the date for the columns specified in the schema
    // The record in the line 54 was fixed.
    val row_error = new GenericRowWithSchema(Array("odsvt.slt0.l_cntry_price",
      "44T6797",
      "BRA",
      "BRL", "A",
      "2021-06-22",
      "2021-12-31",
      "96250.0000",
      "96250.0000","96250.0000",
      "0.0000", "84700.0000", "83451.5000",
      "82681.5000", "82197.5000", "81812.5000",
      "76807.5000", "76615.0000", "38500.0000",
      "81812.5000", "2021-09-15-00.17.01.01000"), schema)

    println("Row Error: " + row_error)

    // Row with the date for the columns specified in the schema
    // The records in the lines 72 and 73 were fixed.
    val row_errors = new GenericRowWithSchema(Array("odsvt.slt0.l_cntry_price",
      "44T6798",
      "BRA",
      "BRL",
      "A",
      "2021-06-22",
      "2021-12-31",
      "96250.0000",
      "96250.0000","96250.0000",
      "0.0000",
      "84700.0000", "83451.5000",
      "82681.5000", "82197.5000", "81812.5000",
      "76807.5000", "76615.0000", "38500.0000",
      "81812.5000", "2021-09-15-00.17.01.01000"), schema)

    println("Row Errors: " + row_errors)

    // Row with the date for the columns specified in the schema
    // The record in the line 94 was fixed.
    val row_error_case_special = new GenericRowWithSchema(Array("odsvt.slt0.l_cntry_price",
      "44T7131",
      "BRA",
      "BRL", "A",
      "2021-06-22",
      "2021-12-31",
      "96250.0000",
      "96250.0000","96250.0000",
      "0.0000", "84700.0000", "83451.5000",
      "82681.5000", "82197.5000", "81812.5000",
      "76807.5000", "76615.0000", "38500.0000",
      "81812.5000", "2021-09-15-00.17.01.01000"), schema)

    println("Row Error Case Special: " + row_error_case_special)

    /**
      * Execute the test in the file and method that we specified in assert,
      * assert validates the result.
      *
      *@param testName -> string with the name that we are going to give
      *                    to the test.
      */

    test("OnPrem Load Error Cntry Price Test - Scenario 3 - One Error Reprocess"){

      val del = delete_record("cntry_price_slt_test", row_error)

      assert(OnPrem.cntry_price_upsert(row_error))

      val rs = look_record("CNTRY_PRICE_ERR", row_error)
    }

    test("OnPrem Load Error Cntry Price Test - Scenario 3 - Two Errors Reprocess"){

      val del = delete_record("cntry_price_slt_test", row_errors)

      assert(OnPrem.cntry_price_upsert(row_errors))

      val rs = look_record("CNTRY_PRICE_ERR", row_errors)
    }

    test("OnPrem Load Error Cntry Price Test - Scenario 3 - Special Error Reprocess"){

      val del = delete_record("cntry_price_slt_test", row_error_case_special)

      assert(OnPrem.cntry_price_upsert(row_error_case_special))

      val rs = look_record("CNTRY_PRICE_ERR", row_error_case_special)
    }
*/
}

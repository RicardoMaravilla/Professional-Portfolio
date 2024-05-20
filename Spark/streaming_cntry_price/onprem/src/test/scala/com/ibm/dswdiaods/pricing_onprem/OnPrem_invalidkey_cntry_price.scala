/*
Name: OnPrem_load_error_cntry_price.scala
Description: Scenario 4 - Logic to test if the data have a error in the keys.
  - before
      record non-exists in base table
      record non-exists in error table
  - after
      record non-exists in base table
      record exists in error table
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

class OnPrem_invalidkey_cntry_price extends AnyFunSuite {
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

    // Rows with the date for the columns specified in the schema

    // The record have an error in the part num line 55
    val row_error_part_num = new GenericRowWithSchema(Array("odsvt.slt0.l_cntry_price",
      "D123456",
      "MDV",
      "USD", "A",
      "2021-06-22",
      "2021-12-31",
      "96250.0000", "96250.0000","96250.0000",
      "0.0000", "84700.0000", "83451.5000",
      "82681.5000", "82197.5000", "81812.5000",
      "76807.5000", "76615.0000", "38500.0000",
      "81812.5000", "2021-09-15-00.17.01.010004"), schema)

    println("Row Error Part Num: " + row_error_part_num)

    // The record have an error in the cntry code line 70
    val row_error_cntry_code = new GenericRowWithSchema(Array("odsvt.slt0.l_cntry_price", "D27UVLL",
      "MMM",
      "BRL", "A",
      "2021-06-22",
      "2021-12-31",
      "96250.0000", "96250.0000","96250.0000",
      "0.0000", "84700.0000", "83451.5000",
      "82681.5000", "82197.5000", "81812.5000",
      "76807.5000", "76615.0000", "38500.0000",
      "81812.5000", "2021-09-15-00.17.01.010004"), schema)

  println("Row Error Cntry Code: " + row_error_cntry_code)

    // The record have an error in the iso currency code line 84
    val row_error_iso_currncy_code = new GenericRowWithSchema(Array("odsvt.slt0.l_cntry_price", "D061RZX", "BRA",
      "WLW",
      "A",
      "2021-06-22",
      "2021-12-31",
      "96250.0000", "96250.0000","96250.0000",
      "0.0000", "84700.0000", "83451.5000",
      "82681.5000", "82197.5000", "81812.5000",
      "76807.5000", "76615.0000", "38500.0000",
      "81812.5000", "2021-09-15-00.17.01.010004"), schema)

  println("Row Error Iso Currency Code: " + row_error_iso_currncy_code)

    // The record have an error in the sap distribtn chnl code line 99
    val row_error_sap_distribtn_chnl_code = new GenericRowWithSchema(Array("odsvt.slt0.l_cntry_price", "E0KFPLL", "MDV",
      "USD",
      "2",
      "2021-06-22",
      "2021-12-31",
      "96250.0000", "96250.0000","96250.0000",
      "0.0000", "84700.0000", "83451.5000",
      "82681.5000", "82197.5000", "81812.5000",
      "76807.5000", "76615.0000", "38500.0000",
      "81812.5000", "2021-09-15-00.17.01.010004"), schema)

  println("Row Error Sap Distribtn Chnl Code: " + row_error_sap_distribtn_chnl_code)

    /**
      * Execute the test in the file and method that we specified in assert,
      * assert validates the result.
      *
      *@param testName -> string with the name that we are going to give
      *                    to the test.
      */

    test("OnPrem Load Invalid Key Cntry Price Test - Scenario 4 - part num"){

        val del = delete_record("CNTRY_PRICE_ERR", row_error_part_num)

        assert(OnPrem.cntry_price_upsert(row_error_part_num))

        val rs = look_record("CNTRY_PRICE_ERR", row_error_part_num)

    }

    test("OnPrem Load Invalid Key Cntry Price Test - Scenario 4 - cntry code"){
        val del = delete_record("CNTRY_PRICE_ERR", row_error_cntry_code)

        assert(OnPrem.cntry_price_upsert(row_error_cntry_code))

        val rs = look_record("CNTRY_PRICE_ERR", row_error_cntry_code)
    }

    test("OnPrem Load Invalid Key Cntry Price Test - Scenario 4 - iso currncy code"){
        val del = delete_record("CNTRY_PRICE_ERR", row_error_iso_currncy_code)

        assert(OnPrem.cntry_price_upsert(row_error_iso_currncy_code))

        val rs = look_record("CNTRY_PRICE_ERR", row_error_iso_currncy_code)
    }

    test("OnPrem Load Invalid Key Cntry Price Test - Scenario 4 - sap distribtn chnl code"){
        val del = delete_record("CNTRY_PRICE_ERR", row_error_sap_distribtn_chnl_code)

        assert(OnPrem.cntry_price_upsert(row_error_sap_distribtn_chnl_code))

        val rs = look_record("CNTRY_PRICE_ERR", row_error_sap_distribtn_chnl_code)
    }
*/
}

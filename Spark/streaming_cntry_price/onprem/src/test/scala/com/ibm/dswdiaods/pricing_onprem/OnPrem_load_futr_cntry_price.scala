/*
Name: OnPrem_load_futr_cntry_price.scala
Description: Scenario 5 - Logic to test if the date are greater than current date is going to insert in future table
    - insert
    - update
    - before
        record non-exists in base table
        record non-exists in future table
    - after
        record exists in base table
        record exists in future table
Created by: Ricardo Maravilla <ricardo.maravilla@ibm.com>
Created Date: 2021/10/13
Notes:
Modification:
    date        owner       description
*/

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
// import Database.{delete_record, look_record}

class OnPrem_load_futr_cntry_price extends AnyFunSuite {
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
    // The record date in the line 57 has to be greater than the current date and less than the date in the line 58.
    val row = new GenericRowWithSchema(Array("odsvt.slt0.l_cntry_price", "D26VKLL", "BRA",
                                            "BRL", "A",
                                            "2021-11-30",
                                            "2021-12-31",
                                            "96250.0000", "96250.0000","96250.0000",
                                            "0.0000", "84700.0000", "83451.5000",
                                            "82681.5000", "82197.5000", "81812.5000",
                                            "76807.5000", "76615.0000", "38500.0000",
                                            "81812.5000", "2021-09-15-00.17.01.010004"), schema)

    println("Row: " + row)


    /**
    * Execute the test in the file and method that we specified in assert,
    * assert validates the result.
    *
    *@param testName -> string with the name that we are going to give
    *                    to the test.
    */

    test("OnPrem Futr Cntry Price Test - Scenario 5"){

        val del = delete_record("futr_cntry_price_slt_test", row)

        // Delay to check the DB
        Thread.sleep(20000)

        assert(OnPrem.cntry_price_upsert(row))

        val rs = look_record("futr_cntry_price_slt_test", row)
    }
*/
}
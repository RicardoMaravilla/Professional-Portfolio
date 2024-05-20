
SELECT
	TRIM(PTES.PART_NUM) AS "partNumber"
	,CASE WHEN TRIM(COALESCE(PTES.IBM_PROD_ID, '')) <> '' THEN TRIM(COALESCE(PTES.IBM_PROD_ID, '')) ELSE NULL END AS "pid"
	,CASE WHEN TRIM(COALESCE(PTES.IBM_PROD_ID_DSCR, '')) <> '' THEN TRIM(COALESCE(PTES.IBM_PROD_ID_DSCR, '')) ELSE NULL END AS "name"
	,CASE WHEN TRIM(COALESCE(PTES.PART_DSCR_FULL, '')) <> '' THEN TRIM(COALESCE(PTES.PART_DSCR_FULL, '')) ELSE NULL END AS "description"
	,PTES.PROD_PUBLSHG_AUD_MASK AS "audienceMask"
	,CASE WHEN TRIM(COALESCE(PTES.SERVICE_DESCRIPTION_URL, '')) <> '' THEN TRIM(COALESCE(PTES.SERVICE_DESCRIPTION_URL, '')) ELSE NULL END AS "serviceDescription"
	,CASE WHEN TRIM(COALESCE(PTES.SAAS_SERVICE_PROVIDER_CODE, '')) <> '' THEN TRIM(COALESCE(PTES.SAAS_SERVICE_PROVIDER_CODE, '')) ELSE NULL END AS "saasServiceProviderCode"
	,CASE WHEN TRIM(COALESCE(URL.SW_URL,'')) <> '' AND PTES.REVN_STREAM_CODE = 'RSWMWS' THEN 1 ELSE 0 END AS "licKeyFlag"
	,CASE WHEN SRSCL.ODS_MTHLY_LIC_FLAG = 1 THEN 1 ELSE 0 END AS "monthlyLicenseFlag"
	,CASE WHEN TRIM(COALESCE(URL.SW_URL,'')) <> '' AND PTES.REVN_STREAM_CODE = 'RSWMWS' THEN TRIM(COALESCE(URL.SW_URL, '')) ELSE NULL END AS "swURL"
	,CASE WHEN TRIM(COALESCE(PTES.CU_DSCR, '')) <> '' THEN TRIM(COALESCE(PTES.CU_DSCR, '')) ELSE NULL END AS "chargeMetric"
	,TRIM(COALESCE(PTES.PRICNG_DLT_FLAG, '')) AS "status"
	,CASE WHEN TRIM(COALESCE(PTES.REVN_STREAM_CODE_DSCR, '')) <> '' THEN TRIM(COALESCE(PTES.REVN_STREAM_CODE_DSCR, '')) ELSE NULL END AS "dswPartType"
	,NULL AS "licenseTerm"
	,NULL AS "licenseType"
	,CASE WHEN TRIM(COALESCE(PTES.REVN_STREAM_CODE, '')) <> '' THEN TRIM(COALESCE(PTES.REVN_STREAM_CODE, '')) ELSE NULL END AS "revenueStreamCode"
	,CASE WHEN TRIM(COALESCE(PTES.REVN_STREAM_CODE_DSCR, '')) <> '' THEN TRIM(COALESCE(PTES.REVN_STREAM_CODE_DSCR, '')) ELSE NULL END AS "revenueStreamCodeDscr"
	,CASE WHEN TRIM(COALESCE(PD.PART_DSCR_LONG, '')) <> '' THEN TRIM(COALESCE(PD.PART_DSCR_LONG, '')) ELSE NULL END AS "partShortDscr"
	,NULL AS "slaPart"
	,CASE WHEN TRIM(COALESCE(PTES.TERMS_OF_USE_NAME, '')) <> '' THEN TRIM(COALESCE(PTES.TERMS_OF_USE_NAME, '')) ELSE NULL END AS "touName"
	,CASE WHEN TRIM(COALESCE(PTES.TERMS_OF_USE_URL, '')) <> '' THEN TRIM(COALESCE(PTES.TERMS_OF_USE_URL, '')) ELSE NULL END AS "touURL"
	,CASE WHEN TRIM(COALESCE(PD.SAP_MATL_TYPE_CODE, '')) <> '' THEN TRIM(COALESCE(PD.SAP_MATL_TYPE_CODE, '')) ELSE NULL END AS "sapMatlTypeCode"
	,CASE WHEN TRIM(COALESCE(PD.SAP_MATL_TYPE_CODE_DSCR, '')) <> '' THEN TRIM(COALESCE(PD.SAP_MATL_TYPE_CODE_DSCR, '')) ELSE NULL END AS "sapMatlTypeCodeDscr"
	,CASE WHEN TRIM(COALESCE(PTES.SAAS_RENWL_MDL_CODE, '')) <> '' THEN TRIM(COALESCE(PTES.SAAS_RENWL_MDL_CODE, '')) ELSE NULL END AS "saasRenewal"
	,CASE WHEN TRIM(COALESCE(PTES.SAAS_RENWL_MDL_CODE_DSCR, '')) <> '' THEN TRIM(COALESCE(PTES.SAAS_RENWL_MDL_CODE_DSCR, '')) ELSE NULL END AS "saasRenwlMdlCodeDscr"
	,TRIM(COALESCE(PTES.SW_SBSCRPTN_ID, '')) AS "subscriptionId"
	,PD.CHRGBL_CMPNT AS "chargeableComponent"
	,CASE WHEN TRIM(COALESCE(PD.CHRGBL_CMPNT_DSCR, '')) <> '' THEN TRIM(COALESCE(PD.CHRGBL_CMPNT_DSCR, '')) ELSE NULL END AS "chargeableComponentDscr"
	,'SWG_SAAS' AS "itemType"
	,CASE WHEN TRIM(COALESCE(PD.UT_LEVEL_CODE_30, '')) <> '' THEN TRIM(COALESCE(PD.UT_LEVEL_CODE_30, '')) ELSE NULL END AS "utl30Code"
	,PTES.PRICE_START_DATE AS "priceStartDate"
FROM
	FFXT1.PRICING_TIERD_ECOM_SCW PTES
	JOIN RSHR2.PROD_DIMNSN PD ON PTES.PART_NUM = PD.PART_NUM
	JOIN SHAR2.REVN_STREAM_CODE_LOOKUP SRSCL ON PTES.REVN_STREAM_CODE = SRSCL.REVN_STREAM_CODE
	LEFT JOIN WWPP2.URL_INFO URL ON PTES.PART_NUM = URL.PART_NUM
WHERE
	PTES.CNTRY_CODE = 'USA'
	AND PTES.PROD_PUBLSHG_AUD_MASK BETWEEN 1 AND 15
	AND PTES.IBM_PROD_ID_DSCR <> ''
	--AND PTES.ODS_MOD_DATE >= ?
	AND PTES.PART_NUM IN [?]
UNION ALL
SELECT
		TRIM(PTES.PART_NUM) AS "partNumber"
	,CASE WHEN TRIM(COALESCE(PTES.IBM_PROD_ID, '')) <> '' THEN TRIM(COALESCE(PTES.IBM_PROD_ID, '')) ELSE NULL END AS "pid"
	,CASE WHEN TRIM(COALESCE(PTES.IBM_PROD_ID_DSCR, '')) <> '' THEN TRIM(COALESCE(PTES.IBM_PROD_ID_DSCR, '')) ELSE NULL END AS "name"
	,CASE WHEN TRIM(COALESCE(PTES.PART_DSCR_FULL, '')) <> '' THEN TRIM(COALESCE(PTES.PART_DSCR_FULL, '')) ELSE NULL END AS "description"
	,PTES.PROD_PUBLSHG_AUD_MASK AS "audienceMask"
	,CASE WHEN TRIM(COALESCE(PTES.SERVICE_DESCRIPTION_URL, '')) <> '' THEN TRIM(COALESCE(PTES.SERVICE_DESCRIPTION_URL, '')) ELSE NULL END AS "serviceDescription"
	,CASE WHEN TRIM(COALESCE(PTES.SAAS_SERVICE_PROVIDER_CODE, '')) <> '' THEN TRIM(COALESCE(PTES.SAAS_SERVICE_PROVIDER_CODE, '')) ELSE NULL END AS "saasServiceProviderCode"
	,CASE WHEN TRIM(COALESCE(URL.SW_URL,'')) <> '' AND PTES.REVN_STREAM_CODE = 'RSWMWS' THEN 1 ELSE 0 END AS "licKeyFlag"
	,CASE WHEN SRSCL.ODS_MTHLY_LIC_FLAG = 1 THEN 1 ELSE 0 END AS "monthlyLicenseFlag"
	,CASE WHEN TRIM(COALESCE(URL.SW_URL,'')) <> '' AND PTES.REVN_STREAM_CODE = 'RSWMWS' THEN TRIM(COALESCE(URL.SW_URL, '')) ELSE NULL END AS "swURL"
	,CASE WHEN TRIM(COALESCE(PTES.CU_DSCR, '')) <> '' THEN TRIM(COALESCE(PTES.CU_DSCR, '')) ELSE NULL END as "chargeMetric"
	,PTES.PRICNG_DLT_FLAG AS "status"
	,CASE WHEN TRIM(COALESCE(PTES.REVN_STREAM_CODE_DSCR, '')) <> '' THEN TRIM(COALESCE(PTES.REVN_STREAM_CODE_DSCR, '')) ELSE NULL END AS "dswPartType"
	,NULL AS "licenseTerm"
	,NULL AS "licenseType"
	,CASE WHEN TRIM(COALESCE(PTES.REVN_STREAM_CODE, '')) <> '' THEN TRIM(COALESCE(PTES.REVN_STREAM_CODE, '')) ELSE NULL END AS "revenueStreamCode"
	,CASE WHEN TRIM(COALESCE(PTES.REVN_STREAM_CODE_DSCR, '')) <> '' THEN TRIM(COALESCE(PTES.REVN_STREAM_CODE_DSCR, '')) ELSE NULL END AS "revenueStreamCodeDscr"
	,CASE WHEN TRIM(COALESCE(PD.PART_DSCR_LONG, '')) <> '' THEN TRIM(COALESCE(PD.PART_DSCR_LONG, '')) ELSE NULL END AS "partShortDscr"
	,NULL AS "slaPart"
	,CASE WHEN TRIM(COALESCE(PTES.TERMS_OF_USE_NAME, '')) <> '' THEN TRIM(COALESCE(PTES.TERMS_OF_USE_NAME, '')) ELSE NULL END AS "touName"
	,CASE WHEN TRIM(COALESCE(PTES.TERMS_OF_USE_URL, '')) <> '' THEN TRIM(COALESCE(PTES.TERMS_OF_USE_URL, '')) ELSE NULL END AS "touURL"
	,CASE WHEN TRIM(COALESCE(PD.SAP_MATL_TYPE_CODE, '')) <> '' THEN TRIM(COALESCE(PD.SAP_MATL_TYPE_CODE, '')) ELSE NULL END AS "sapMatlTypeCode"
	,CASE WHEN TRIM(COALESCE(PD.SAP_MATL_TYPE_CODE_DSCR, '')) <> '' THEN TRIM(COALESCE(PD.SAP_MATL_TYPE_CODE_DSCR, '')) ELSE NULL END AS "sapMatlTypeCodeDscr"
	,CASE WHEN TRIM(COALESCE(PTES.SAAS_RENWL_MDL_CODE, '')) <> '' THEN TRIM(COALESCE(PTES.SAAS_RENWL_MDL_CODE, '')) ELSE NULL END AS "saasRenewal"
	,CASE WHEN TRIM(COALESCE(PTES.SAAS_RENWL_MDL_CODE_DSCR, '')) <> '' THEN TRIM(COALESCE(PTES.SAAS_RENWL_MDL_CODE_DSCR, '')) ELSE NULL END AS "saasRenwlMdlCodeDscr"
	,TRIM(COALESCE(PTES.SW_SBSCRPTN_ID, '')) AS "subscriptionId"
	,PD.CHRGBL_CMPNT AS "chargeableComponent"
	,CASE WHEN TRIM(COALESCE(PD.CHRGBL_CMPNT_DSCR, '')) <> '' THEN TRIM(COALESCE(PD.CHRGBL_CMPNT_DSCR, '')) ELSE NULL END AS "chargeableComponentDscr"
	,'SWG_SAAS' AS "itemType"
	,CASE WHEN TRIM(COALESCE(PD.UT_LEVEL_CODE_30, '')) <> '' THEN TRIM(COALESCE(PD.UT_LEVEL_CODE_30, '')) ELSE NULL END AS "utl30Code"
	,PTES.PRICE_START_DATE AS "priceStartDate"
FROM
	FFXT1.PRICING_TIERD_ECOM_SCW PTES
	JOIN RSHR2.PROD_DIMNSN PD ON PTES.PART_NUM = PD.PART_NUM
	JOIN SHAR2.REVN_STREAM_CODE_LOOKUP SRSCL ON PTES.REVN_STREAM_CODE = SRSCL.REVN_STREAM_CODE
	LEFT JOIN WWPP2.URL_INFO URL ON PTES.PART_NUM = URL.PART_NUM
WHERE
	PTES.CNTRY_CODE <> 'USA'
	AND PTES.PROD_PUBLSHG_AUD_MASK BETWEEN 1 AND 15
	AND PTES.IBM_PROD_ID_DSCR <> ''
	--AND PTES.ODS_MOD_DATE >= ?
	AND PTES.PART_NUM IN [?]
	AND NOT EXISTS (SELECT 1 FROM FFXT1.PRICING_TIERD_ECOM_SCW PTES2 WHERE PTES.PART_NUM = PTES2.PART_NUM AND CNTRY_CODE = 'USA')
GROUP BY
	TRIM(PTES.PART_NUM)
	,CASE WHEN TRIM(COALESCE(PTES.IBM_PROD_ID, '')) <> '' THEN TRIM(COALESCE(PTES.IBM_PROD_ID, '')) ELSE NULL END
	,CASE WHEN TRIM(COALESCE(PTES.IBM_PROD_ID_DSCR, '')) <> '' THEN TRIM(COALESCE(PTES.IBM_PROD_ID_DSCR, '')) ELSE NULL END
	,CASE WHEN TRIM(COALESCE(PTES.PART_DSCR_FULL, '')) <> '' THEN TRIM(COALESCE(PTES.PART_DSCR_FULL, '')) ELSE NULL END
	,PTES.PROD_PUBLSHG_AUD_MASK
	,CASE WHEN TRIM(COALESCE(PTES.SERVICE_DESCRIPTION_URL, '')) <> '' THEN TRIM(COALESCE(PTES.SERVICE_DESCRIPTION_URL, '')) ELSE NULL END
	,CASE WHEN TRIM(COALESCE(PTES.SAAS_SERVICE_PROVIDER_CODE, '')) <> '' THEN TRIM(COALESCE(PTES.SAAS_SERVICE_PROVIDER_CODE, '')) ELSE NULL END
	,CASE WHEN TRIM(COALESCE(URL.SW_URL,'')) <> '' AND PTES.REVN_STREAM_CODE = 'RSWMWS' THEN 1 ELSE 0 END
	,CASE WHEN SRSCL.ODS_MTHLY_LIC_FLAG = 1 THEN 1 ELSE 0 END
	,CASE WHEN TRIM(COALESCE(URL.SW_URL,'')) <> '' AND PTES.REVN_STREAM_CODE = 'RSWMWS' THEN TRIM(COALESCE(URL.SW_URL, '')) ELSE NULL END
	,CASE WHEN TRIM(COALESCE(PTES.CU_DSCR, '')) <> '' THEN TRIM(COALESCE(PTES.CU_DSCR, '')) ELSE NULL END
	,PTES.PRICNG_DLT_FLAG
	,CASE WHEN TRIM(COALESCE(PTES.REVN_STREAM_CODE_DSCR, '')) <> '' THEN TRIM(COALESCE(PTES.REVN_STREAM_CODE_DSCR, '')) ELSE NULL END
	,14
	,15
	,CASE WHEN TRIM(COALESCE(PTES.REVN_STREAM_CODE, '')) <> '' THEN TRIM(COALESCE(PTES.REVN_STREAM_CODE, '')) ELSE NULL END
	,CASE WHEN TRIM(COALESCE(PTES.REVN_STREAM_CODE_DSCR, '')) <> '' THEN TRIM(COALESCE(PTES.REVN_STREAM_CODE_DSCR, '')) ELSE NULL END
	,CASE WHEN TRIM(COALESCE(PD.PART_DSCR_LONG, '')) <> '' THEN TRIM(COALESCE(PD.PART_DSCR_LONG, '')) ELSE NULL END
	,19
	,CASE WHEN TRIM(COALESCE(PTES.TERMS_OF_USE_NAME, '')) <> '' THEN TRIM(COALESCE(PTES.TERMS_OF_USE_NAME, '')) ELSE NULL END
	,CASE WHEN TRIM(COALESCE(PTES.TERMS_OF_USE_URL, '')) <> '' THEN TRIM(COALESCE(PTES.TERMS_OF_USE_URL, '')) ELSE NULL END
	,CASE WHEN TRIM(COALESCE(PD.SAP_MATL_TYPE_CODE, '')) <> '' THEN TRIM(COALESCE(PD.SAP_MATL_TYPE_CODE, '')) ELSE NULL END
	,CASE WHEN TRIM(COALESCE(PD.SAP_MATL_TYPE_CODE_DSCR, '')) <> '' THEN TRIM(COALESCE(PD.SAP_MATL_TYPE_CODE_DSCR, '')) ELSE NULL END
	,CASE WHEN TRIM(COALESCE(PTES.SAAS_RENWL_MDL_CODE, '')) <> '' THEN TRIM(COALESCE(PTES.SAAS_RENWL_MDL_CODE, '')) ELSE NULL END
	,CASE WHEN TRIM(COALESCE(PTES.SAAS_RENWL_MDL_CODE_DSCR, '')) <> '' THEN TRIM(COALESCE(PTES.SAAS_RENWL_MDL_CODE_DSCR, '')) ELSE NULL END
	,TRIM(COALESCE(PTES.SW_SBSCRPTN_ID, ''))
	,PD.CHRGBL_CMPNT
	,CASE WHEN TRIM(COALESCE(PD.CHRGBL_CMPNT_DSCR, '')) <> '' THEN TRIM(COALESCE(PD.CHRGBL_CMPNT_DSCR, '')) ELSE NULL END
	,29
	,CASE WHEN TRIM(COALESCE(PD.UT_LEVEL_CODE_30, '')) <> '' THEN TRIM(COALESCE(PD.UT_LEVEL_CODE_30, '')) ELSE NULL END
	,PTES.PRICE_START_DATE
UNION ALL
SELECT 
  TRIM(PTES.PART_NUM) AS "partNumber"
  ,CASE WHEN TRIM(COALESCE(PTES.IBM_PROD_ID, '')) <> '' THEN TRIM(COALESCE(PTES.IBM_PROD_ID, '')) ELSE NULL END AS "pid"
  ,CASE WHEN TRIM(COALESCE(PTES.IBM_PROD_ID_DSCR, '')) <> '' THEN TRIM(COALESCE(PTES.IBM_PROD_ID_DSCR, '')) ELSE NULL END AS "name"
  ,TRIM(COALESCE(PTES.PART_DSCR_FULL, '')) AS "description"
  ,PTES.PROD_PUBLSHG_AUD_MASK AS "audienceMask"
  ,NULL as "serviceDescription"
  ,NULL as "saasServiceProviderCode"
  ,PTES.LIC_KEY_FLAG AS "licKeyFlag"
  ,CASE WHEN SRSCL.ODS_MTHLY_LIC_FLAG = 1 THEN 1 ELSE 0 END AS "monthlyLicenseFlag"
  ,CASE WHEN TRIM(COALESCE(URL.SW_URL,'')) <> '' THEN TRIM(COALESCE(URL.SW_URL,'')) ELSE '' END AS "swURL"
  ,'Not Applicable' as "chargeMetric"
  ,TRIM(COALESCE(PTES.PRICNG_DLT_FLAG, '')) AS "status"
  ,CASE WHEN TRIM(COALESCE(PTES.REVN_STREAM_CODE_DSCR, '')) <> '' THEN TRIM(COALESCE(PTES.REVN_STREAM_CODE_DSCR, '')) ELSE NULL END AS "dswPartType"
  ,CASE WHEN TRIM(COALESCE(PTES.REVN_STREAM_TERM_DSCR, '')) = '1 Year' THEN 'Annual' ELSE TRIM(COALESCE(PTES.REVN_STREAM_TERM_DSCR, '')) END AS "licenseTerm"
  ,TRIM(COALESCE(PTES.CU_DSCR, '')) AS "licenseType"
  ,CASE WHEN TRIM(COALESCE(PTES.REVN_STREAM_CODE, '')) <> '' THEN TRIM(COALESCE(PTES.REVN_STREAM_CODE, '')) ELSE NULL END AS "revenueStreamCode"
  ,CASE WHEN TRIM(COALESCE(PTES.REVN_STREAM_CODE_DSCR, '')) <> '' THEN TRIM(COALESCE(PTES.REVN_STREAM_CODE_DSCR, '')) ELSE NULL END AS "revenueStreamCodeDscr"
  ,CASE WHEN TRIM(COALESCE(PD.PART_DSCR_LONG, '')) <> '' THEN TRIM(COALESCE(PD.PART_DSCR_LONG, '')) ELSE NULL END AS "partShortDscr"
  ,NULL AS "slaPart"
  ,NULL AS "touName"
  ,NULL AS "touURL"
  ,CASE WHEN TRIM(COALESCE(PD.SAP_MATL_TYPE_CODE, '')) <> '' THEN TRIM(COALESCE(PD.SAP_MATL_TYPE_CODE, '')) ELSE NULL END AS "sapMatlTypeCode"
  ,CASE WHEN TRIM(COALESCE(PD.SAP_MATL_TYPE_CODE_DSCR, '')) <> '' THEN TRIM(COALESCE(PD.SAP_MATL_TYPE_CODE_DSCR, '')) ELSE NULL END AS "sapMatlTypeCodeDscr"
  ,NULL AS "saasRenewal"
  ,NULL AS "saasRenwlMdlCodeDscr"
  ,TRIM(COALESCE(PTES.SW_SBSCRPTN_ID, '')) AS "subscriptionId"
  ,PD.CHRGBL_CMPNT AS "chargeableComponent"
  ,CASE WHEN TRIM(COALESCE(PD.CHRGBL_CMPNT_DSCR, '')) <> '' THEN TRIM(COALESCE(PD.CHRGBL_CMPNT_DSCR, '')) ELSE NULL END AS "chargeableComponentDscr"
  ,'SWG_ONPREM' AS "itemType"
  ,CASE WHEN TRIM(COALESCE(PD.UT_LEVEL_CODE_30, '')) <> '' THEN TRIM(COALESCE(PD.UT_LEVEL_CODE_30, '')) ELSE NULL END AS "utl30Code"
	,NULL AS "priceStartDate"
FROM 
  FFXT1.SW_PROD_SCW PTES
	JOIN SHAR2.REVN_STREAM_CODE_LOOKUP SRSCL ON PTES.REVN_STREAM_CODE = SRSCL.REVN_STREAM_CODE
	LEFT JOIN WWPP2.URL_INFO URL ON PTES.PART_NUM = URL.PART_NUM
	JOIN RSHR2.PROD_DIMNSN PD ON PTES.PART_NUM = PD.PART_NUM
WHERE 	 
	PTES.CNTRY_CODE = 'USA'
	AND PTES.PROD_PUBLSHG_AUD_MASK BETWEEN 1 AND 15
	AND PTES.IBM_PROD_ID_DSCR <> ''
	AND PD.SAAS_FLAG='N'
	--AND PTES.ODS_MOD_DATE >= ?
	AND PTES.PART_NUM IN [?]
USE bim_report
GO

/* --------------------------------------------------------------------------------- *

*/

-- DROP TABLE stg.exchange_rate
SELECT * 
FROM stg.exchange_rate
ORDER BY REPORT_DATE, FROM_CURRENCY_CODE
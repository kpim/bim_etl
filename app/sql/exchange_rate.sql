USE bim_report
GO

/* --------------------------------------------------------------------------------- *

*/
CREATE TABLE stg.exchange_rate (
    REPORT_DATE DATE,
    FROM_CURRENCY_CODE NVARCHAR(10),
    TO_CURRENCY_CODE NVARCHAR(10),

    BUY_CASH DECIMAL(18, 4),
    BUY_TRANSFER DECIMAL(18, 4),
    SELL_CASH DECIMAL(18, 4),
    SELL_TRANSFER DECIMAL(18, 4),

    CREATED_AT DATETIME,
    MODIFIED_AT DATETIME
)

-- DROP TABLE stg.exchange_rate
SELECT * 
FROM stg.exchange_rate
ORDER BY REPORT_DATE, FROM_CURRENCY_CODE
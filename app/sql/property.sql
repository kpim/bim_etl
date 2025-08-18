USE bim_report
GO

/* --------------------------------------------------------------------------------- *

*/
-- DROP TABLE dbo.property
CREATE TABLE dbo.property (
    ID INT NOT NULL PRIMARY KEY,
    CODE NVARCHAR(20) UNIQUE,
    NAME NVARCHAR(100) UNIQUE,
    LOCATION NVARCHAR(50),
    TYPE NVARCHAR(20),
    OPERATOR NVARCHAR(50),
    SYSTEM_USED NVARCHAR(20),
    DATA_FORM NVARCHAR(20),
    ORDER_INDEX INT
)

INSERT INTO dbo.property (ID, CODE, NAME, LOCATION, TYPE, OPERATOR, SYSTEM_USED, DATA_FORM, ORDER_INDEX) VALUES
(665, 'HLP', 'Ha Long Plaza Hotel', 'Ha Long', 'Hotel', 'BIM', 'SMILE', 'SMILE HL', 1),
(945, 'SRC', 'Syrena Cruises', 'Ha Long', 'Cruise', 'BIM', 'SMILE', 'SMILE HL', 2),
(206, 'FSH', 'Fraser Suites Hanoi', 'Ha Noi', 'Hotel', 'Fraser Hospitality', NULL, NULL, 3),
(393, 'PHPQ', 'Park Hyatt Phu Quoc Residences', 'Phu Quoc', 'Hotel', 'Hyatt', 'Opera', 'Opera', 4),
(200, 'ICHL', 'InterContinental Ha Long Bay Resort', 'Ha Long', 'Hotel', 'IHG', 'Opera', 'Opera', 5),
(209, 'ICPQ', 'InterContinental Phu Quoc Long Beach Resort', 'Phu Quoc', 'Hotel', 'IHG', 'Opera', 'Opera', 6),
(635, 'REPQ', 'Regent Phu Quoc', 'Phu Quoc', 'Hotel', 'IHG', 'Opera', 'Opera', 7),
(526, 'CPV', 'Crowne Plaza Vientiane', 'Vientiane', 'Hotel', 'IHG', 'Opera', 'Opera', 8),
(595, 'HIVT', 'Holiday Inn & Suites Vientiane', 'Vientiane', 'Hotel', 'IHG', 'Opera', 'Opera', 9),
(289, 'SCSRHL', 'Sailing Club Signature Resort Ha Long Bay', 'Ha Long', 'Hotel', 'Sailing Club', 'SMILE', 'SMILE HL', 10),
(371, 'SCSRPQ', 'Sailing Club Signature Resort Phu Quoc', 'Phu Quoc', 'Hotel', 'Sailing Club', 'SMILE', 'SMILE PQ', 11),
(702, 'SBHPQ', 'Soul Boutique Hotel Phu Quoc', 'Phu Quoc', 'Hotel', 'Sailing Club', 'SMILE', 'SMILE PQ', 12),
(893, 'CMH', 'Citadines Marina Ha Long', 'Ha Long', 'Hotel', 'The Ascott', NULL, NULL, 13)

SELECT * 
FROM dbo.property
ORDER BY ORDER_INDEX

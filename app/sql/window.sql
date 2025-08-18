USE bim_report
GO

/* --------------------------------------------------------------------------------- *

*/
-- DROP TABLE dbo.window
CREATE TABLE dbo.window (
    ID INT NOT NULL PRIMARY KEY,
    WINDOW NVARCHAR(20) UNIQUE,
    [FROM] INT,
    [TO] INT
)

INSERT INTO dbo.window VALUES
(1, '0-3 DAYS', 0, 3),
(2, '<1 WEEK', 4, 7),
(3, '2 WEEKS', 8, 14),
(4, '3 WEEKS', 15, 21),
(5, '4 WEEKS', 22, 30),
(6, '1-2 MONTHS', 31, 60),
(7, '2-3 MONTHS', 61, 90),
(8, '3-6 MONTHS', 91, 180),
(9, '6-12 MONTHS', 181, 365),
(10, '>12 MONTHS', 366, NULL)

SELECT * FROM dbo.window
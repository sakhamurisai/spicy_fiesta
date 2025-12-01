DECLARE @d DATE  = '1980-01-01'
WHILE @d <= '2025-01-01'
BEGIN
    INSERT INTO [dbo].[calendar] ([calendar_GUID],[financial_date],[financial_year],[quarter],
    [week_number],[week_start_date],[week_end_date],[financial_month_start_date],
    [financial_month_end_date])
    SELECT 
        FORMAT(@d,'YYYYMMDD') AS calendar_guid,
        @d AS date,
        YEAR(@d) AS year,
        CASE 
            WHEN MONTH(@d) IN (1,2,3) THEN 'Q1'
            WHEN MONTH(@d) IN (4,5,6) THEN 'Q2'
            WHEN MONTH(@d) IN (7,8,9) THEN 'Q3'
        ELSE
            'Q4' 
        END AS quater,
        DATEPART(WEEK,@d) AS week_number,
        DATEADD(WEEK, DATEDIFF(WEEK, 0, @d), 0) AS week_start,
        DATEADD(DAY, 6, DATEADD(WEEK, DATEDIFF(WEEK, 0, @d), 0)) AS week_end,
        DATEFROMPARTS(YEAR(@d), MONTH(@d), 1) AS month_start,
        EOMONTH(@d) AS month_end
    SET @d = DATEADD(DAY,1,@d);
END
GO;

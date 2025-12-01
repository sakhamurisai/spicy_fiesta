# 00_calendar.py
from utils import get_spark, write_parquet
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def main(output_root="./output_parquet"):
    spark = get_spark("calendar")
    start = "1980-01-01"
    end = "2080-12-31"

    df = spark.sql(f"SELECT sequence(to_date('{start}'), to_date('{end}'), interval 1 day) as dseq")
    cal = df.select(F.explode(F.col("dseq")).alias("CalendarDate"))

    cal = cal.withColumn("Year", F.year("CalendarDate")) \
             .withColumn("Quarter", F.quarter("CalendarDate")) \
             .withColumn("Month", F.month("CalendarDate")) \
             .withColumn("MonthName", F.date_format("CalendarDate","MMMM")) \
             .withColumn("Day", F.dayofmonth("CalendarDate")) \
             .withColumn("DayOfWeek", F.date_format("CalendarDate","u").cast("int")) \
             .withColumn("DayName", F.date_format("CalendarDate","EEEE")) \
             .withColumn("WeekOfYear", F.weekofyear("CalendarDate")) \
             .withColumn("ISOYear", F.year("CalendarDate")) \
             .withColumn("ISOWeek", F.weekofyear("CalendarDate")) \
             .withColumn("IsWeekend", (F.expr("date_format(CalendarDate,'u') in ('6','7')")).cast("int")) \
             .withColumn("IsHoliday", F.lit(0)) \
             .withColumn("HolidayName", F.lit(None).cast("string")) \
             .withColumn("IsMonthStart", (F.dayofmonth("CalendarDate")==1).cast("int")) \
             .withColumn("IsMonthEnd", (F.last_day("CalendarDate")==F.col("CalendarDate")).cast("int")) \
             .withColumn("IsQuarterStart", F.lit(0)) \
             .withColumn("IsQuarterEnd", F.lit(0)) \
             .withColumn("IsYearStart", ((F.dayofmonth("CalendarDate")==1) & (F.month("CalendarDate")==1)).cast("int")) \
             .withColumn("IsYearEnd", ((F.dayofmonth("CalendarDate")==31) & (F.month("CalendarDate")==12)).cast("int")) \
             .withColumn("FiscalYear", F.year(F.add_months("CalendarDate", 3))) \
             .withColumn("FiscalMonth", F.month(F.add_months("CalendarDate", 3))) \
             .withColumn("FiscalQuarter", F.quarter(F.add_months("CalendarDate", 3))) \
             .withColumn("DayOfYear", F.dayofyear("CalendarDate")) \
             .withColumn("CreatedDate", F.current_timestamp())

    # add CalendarID using row_number ordering

    w = Window.orderBy(F.col("CalendarDate"))
    cal = cal.withColumn("CalendarID", row_number().over(w))
    cols = ["CalendarID","CalendarDate","Year","Quarter","Month","MonthName","Day","DayOfWeek","DayName",
            "WeekOfYear","ISOYear","ISOWeek","IsWeekend","IsHoliday","HolidayName","IsMonthStart","IsMonthEnd",
            "IsQuarterStart","IsQuarterEnd","IsYearStart","IsYearEnd","FiscalYear","FiscalMonth","FiscalQuarter",
            "DayOfYear","CreatedDate"]
    cal = cal.select(*cols)
    write_parquet(cal, f"{output_root}/dim.Calendar", partitionBy="Year")
    spark.stop()

if __name__=="__main__":
    import sys
    out = sys.argv[1] if len(sys.argv)>1 else "./output_parquet"
    main(out)

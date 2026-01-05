from pyspark.sql.functions import * 
from pyspark.sql.types import *
from azure_config import *
from utils import *

spark = get_spark("calendar_write")
configure_azure_blob_storage(spark)
azure_read_dim_calender = get_azure_blob_path("dim")
df = spark.read.format("parquet").option("recursiveFileLookup","true").load(azure_read_dim_calender)
write_df = df.select(['CalendarID', 'CalendarDate', 'Quarter', 'Month', 'MonthName', 'Day', 'DayOfWeek', 'DayName', 'WeekOfYear', 'ISOYear', 'ISOWeek', 'IsWeekend', 'IsHoliday', 'HolidayName', 'IsMonthStart', 'IsMonthEnd', 'IsQuarterStart', 'IsQuarterEnd', 'IsYearStart', 'IsYearEnd', 'FiscalYear', 'FiscalMonth', 'FiscalQuarter', 'DayOfYear', 'CreatedDate'])
write_to_tables(df= write_df, schema_name= 'dim', table_name= 'Calendar', spark=spark)
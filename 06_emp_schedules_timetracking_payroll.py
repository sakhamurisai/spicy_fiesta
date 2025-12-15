"""Employee schedules, time tracking, and payroll generation."""
from utils import get_spark, write_parquet
from azure_config import *
import pyspark.sql.functions as F
import logging

logger = logging.getLogger(__name__)


def main():
    """Generate schedules, time tracking, and payroll tables."""
    spark = get_spark("schedules")
    configure_azure_blob_storage(spark)
    
    try:
        azure_emp_path = get_azure_blob_path("emp")
        azure_dim_path = get_azure_blob_path("dim")
        
        emp_df = spark.read.parquet(azure_emp_path).select("EmployeeID", "PrimaryLocationID")
        cal_df = spark.read.parquet(azure_dim_path).filter(F.col("Year") >= 2020).select("CalendarID").limit(30)
        
        cal_list = [r.CalendarID for r in cal_df.collect()]
        rows = []
        for emp in emp_df.collect():
            for cal_id in cal_list:
                rows.append((emp.EmployeeID, emp.PrimaryLocationID, cal_id, "09:00:00", "17:00:00", "Day", 30, 1, None))
        
        sched_df = spark.createDataFrame(rows, ["EmployeeID", "LocationID", "ShiftDateID", "StartTime",
                                                "EndTime", "ShiftType", "BreakMinutes", "IsApproved", "ApprovedBy"])
        sched_df = sched_df.withColumn("ScheduleID", F.monotonically_increasing_id() + 1).withColumn("CreatedDate", F.current_timestamp())
        write_parquet(sched_df, azure_emp_path)
        
        tt_rows = [(s.ScheduleID, s.EmployeeID, s.LocationID, s.ShiftDateID,
                   "2022-01-01 09:05:00", "2022-01-01 17:02:00", None, None)
                  for s in sched_df.collect()]
        tt_df = spark.createDataFrame(tt_rows, ["ScheduleID", "EmployeeID", "LocationID", "ClockInDateID",
                                                "ClockInTime", "ClockOutTime", "BreakStartTime", "BreakEndTime"])
        tt_df = tt_df.withColumn("TimeTrackingID", F.monotonically_increasing_id() + 1).withColumn("CreatedDate", F.current_timestamp())
        write_parquet(tt_df, azure_emp_path)
        
        payroll_df = tt_df.groupBy("EmployeeID").agg(
            F.sum(F.expr("(unix_timestamp(ClockOutTime) - unix_timestamp(ClockInTime))/3600.0")).alias("HoursWorked")
        )
        payroll_df = (payroll_df
            .withColumn("PayrollID", F.monotonically_increasing_id() + 1)
            .withColumn("GrossPay", F.round(F.col("HoursWorked") * 15.0, 2))
            .withColumn("PayrollDateID", F.lit(None).cast("int"))
            .withColumn("CreatedDate", F.current_timestamp())
        )
        write_parquet(payroll_df, azure_emp_path)
        
        logger.info("Schedules, time tracking, and payroll created")
        
    except Exception as e:
        logger.error(f"Error generating schedules: {str(e)}")
        raise
if __name__ == "__main__":
    main()
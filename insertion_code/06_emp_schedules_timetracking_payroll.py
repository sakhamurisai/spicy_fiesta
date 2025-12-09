"""Employee schedules, time tracking, and payroll generation."""
import sys
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

try:
    from utils import get_spark, write_parquet
except ImportError:
    sys.path.insert(0, '.')
    from utils import get_spark, write_parquet

DEFAULT_OUTPUT_ROOT = "./output_parquet"

def main(output_root: str = DEFAULT_OUTPUT_ROOT) -> None:
    """Generate schedules, time tracking, and payroll tables."""
    spark = None
    try:
        spark = get_spark("schedules")
        emp_df = spark.read.parquet(f"{output_root}/emp.Employees").select("EmployeeID", "PrimaryLocationID")
        cal_df = spark.read.parquet(f"{output_root}/dim.Calendar").filter(F.col("Year") >= 2020).select("CalendarID").limit(30)
        
        cal_list = [r.CalendarID for r in cal_df.collect()]
        rows = []
        for emp in emp_df.collect():
            for cal_id in cal_list:
                rows.append((emp.EmployeeID, emp.PrimaryLocationID, cal_id, "09:00:00", "17:00:00", "Day", 30, 1, None))
        
        sched_df = spark.createDataFrame(rows, ["EmployeeID", "LocationID", "ShiftDateID", "StartTime",
                                                "EndTime", "ShiftType", "BreakMinutes", "IsApproved", "ApprovedBy"])
        sched_df = sched_df.withColumn("ScheduleID", F.monotonically_increasing_id() + 1).withColumn("CreatedDate", F.current_timestamp())
        write_parquet(sched_df, f"{output_root}/emp.Schedules")
        
        # Time tracking
        tt_rows = [(s.ScheduleID, s.EmployeeID, s.LocationID, s.ShiftDateID,
                   "2022-01-01 09:05:00", "2022-01-01 17:02:00", None, None)
                  for s in sched_df.collect()]
        tt_df = spark.createDataFrame(tt_rows, ["ScheduleID", "EmployeeID", "LocationID", "ClockInDateID",
                                                "ClockInTime", "ClockOutTime", "BreakStartTime", "BreakEndTime"])
        tt_df = tt_df.withColumn("TimeTrackingID", F.monotonically_increasing_id() + 1).withColumn("CreatedDate", F.current_timestamp())
        write_parquet(tt_df, f"{output_root}/emp.TimeTracking")
        
        # Payroll
        payroll_df = tt_df.groupBy("EmployeeID").agg(
            F.sum(F.expr("(unix_timestamp(ClockOutTime) - unix_timestamp(ClockInTime))/3600.0")).alias("HoursWorked")
        )
        payroll_df = (payroll_df
            .withColumn("PayrollID", F.monotonically_increasing_id() + 1)
            .withColumn("GrossPay", F.round(F.col("HoursWorked") * 15.0, 2))
            .withColumn("PayrollDateID", F.lit(None).cast("int"))
            .withColumn("CreatedDate", F.current_timestamp())
        )
        write_parquet(payroll_df, f"{output_root}/emp.Payroll")
        print("Schedules, time tracking, and payroll created")
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUTPUT_ROOT)

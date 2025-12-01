# 06_emp_schedules_timetracking_payroll.py
from utils import get_spark, write_parquet
import pyspark.sql.functions as F
from pyspark.sql import Window

def main(output_root="./output_parquet"):
    spark = get_spark("schedules")
    emp = spark.read.parquet(f"{output_root}/emp.Employees").select("EmployeeID","PrimaryLocationID").cache()
    cal = spark.read.parquet(f"{output_root}/dim.Calendar").filter(F.col("Year") >= 2020).select("CalendarID","CalendarDate").limit(365).cache()
    # Schedules: each employee has schedule for sample 30 days
    rows = []
    for e in emp.collect():
        empid = e.EmployeeID
        loc = e.PrimaryLocationID
        # assign 14 schedule days per employee for sample period
        for i, c in enumerate(cal.collect()[:30]):
            rows.append((empid, loc, c.CalendarID, "09:00:00", "17:00:00", "Day", 30, 1, None))
    sched_df = spark.createDataFrame(rows, ["EmployeeID","LocationID","ShiftDateID","StartTime","EndTime","ShiftType","BreakMinutes","IsApproved","ApprovedBy"]) \
                    .withColumn("ScheduleID", F.monotonically_increasing_id()+1).withColumn("CreatedDate", F.current_timestamp())
    write_parquet(sched_df.select("ScheduleID","EmployeeID","LocationID","ShiftDateID","StartTime","EndTime","ShiftType","BreakMinutes","IsApproved","ApprovedBy","CreatedDate"),
                  f"{output_root}/emp.Schedules")

    # TimeTracking: generate actual clock-in/out from schedule with small random offsets
    tt_rows = []
    for s in sched_df.select("ScheduleID","EmployeeID","LocationID","ShiftDateID","StartTime","EndTime").collect():
        # create simple clock in/out
        tt_rows.append((s.ScheduleID, s.EmployeeID, s.LocationID, s.ShiftDateID, "2022-01-01 09:05:00", "2022-01-01 17:02:00", None, None))
    tt_df = spark.createDataFrame(tt_rows, ["ScheduleID","EmployeeID","LocationID","ClockInDateID","ClockInTime","ClockOutTime","BreakStartTime","BreakEndTime"]) \
                 .withColumn("TimeTrackingID", F.monotonically_increasing_id()+1).withColumn("CreatedDate", F.current_timestamp())
    # payroll: aggregate hours per employee for the sample period
    payroll = tt_df.groupBy("EmployeeID").agg(F.sum(F.expr(" (unix_timestamp(ClockOutTime) - unix_timestamp(ClockInTime))/3600.0 ")).alias("HoursWorked")) \
            .withColumn("PayrollID", F.monotonically_increasing_id()+1) \
            .withColumn("GrossPay", F.round(F.col("HoursWorked") * 15.0,2)) \
            .withColumn("PayrollDateID", F.lit(None).cast("int")) \
            .withColumn("CreatedDate", F.current_timestamp())

    write_parquet(tt_df.select("TimeTrackingID","EmployeeID","LocationID","ClockInDateID","ClockInTime","ClockOutTime","BreakStartTime","BreakEndTime","CreatedDate"),
                  f"{output_root}/emp.TimeTracking")
    write_parquet(payroll.select("PayrollID","EmployeeID","HoursWorked","GrossPay","PayrollDateID","CreatedDate"),
                  f"{output_root}/emp.Payroll")

    spark.stop()

if __name__=="__main__":
    import sys
    out = sys.argv[1] if len(sys.argv)>1 else "./output_parquet"
    main(out)

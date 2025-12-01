
"""
game = True
while game:
    for i in range(10000000):
        spark = get_spark()
        calendar_df = spark.createDataFrame([
            (1, "2024-01-01", 2024, "Q1", 1, "2023-12-31", "2024-01-06", "2024-01-01", "2024-01-31"),
            (2, "2024-01-02", 2024, "Q1", 1, "2023-12-31", "2024-01-06", "2024-01-01", "2024-01-31")
        ], [
            "calendar_key","financial_date","financial_year","quarter",
            "week_number","week_start_date","week_end_date",
            "financial_month_start_date","financial_month_end_date"
        ])

"""
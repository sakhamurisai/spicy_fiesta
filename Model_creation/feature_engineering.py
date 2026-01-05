"""
Feature Engineering for Restaurant Recommendation System
Calculates RFM, taste profiles, spending patterns, and behavioral features
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FeatureEngineer:
    """Engineer features for ML models"""
    
    def __init__(self, spark):
        self.spark = spark
        self.reference_date = "2026-01-05"  # Current date for recency calculation
    
    def calculate_rfm_scores(self, transactions_df, customers_df):
        """Calculate Recency, Frequency, Monetary scores"""
        
        logger.info("Calculating RFM scores...")
        
        # Aggregate transaction data per customer
        rfm_base = transactions_df.groupBy("customer_id").agg(
            max("order_timestamp").alias("last_order_date"),
            min("order_timestamp").alias("first_order_date"),
            count("order_id").alias("total_orders"),
            sum("order_total").alias("total_spent"),
            avg("order_total").alias("avg_order_value")
        )
        
        # Calculate recency (days since last order)
        rfm_base = rfm_base.withColumn(
            "days_since_last_order",
            datediff(lit(self.reference_date), col("last_order_date"))
        ).withColumn(
            "days_since_first_order",
            datediff(lit(self.reference_date), col("first_order_date"))
        )
        
        # Calculate RFM scores (1-5 scale)
        # Recency: Lower is better
        rfm_percentiles = rfm_base.approxQuantile(
            "days_since_last_order", 
            [0.2, 0.4, 0.6, 0.8], 
            0.01
        )
        
        rfm_with_scores = rfm_base.withColumn(
            "recency_score",
            when(col("days_since_last_order") <= rfm_percentiles[0], 5)
            .when(col("days_since_last_order") <= rfm_percentiles[1], 4)
            .when(col("days_since_last_order") <= rfm_percentiles[2], 3)
            .when(col("days_since_last_order") <= rfm_percentiles[3], 2)
            .otherwise(1)
        )
        
        # Frequency: Higher is better
        freq_percentiles = rfm_base.approxQuantile(
            "total_orders",
            [0.2, 0.4, 0.6, 0.8],
            0.01
        )
        
        rfm_with_scores = rfm_with_scores.withColumn(
            "frequency_score",
            when(col("total_orders") >= freq_percentiles[3], 5)
            .when(col("total_orders") >= freq_percentiles[2], 4)
            .when(col("total_orders") >= freq_percentiles[1], 3)
            .when(col("total_orders") >= freq_percentiles[0], 2)
            .otherwise(1)
        )
        
        # Monetary: Higher is better
        monetary_percentiles = rfm_base.approxQuantile(
            "total_spent",
            [0.2, 0.4, 0.6, 0.8],
            0.01
        )
        
        rfm_with_scores = rfm_with_scores.withColumn(
            "monetary_score",
            when(col("total_spent") >= monetary_percentiles[3], 5)
            .when(col("total_spent") >= monetary_percentiles[2], 4)
            .when(col("total_spent") >= monetary_percentiles[1], 3)
            .when(col("total_spent") >= monetary_percentiles[0], 2)
            .otherwise(1)
        )
        
        # Create combined RFM score and segment
        rfm_with_scores = rfm_with_scores.withColumn(
            "rfm_score",
            col("recency_score") * 100 + col("frequency_score") * 10 + col("monetary_score")
        )
        
        rfm_with_scores = rfm_with_scores.withColumn(
            "rfm_segment",
            when(col("rfm_score") >= 444, "VIP")
            .when((col("rfm_score") >= 344) & (col("frequency_score") >= 4), "Loyal")
            .when(col("recency_score") <= 2, "At Risk")
            .when(col("frequency_score") <= 2, "Occasional")
            .otherwise("Regular")
        )
        
        logger.info(f"✅ RFM calculation complete")
        
        return rfm_with_scores
    
    def calculate_taste_profile(self, order_items_df, items_df, transactions_df):
        """Calculate customer taste preferences"""
        
        logger.info("Calculating taste profiles...")
        
        # Join order items with item details
        order_items_enriched = order_items_df \
            .join(items_df, "item_id") \
            .join(transactions_df.select("order_id", "customer_id"), "order_id")
        
        # Calculate spice preference per customer
        taste_profile = order_items_enriched.groupBy("customer_id").agg(
            avg("spice_level").alias("avg_spice_level"),
            stddev("spice_level").alias("spice_variance"),
            countDistinct("item_id").alias("unique_items_ordered"),
            countDistinct("category").alias("unique_categories"),
            sum("quantity").alias("total_items_ordered")
        )
        
        # Calculate spice preference score (0-100)
        taste_profile = taste_profile.withColumn(
            "spicy_preference_score",
            (col("avg_spice_level") / 10.0 * 100)
        )
        
        # Calculate experimentation score (variety / total orders ratio)
        taste_profile = taste_profile.withColumn(
            "experimentation_score",
            least(
                (col("unique_items_ordered") / col("total_items_ordered") * 200).cast("int"),
                lit(100)
            )
        )
        
        # Classify taste profile
        taste_profile = taste_profile.withColumn(
            "taste_profile",
            when((col("spicy_preference_score") >= 60) & (col("experimentation_score") < 40), "Spicy Lover")
            .when((col("spicy_preference_score") < 40) & (col("experimentation_score") < 40), "Mild Lover")
            .when(col("experimentation_score") >= 60, "Adventurous")
            .otherwise("Balanced")
        )
        
        # Get favorite category
        category_counts = order_items_enriched.groupBy("customer_id", "category") \
            .agg(count("*").alias("category_count"))
        
        window_spec = Window.partitionBy("customer_id").orderBy(desc("category_count"))
        
        favorite_category = category_counts \
            .withColumn("rank", row_number().over(window_spec)) \
            .filter(col("rank") == 1) \
            .select("customer_id", col("category").alias("favorite_category"))
        
        taste_profile = taste_profile.join(favorite_category, "customer_id", "left")
        
        logger.info(f"✅ Taste profile calculation complete")
        
        return taste_profile
    
    def calculate_channel_behavior(self, transactions_df):
        """Calculate channel preferences and behavior"""
        
        logger.info("Calculating channel behavior...")
        
        # Channel statistics per customer
        channel_stats = transactions_df.groupBy("customer_id", "channel") \
            .agg(count("*").alias("channel_count"))
        
        total_orders = transactions_df.groupBy("customer_id") \
            .agg(count("*").alias("total_orders"))
        
        channel_stats = channel_stats.join(total_orders, "customer_id")
        
        channel_stats = channel_stats.withColumn(
            "channel_pct",
            (col("channel_count") / col("total_orders") * 100)
        )
        
        # Get preferred channel (highest percentage)
        window_spec = Window.partitionBy("customer_id").orderBy(desc("channel_pct"))
        
        preferred_channel = channel_stats \
            .withColumn("rank", row_number().over(window_spec)) \
            .filter(col("rank") == 1) \
            .select("customer_id", col("channel").alias("preferred_channel"), col("channel_pct"))
        
        # Calculate channel diversity (uses multiple channels)
        channel_diversity = channel_stats.groupBy("customer_id") \
            .agg(countDistinct("channel").alias("num_channels_used"))
        
        channel_diversity = channel_diversity.withColumn(
            "channel_diversity",
            when(col("num_channels_used") >= 3, "High")
            .when(col("num_channels_used") == 2, "Medium")
            .otherwise("Low")
        )
        
        # Combine
        channel_behavior = preferred_channel.join(channel_diversity, "customer_id")
        
        logger.info(f"✅ Channel behavior calculation complete")
        
        return channel_behavior
    
    def calculate_timing_patterns(self, transactions_df):
        """Calculate time-of-day and day-of-week preferences"""
        
        logger.info("Calculating timing patterns...")
        
        # Time of day preference
        time_stats = transactions_df.groupBy("customer_id", "hour_of_day") \
            .agg(count("*").alias("hour_count"))
        
        total_orders = transactions_df.groupBy("customer_id") \
            .agg(count("*").alias("total_orders"))
        
        time_stats = time_stats.join(total_orders, "customer_id")
        
        time_stats = time_stats.withColumn(
            "hour_pct",
            (col("hour_count") / col("total_orders") * 100)
        )
        
        # Classify time of day
        time_stats = time_stats.withColumn(
            "time_period",
            when(col("hour_of_day") < 11, "Morning")
            .when(col("hour_of_day") < 15, "Lunch")
            .when(col("hour_of_day") < 20, "Dinner")
            .otherwise("Late Night")
        )
        
        window_spec = Window.partitionBy("customer_id").orderBy(desc("hour_count"))
        
        preferred_time = time_stats \
            .withColumn("rank", row_number().over(window_spec)) \
            .filter(col("rank") == 1) \
            .select("customer_id", col("time_period").alias("preferred_time_of_day"))
        
        # Weekend preference
        weekend_stats = transactions_df.groupBy("customer_id") \
            .agg(
                sum(when(col("is_weekend") == 1, 1).otherwise(0)).alias("weekend_orders"),
                count("*").alias("total_orders")
            )
        
        weekend_stats = weekend_stats.withColumn(
            "weekend_pct",
            (col("weekend_orders") / col("total_orders") * 100)
        )
        
        weekend_stats = weekend_stats.withColumn(
            "weekend_preference",
            when(col("weekend_pct") >= 60, "Weekend Lover")
            .when(col("weekend_pct") <= 40, "Weekday Regular")
            .otherwise("Balanced")
        )
        
        # Combine
        timing_patterns = preferred_time.join(
            weekend_stats.select("customer_id", "weekend_preference", "weekend_pct"),
            "customer_id"
        )
        
        logger.info(f"✅ Timing pattern calculation complete")
        
        return timing_patterns
    
    def calculate_visit_frequency(self, transactions_df):
        """Calculate visit frequency and trends"""
        
        logger.info("Calculating visit frequency...")
        
        # Order timestamps sorted
        window_spec = Window.partitionBy("customer_id").orderBy("order_timestamp")
        
        transactions_with_lag = transactions_df \
            .withColumn("prev_order_date", lag("order_timestamp").over(window_spec)) \
            .withColumn(
                "days_between_orders",
                datediff(col("order_timestamp"), col("prev_order_date"))
            ) \
            .filter(col("days_between_orders").isNotNull())
        
        visit_freq = transactions_with_lag.groupBy("customer_id").agg(
            avg("days_between_orders").alias("avg_days_between_orders"),
            stddev("days_between_orders").alias("visit_frequency_variance")
        )
        
        # Classify frequency
        visit_freq = visit_freq.withColumn(
            "visit_frequency_category",
            when(col("avg_days_between_orders") <= 3, "Daily")
            .when(col("avg_days_between_orders") <= 7, "Weekly")
            .when(col("avg_days_between_orders") <= 30, "Monthly")
            .otherwise("Occasional")
        )
        
        # Calculate order trend (increasing/decreasing over time)
        recent_window = Window.partitionBy("customer_id").orderBy(desc("order_timestamp")).rowsBetween(0, 4)
        old_window = Window.partitionBy("customer_id").orderBy("order_timestamp").rowsBetween(0, 4)
        
        trend_calc = transactions_df \
            .withColumn("recent_avg", avg("order_total").over(recent_window)) \
            .withColumn("old_avg", avg("order_total").over(old_window)) \
            .groupBy("customer_id") \
            .agg(
                max("recent_avg").alias("recent_avg_spending"),
                max("old_avg").alias("old_avg_spending")
            )
        
        trend_calc = trend_calc.withColumn(
            "spending_trend",
            when(col("recent_avg_spending") > col("old_avg_spending") * 1.2, "Increasing")
            .when(col("recent_avg_spending") < col("old_avg_spending") * 0.8, "Decreasing")
            .otherwise("Stable")
        )
        
        visit_freq = visit_freq.join(trend_calc.select("customer_id", "spending_trend"), "customer_id", "left")
        
        logger.info(f"✅ Visit frequency calculation complete")
        
        return visit_freq
    
    def calculate_churn_risk(self, rfm_df, visit_freq_df):
        """Calculate churn risk score"""
        
        logger.info("Calculating churn risk...")
        
        churn_features = rfm_df.select(
            "customer_id",
            "days_since_last_order",
            "recency_score",
            "frequency_score"
        ).join(
            visit_freq_df.select("customer_id", "avg_days_between_orders", "spending_trend"),
            "customer_id"
        )
        
        # Churn risk score (0-100)
        churn_features = churn_features.withColumn(
            "churn_risk_score",
            least(
                (
                    (col("days_since_last_order") / col("avg_days_between_orders") * 30) +
                    ((5 - col("recency_score")) * 10) +
                    ((5 - col("frequency_score")) * 10) +
                    when(col("spending_trend") == "Decreasing", 20).otherwise(0)
                ).cast("int"),
                lit(100)
            )
        )
        
        # Classify risk
        churn_features = churn_features.withColumn(
            "churn_risk_category",
            when(col("churn_risk_score") >= 70, "High")
            .when(col("churn_risk_score") >= 40, "Medium")
            .otherwise("Low")
        )
        
        logger.info(f"✅ Churn risk calculation complete")
        
        return churn_features
    
    def create_feature_matrix(self, data_dict):
        """Combine all features into single feature matrix"""
        
        logger.info("Creating master feature matrix...")
        
        transactions_df = data_dict['transactions']
        order_items_df = data_dict['order_items']
        items_df = data_dict['items']
        customers_df = data_dict['customers']
        
        # Calculate all feature sets
        rfm_df = self.calculate_rfm_scores(transactions_df, customers_df)
        taste_df = self.calculate_taste_profile(order_items_df, items_df, transactions_df)
        channel_df = self.calculate_channel_behavior(transactions_df)
        timing_df = self.calculate_timing_patterns(transactions_df)
        visit_freq_df = self.calculate_visit_frequency(transactions_df)
        churn_df = self.calculate_churn_risk(rfm_df, visit_freq_df)
        
        # Join all features
        feature_matrix = customers_df.select("customer_id") \
            .join(rfm_df, "customer_id") \
            .join(taste_df, "customer_id", "left") \
            .join(channel_df, "customer_id", "left") \
            .join(timing_df, "customer_id", "left") \
            .join(visit_freq_df, "customer_id", "left") \
            .join(churn_df.select("customer_id", "churn_risk_score", "churn_risk_category"), "customer_id", "left")
        
        # Fill nulls
        feature_matrix = feature_matrix.fillna({
            "avg_spice_level": 3.0,
            "experimentation_score": 30,
            "taste_profile": "Unknown",
            "favorite_category": "Unknown",
            "preferred_channel": "Unknown",
            "preferred_time_of_day": "Unknown",
            "weekend_preference": "Unknown",
            "avg_days_between_orders": 30.0,
            "visit_frequency_category": "Occasional",
            "churn_risk_score": 50,
            "churn_risk_category": "Medium"
        })
        
        logger.info(f"✅ Feature matrix created with {feature_matrix.count()} customers")
        
        return feature_matrix


def main():
    """Main execution"""
    
    spark = SparkSession.builder \
        .appName("Restaurant-Feature-Engineering") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    try:
        # Load generated data
        data_path = "/home/claude/ml_data"
        
        data_dict = {
            "customers": spark.read.parquet(f"{data_path}/customers"),
            "transactions": spark.read.parquet(f"{data_path}/transactions"),
            "order_items": spark.read.parquet(f"{data_path}/order_items"),
            "items": spark.read.parquet(f"{data_path}/items")
        }
        
        # Engineer features
        engineer = FeatureEngineer(spark)
        feature_matrix = engineer.create_feature_matrix(data_dict)
        
        # Display sample
        print("\n=== SAMPLE FEATURE MATRIX ===")
        feature_matrix.select(
            "customer_id", "rfm_segment", "taste_profile", 
            "spicy_preference_score", "experimentation_score",
            "preferred_channel", "visit_frequency_category",
            "churn_risk_category"
        ).show(10, truncate=False)
        
        # Save feature matrix
        output_path = f"{data_path}/feature_matrix"
        feature_matrix.write.mode("overwrite").parquet(output_path)
        logger.info(f"✅ Feature matrix saved to {output_path}")
        
        # Statistics
        print("\n=== FEATURE STATISTICS ===")
        feature_matrix.groupBy("rfm_segment").count().show()
        feature_matrix.groupBy("taste_profile").count().show()
        feature_matrix.groupBy("churn_risk_category").count().show()
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

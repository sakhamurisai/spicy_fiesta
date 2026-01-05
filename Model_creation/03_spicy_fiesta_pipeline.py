"""
COMPLETE PIPELINE - Spicy Fiesta Restaurant
Orchestrates entire ML workflow: Data → Features → Models → Recommendations
"""

import subprocess
import sys
import time
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml import PipelineModel
from pyspark.ml.recommendation import ALSModel

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SpicyFiestaPipeline:
    """Complete ML pipeline orchestrator"""
    
    def __init__(self, spark, data_path, models_path):
        self.spark = spark
        self.data_path = data_path
        self.models_path = models_path
        self.start_time = time.time()
    
    def print_banner(self):
        """Print startup banner"""
        banner = """
        ╔═══════════════════════════════════════════════════════════════╗
        ║                                                               ║
        ║       🌮 SPICY FIESTA RESTAURANT - ML PIPELINE 🤖             ║
        ║                                                               ║
        ║   Complete Recommendation & Prediction System                ║
        ║                                                               ║
        ╚═══════════════════════════════════════════════════════════════╝
        """
        print(banner)
        print(f"\n📅 Pipeline Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    def step_1_feature_engineering(self):
        """Calculate RFM scores and features"""
        
        logger.info("="*80)
        logger.info("STEP 1: FEATURE ENGINEERING")
        logger.info("="*80)
        
        from feature_engineering import FeatureEngineer
        
        # Load data
        data_dict = {
            "customers": self.spark.read.parquet(f"{self.data_path}/customers"),
            "transactions": self.spark.read.parquet(f"{self.data_path}/transactions"),
            "order_items": self.spark.read.parquet(f"{self.data_path}/order_items"),
            "items": self.spark.read.parquet(f"{self.data_path}/items")
        }
        
        # Engineer features
        engineer = FeatureEngineer(self.spark)
        feature_matrix = engineer.create_feature_matrix(data_dict)
        
        # Save
        feature_matrix.write.mode("overwrite").parquet(f"{self.data_path}/feature_matrix")
        
        logger.info(f"✅ Features created: {feature_matrix.count():,} customers")
        
        return feature_matrix
    
    def step_2_load_models(self):
        """Load trained models"""
        
        logger.info("="*80)
        logger.info("STEP 2: LOADING TRAINED MODELS")
        logger.info("="*80)
        
        models = {}
        
        try:
            # Load ALS
            models['als'] = ALSModel.load(f"{self.models_path}/collaborative_filtering")
            logger.info("✅ Loaded Collaborative Filtering model")
            
            # Load other models
            models['spending'] = PipelineModel.load(f"{self.models_path}/spending_predictor")
            logger.info("✅ Loaded Spending Predictor model")
            
            models['taste'] = PipelineModel.load(f"{self.models_path}/taste_classifier")
            logger.info("✅ Loaded Taste Classifier model")
            
            models['frequency'] = PipelineModel.load(f"{self.models_path}/frequency_predictor")
            logger.info("✅ Loaded Frequency Predictor model")
            
            models['churn'] = PipelineModel.load(f"{self.models_path}/churn_predictor")
            logger.info("✅ Loaded Churn Predictor model")
            
            # Load CF recommendations
            models['cf_recs'] = self.spark.read.parquet(f"{self.models_path}/cf_recommendations")
            logger.info("✅ Loaded pre-computed recommendations")
            
        except Exception as e:
            logger.error(f"❌ Error loading models: {e}")
            logger.error("Please run model training first: python 02_spicy_fiesta_model_training.py")
            raise
        
        return models
    
    def step_3_generate_recommendations(self, models, feature_matrix, top_n=10):
        """Generate recommendations for all customers"""
        
        logger.info("="*80)
        logger.info("STEP 3: GENERATING RECOMMENDATIONS")
        logger.info("="*80)
        
        items_df = self.spark.read.parquet(f"{self.data_path}/items")
        
        # Get collaborative filtering recommendations
        cf_recs = models['cf_recs'] \
            .select(
                col("customer_id"),
                explode("recommendations").alias("rec")
            ) \
            .select(
                col("customer_id"),
                col("rec.item_id").alias("item_id"),
                col("rec.rating").alias("cf_score")
            )
        
        # Join with item details
        recommendations = cf_recs.join(items_df, "item_id")
        
        # Rank recommendations
        window_spec = Window.partitionBy("customer_id").orderBy(desc("cf_score"))
        
        recommendations = recommendations \
            .withColumn("rank", row_number().over(window_spec)) \
            .filter(col("rank") <= top_n) \
            .select(
                "customer_id",
                "item_id",
                "item_name",
                "category",
                "price",
                "spice_level",
                "cf_score",
                "rank"
            )
        
        logger.info(f"✅ Generated {recommendations.count():,} recommendations")
        
        # Save recommendations
        recommendations.write.mode("overwrite").parquet(f"{self.data_path}/recommendations")
        
        return recommendations
    
    def step_4_predict_spending(self, models, feature_matrix):
        """Predict customer spending"""
        
        logger.info("="*80)
        logger.info("STEP 4: PREDICTING CUSTOMER SPENDING")
        logger.info("="*80)
        
        predictions = models['spending'].transform(feature_matrix)
        
        spending_predictions = predictions.select(
            "customer_id",
            col("prediction").alias("predicted_order_value"),
            "avg_order_value",
            "total_orders",
            "total_spent"
        )
        
        # Save predictions
        spending_predictions.write.mode("overwrite").parquet(f"{self.data_path}/spending_predictions")
        
        logger.info(f"✅ Predicted spending for {spending_predictions.count():,} customers")
        
        return spending_predictions
    
    def step_5_identify_churn_risk(self, models, feature_matrix):
        """Identify at-risk customers"""
        
        logger.info("="*80)
        logger.info("STEP 5: IDENTIFYING CHURN RISK")
        logger.info("="*80)
        
        predictions = models['churn'].transform(feature_matrix)
        
        churn_predictions = predictions.select(
            "customer_id",
            "churn_risk_category",
            "churn_risk_score",
            "days_since_last_order",
            "recency_score",
            "frequency_score"
        )
        
        # Count by risk level
        risk_counts = churn_predictions.groupBy("churn_risk_category").count().collect()
        for row in risk_counts:
            logger.info(f"  {row['churn_risk_category']}: {row['count']:,} customers")
        
        # Save predictions
        churn_predictions.write.mode("overwrite").parquet(f"{self.data_path}/churn_predictions")
        
        return churn_predictions
    
    def step_6_generate_notifications(self, feature_matrix, recommendations, churn_predictions):
        """Generate notification messages"""
        
        logger.info("="*80)
        logger.info("STEP 6: GENERATING NOTIFICATIONS")
        logger.info("="*80)
        
        # Get high churn risk customers
        high_risk = churn_predictions.filter(col("churn_risk_category") == "High")
        
        # Get top recommendation for each high-risk customer
        top_recs = recommendations.filter(col("rank") == 1)
        
        # Join customer profile
        customer_profile = feature_matrix.select(
            "customer_id",
            "rfm_segment",
            "taste_profile",
            "favorite_category",
            "preferred_channel"
        )
        
        # Create notifications
        notifications = high_risk \
            .join(customer_profile, "customer_id") \
            .join(top_recs.select("customer_id", col("item_name").alias("top_item")), "customer_id", "left") \
            .withColumn(
                "message",
                when(col("rfm_segment") == "VIP", 
                     concat(lit("🌟 VIP Special! We miss you. Get 20% off "), col("top_item"), lit(" today!")))
                .when(col("churn_risk_score") >= 80,
                      concat(lit("🎁 Come back! Enjoy "), col("top_item"), lit(" with 25% OFF!")))
                .otherwise(
                    concat(lit("🌮 Your favorite "), col("favorite_category"), lit(" items are waiting! Order now."))
                )
            ) \
            .withColumn("reason", lit("High Churn Risk")) \
            .withColumn("priority", 
                when(col("rfm_segment") == "VIP", lit("Critical"))
                .when(col("churn_risk_score") >= 80, lit("High"))
                .otherwise(lit("Medium"))
            ) \
            .withColumn("created_at", current_timestamp()) \
            .select(
                "customer_id",
                "message",
                "reason",
                "priority",
                "churn_risk_score",
                "preferred_channel",
                "created_at"
            )
        
        logger.info(f"✅ Generated {notifications.count():,} notifications")
        
        # Count by priority
        priority_counts = notifications.groupBy("priority").count().collect()
        for row in priority_counts:
            logger.info(f"  {row['priority']}: {row['count']:,} notifications")
        
        # Save notifications
        notifications.write.mode("overwrite").parquet(f"{self.data_path}/notifications")
        
        return notifications
    
    def step_7_create_summary_dashboard(self, feature_matrix, recommendations, spending_predictions, churn_predictions):
        """Create summary dashboard data"""
        
        logger.info("="*80)
        logger.info("STEP 7: CREATING SUMMARY DASHBOARD")
        logger.info("="*80)
        
        # Overall statistics
        total_customers = feature_matrix.count()
        
        # RFM segments
        rfm_counts = feature_matrix.groupBy("rfm_segment").count().orderBy(desc("count")).collect()
        
        # Taste profiles
        taste_counts = feature_matrix.groupBy("taste_profile").count().orderBy(desc("count")).collect()
        
        # Channel preference
        channel_counts = feature_matrix.groupBy("preferred_channel").count().orderBy(desc("count")).collect()
        
        # Average predictions
        avg_spending = spending_predictions.agg(avg("predicted_order_value")).collect()[0][0]
        
        # Churn risk distribution
        churn_counts = churn_predictions.groupBy("churn_risk_category").count().orderBy(desc("count")).collect()
        
        # Print dashboard
        print("\n" + "="*80)
        print("CUSTOMER INSIGHTS DASHBOARD")
        print("="*80)
        
        print(f"\n📊 Total Customers: {total_customers:,}")
        print(f"💰 Avg Predicted Order Value: ${avg_spending:.2f}")
        
        print("\n🎯 RFM Segments:")
        for row in rfm_counts:
            pct = (row['count'] / total_customers) * 100
            print(f"  {row['rfm_segment']:<15} {row['count']:>6,} ({pct:>5.1f}%)")
        
        print("\n🌶️  Taste Profiles:")
        for row in taste_counts:
            pct = (row['count'] / total_customers) * 100
            print(f"  {row['taste_profile']:<15} {row['count']:>6,} ({pct:>5.1f}%)")
        
        print("\n📱 Preferred Channels:")
        for row in channel_counts:
            pct = (row['count'] / total_customers) * 100
            print(f"  {row['preferred_channel']:<15} {row['count']:>6,} ({pct:>5.1f}%)")
        
        print("\n⚠️  Churn Risk:")
        for row in churn_counts:
            pct = (row['count'] / total_customers) * 100
            print(f"  {row['churn_risk_category']:<15} {row['count']:>6,} ({pct:>5.1f}%)")
        
        print("="*80)
    
    def run_complete_pipeline(self):
        """Execute complete pipeline"""
        
        self.print_banner()
        
        try:
            # Step 1: Feature Engineering
            feature_matrix = self.step_1_feature_engineering()
            
            # Step 2: Load Models
            models = self.step_2_load_models()
            
            # Step 3: Generate Recommendations
            recommendations = self.step_3_generate_recommendations(models, feature_matrix)
            
            # Step 4: Predict Spending
            spending_predictions = self.step_4_predict_spending(models, feature_matrix)
            
            # Step 5: Identify Churn Risk
            churn_predictions = self.step_5_identify_churn_risk(models, feature_matrix)
            
            # Step 6: Generate Notifications
            notifications = self.step_6_generate_notifications(
                feature_matrix, recommendations, churn_predictions
            )
            
            # Step 7: Create Dashboard
            self.step_7_create_summary_dashboard(
                feature_matrix, recommendations, spending_predictions, churn_predictions
            )
            
            # Final summary
            elapsed = time.time() - self.start_time
            
            print("\n" + "="*80)
            print("✅ PIPELINE COMPLETED SUCCESSFULLY!")
            print("="*80)
            print(f"⏱️  Total Time: {elapsed:.2f} seconds ({elapsed/60:.2f} minutes)")
            print(f"\n📁 Output Location: {self.data_path}")
            print("\nGenerated Files:")
            print("  ✅ feature_matrix/     - Customer features & scores")
            print("  ✅ recommendations/    - Top 10 items per customer")
            print("  ✅ spending_predictions/ - Predicted order values")
            print("  ✅ churn_predictions/  - Churn risk assessments")
            print("  ✅ notifications/      - Messages to send")
            print("="*80)
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {str(e)}")
            raise


def main():
    """Main execution"""
    
    # ========================================================================
    # CONFIGURATION
    # ========================================================================
    
    DATA_PATH = "/mnt/ml_data"  # Where your data is stored
    MODELS_PATH = f"{DATA_PATH}/models"
    
    # ========================================================================
    # INITIALIZE SPARK
    # ========================================================================
    
    spark = SparkSession.builder \
        .appName("SpicyFiesta-Complete-Pipeline") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .getOrCreate()
    
    try:
        # Run complete pipeline
        pipeline = SpicyFiestaPipeline(spark, DATA_PATH, MODELS_PATH)
        pipeline.run_complete_pipeline()
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

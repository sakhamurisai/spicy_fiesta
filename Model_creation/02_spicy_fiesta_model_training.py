"""
MODEL TRAINING - Spicy Fiesta Restaurant
Trains all ML models: Collaborative Filtering, Content-Based, Spending, Taste, Frequency, Churn
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator, ClusteringEvaluator
from pyspark.ml import Pipeline
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SpicyFiestaModelTraining:
    """Train all ML models for Spicy Fiesta"""
    
    def __init__(self, spark, data_path):
        self.spark = spark
        self.data_path = data_path
        self.models = {}
        
    def load_data(self):
        """Load prepared data"""
        
        logger.info("Loading prepared data...")
        
        self.customers_df = self.spark.read.parquet(f"{self.data_path}/customers")
        self.transactions_df = self.spark.read.parquet(f"{self.data_path}/transactions")
        self.order_items_df = self.spark.read.parquet(f"{self.data_path}/order_items")
        self.items_df = self.spark.read.parquet(f"{self.data_path}/items")
        
        # Load feature matrix if exists
        try:
            self.feature_matrix = self.spark.read.parquet(f"{self.data_path}/feature_matrix")
            logger.info("✅ Feature matrix loaded")
        except:
            logger.warning("⚠️  Feature matrix not found. Please run feature engineering first!")
            raise Exception("Feature matrix not found. Run 02_feature_engineering.py first")
        
        logger.info("✅ All data loaded successfully")
    
    def train_collaborative_filtering(self):
        """Train ALS model for item recommendations"""
        
        logger.info("="*80)
        logger.info("TRAINING MODEL 1: Collaborative Filtering (ALS)")
        logger.info("="*80)
        
        # Create user-item matrix
        user_item_matrix = self.order_items_df \
            .join(
                self.transactions_df.select("order_id", "customer_id"),
                "order_id"
            ) \
            .groupBy("customer_id", "item_id") \
            .agg(
                sum("quantity").alias("total_quantity"),
                count("*").alias("order_frequency"),
                sum("line_total").alias("total_spent")
            ) \
            .withColumn(
                "rating",
                (log1p(col("total_quantity")) * 2) + 
                (log1p(col("order_frequency")) * 3) +
                (log1p(col("total_spent")) * 1)
            )
        
        logger.info(f"User-item interactions: {user_item_matrix.count():,}")
        
        # Split data
        train_data, test_data = user_item_matrix.randomSplit([0.8, 0.2], seed=42)
        
        # Train ALS
        als = ALS(
            userCol="customer_id",
            itemCol="item_id",
            ratingCol="rating",
            rank=20,
            maxIter=15,
            regParam=0.1,
            coldStartStrategy="drop",
            implicitPrefs=False,
            nonnegative=True
        )
        
        model = als.fit(train_data)
        
        # Evaluate
        predictions = model.transform(test_data)
        evaluator = RegressionEvaluator(
            metricName="rmse",
            labelCol="rating",
            predictionCol="prediction"
        )
        rmse = evaluator.evaluate(predictions)
        
        # Generate recommendations for all users
        user_recs = model.recommendForAllUsers(10)
        
        logger.info(f"✅ ALS Model Trained - RMSE: {rmse:.4f}")
        
        self.models['collaborative_filtering'] = {
            'model': model,
            'recommendations': user_recs,
            'metrics': {'rmse': rmse}
        }
        
        return model, user_recs
    
    def train_spending_predictor(self):
        """Train model to predict customer spending"""
        
        logger.info("="*80)
        logger.info("TRAINING MODEL 2: Spending Predictor")
        logger.info("="*80)
        
        # Prepare features
        feature_cols = [
            "recency_score", "frequency_score", "monetary_score",
            "avg_order_value", "total_orders",
            "spicy_preference_score", "experimentation_score",
            "avg_days_between_orders", "churn_risk_score"
        ]
        
        ml_data = self.feature_matrix.select(
            "customer_id",
            *feature_cols,
            col("avg_order_value").alias("target")
        ).fillna(0)
        
        # Feature pipeline
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features"
        )
        
        scaler = StandardScaler(
            inputCol="features",
            outputCol="scaled_features"
        )
        
        rf = RandomForestRegressor(
            featuresCol="scaled_features",
            labelCol="target",
            numTrees=100,
            maxDepth=10,
            seed=42
        )
        
        pipeline = Pipeline(stages=[assembler, scaler, rf])
        
        # Split and train
        train_data, test_data = ml_data.randomSplit([0.8, 0.2], seed=42)
        model = pipeline.fit(train_data)
        
        # Evaluate
        predictions = model.transform(test_data)
        rmse_eval = RegressionEvaluator(labelCol="target", predictionCol="prediction", metricName="rmse")
        r2_eval = RegressionEvaluator(labelCol="target", predictionCol="prediction", metricName="r2")
        
        rmse = rmse_eval.evaluate(predictions)
        r2 = r2_eval.evaluate(predictions)
        
        logger.info(f"✅ Spending Predictor Trained - RMSE: {rmse:.4f}, R²: {r2:.4f}")
        
        self.models['spending_predictor'] = {
            'model': model,
            'metrics': {'rmse': rmse, 'r2': r2}
        }
        
        return model
    
    def train_taste_classifier(self):
        """Train taste profile classifier"""
        
        logger.info("="*80)
        logger.info("TRAINING MODEL 3: Taste Profile Classifier")
        logger.info("="*80)
        
        feature_cols = [
            "avg_spice_level",
            "spicy_preference_score",
            "experimentation_score",
            "spice_variance"
        ]
        
        ml_data = self.feature_matrix.select(
            "customer_id",
            *feature_cols
        ).fillna(0)
        
        # Feature pipeline
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        kmeans = KMeans(featuresCol="scaled_features", k=4, seed=42, maxIter=20)
        
        pipeline = Pipeline(stages=[assembler, scaler, kmeans])
        
        # Train
        model = pipeline.fit(ml_data)
        predictions = model.transform(ml_data)
        
        # Evaluate
        evaluator = ClusteringEvaluator(featuresCol="scaled_features", metricName="silhouette")
        silhouette = evaluator.evaluate(predictions)
        
        # Map clusters to taste profiles
        cluster_stats = predictions.groupBy("prediction").agg(
            avg("avg_spice_level").alias("avg_spice"),
            avg("experimentation_score").alias("avg_experiment"),
            count("*").alias("count")
        ).collect()
        
        cluster_mapping = {}
        for row in cluster_stats:
            cluster_id = row['prediction']
            spice = row['avg_spice']
            experiment = row['avg_experiment']
            
            if spice >= 6 and experiment < 50:
                profile = "Spicy Lover"
            elif spice < 4 and experiment < 50:
                profile = "Mild Lover"
            elif experiment >= 60:
                profile = "Adventurous"
            else:
                profile = "Balanced"
            
            cluster_mapping[cluster_id] = profile
            logger.info(f"  Cluster {cluster_id} → {profile} (n={row['count']:,})")
        
        logger.info(f"✅ Taste Classifier Trained - Silhouette: {silhouette:.4f}")
        
        self.models['taste_classifier'] = {
            'model': model,
            'cluster_mapping': cluster_mapping,
            'metrics': {'silhouette': silhouette}
        }
        
        return model, cluster_mapping
    
    def train_frequency_predictor(self):
        """Train visit frequency predictor"""
        
        logger.info("="*80)
        logger.info("TRAINING MODEL 4: Visit Frequency Predictor")
        logger.info("="*80)
        
        feature_cols = [
            "recency_score", "frequency_score",
            "avg_days_between_orders", "total_orders",
            "days_since_last_order", "churn_risk_score"
        ]
        
        # Index target variable
        indexer = StringIndexer(
            inputCol="visit_frequency_category",
            outputCol="frequency_label"
        )
        
        ml_data = self.feature_matrix.select(
            "customer_id",
            *feature_cols,
            "visit_frequency_category"
        ).fillna(0)
        
        ml_data = indexer.fit(ml_data).transform(ml_data)
        
        # Feature pipeline
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        rf = RandomForestClassifier(
            featuresCol="scaled_features",
            labelCol="frequency_label",
            numTrees=100,
            maxDepth=10,
            seed=42
        )
        
        pipeline = Pipeline(stages=[assembler, scaler, rf])
        
        # Split and train
        train_data, test_data = ml_data.randomSplit([0.8, 0.2], seed=42)
        model = pipeline.fit(train_data)
        
        # Evaluate
        predictions = model.transform(test_data)
        evaluator = MulticlassClassificationEvaluator(
            labelCol="frequency_label",
            predictionCol="prediction",
            metricName="accuracy"
        )
        accuracy = evaluator.evaluate(predictions)
        
        logger.info(f"✅ Frequency Predictor Trained - Accuracy: {accuracy:.4f}")
        
        self.models['frequency_predictor'] = {
            'model': model,
            'metrics': {'accuracy': accuracy}
        }
        
        return model
    
    def train_churn_predictor(self):
        """Train churn risk predictor"""
        
        logger.info("="*80)
        logger.info("TRAINING MODEL 5: Churn Risk Predictor")
        logger.info("="*80)
        
        feature_cols = [
            "days_since_last_order", "recency_score", "frequency_score",
            "monetary_score", "avg_days_between_orders",
            "total_orders", "churn_risk_score"
        ]
        
        # Index target
        indexer = StringIndexer(inputCol="churn_risk_category", outputCol="churn_label")
        
        ml_data = self.feature_matrix.select(
            "customer_id",
            *feature_cols,
            "churn_risk_category"
        ).fillna(0)
        
        ml_data = indexer.fit(ml_data).transform(ml_data)
        
        # Feature pipeline
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        gbt = GBTClassifier(
            featuresCol="scaled_features",
            labelCol="churn_label",
            maxIter=50,
            maxDepth=5,
            seed=42
        )
        
        pipeline = Pipeline(stages=[assembler, scaler, gbt])
        
        # Split and train
        train_data, test_data = ml_data.randomSplit([0.8, 0.2], seed=42)
        model = pipeline.fit(train_data)
        
        # Evaluate
        predictions = model.transform(test_data)
        acc_eval = MulticlassClassificationEvaluator(
            labelCol="churn_label", predictionCol="prediction", metricName="accuracy"
        )
        f1_eval = MulticlassClassificationEvaluator(
            labelCol="churn_label", predictionCol="prediction", metricName="f1"
        )
        
        accuracy = acc_eval.evaluate(predictions)
        f1 = f1_eval.evaluate(predictions)
        
        logger.info(f"✅ Churn Predictor Trained - Accuracy: {accuracy:.4f}, F1: {f1:.4f}")
        
        self.models['churn_predictor'] = {
            'model': model,
            'metrics': {'accuracy': accuracy, 'f1': f1}
        }
        
        return model
    
    def train_all_models(self):
        """Train all ML models"""
        
        logger.info("\n" + "="*80)
        logger.info("SPICY FIESTA - ML MODEL TRAINING")
        logger.info("="*80)
        
        # Load data
        self.load_data()
        
        # Train models
        self.train_collaborative_filtering()
        self.train_spending_predictor()
        self.train_taste_classifier()
        self.train_frequency_predictor()
        self.train_churn_predictor()
        
        logger.info("\n" + "="*80)
        logger.info("✅ ALL MODELS TRAINED SUCCESSFULLY")
        logger.info("="*80)
        
        return self.models
    
    def save_models(self, output_path):
        """Save trained models"""
        
        logger.info(f"\nSaving models to {output_path}...")
        
        for name, model_dict in self.models.items():
            if 'model' in model_dict:
                model_path = f"{output_path}/{name}"
                model_dict['model'].write().overwrite().save(model_path)
                logger.info(f"✅ Saved {name}")
        
        # Save collaborative filtering recommendations
        if 'collaborative_filtering' in self.models:
            recs_path = f"{output_path}/cf_recommendations"
            self.models['collaborative_filtering']['recommendations'] \
                .write.mode("overwrite").parquet(recs_path)
            logger.info(f"✅ Saved CF recommendations")
        
        logger.info("✅ All models saved!")
    
    def print_summary(self):
        """Print training summary"""
        
        print("\n" + "="*80)
        print("MODEL TRAINING SUMMARY")
        print("="*80)
        
        for name, model_dict in self.models.items():
            print(f"\n{name.upper().replace('_', ' ')}")
            print("-" * 40)
            if 'metrics' in model_dict:
                for metric, value in model_dict['metrics'].items():
                    print(f"  {metric.upper()}: {value:.4f}")
        
        print("\n" + "="*80)


def main():
    """Main execution"""
    
    # ========================================================================
    # CONFIGURATION
    # ========================================================================
    
    DATA_PATH = "/mnt/ml_data"  # Where data preparation saved parquet files
    MODEL_OUTPUT_PATH = f"{DATA_PATH}/models"
    
    # ========================================================================
    # INITIALIZE SPARK
    # ========================================================================
    
    spark = SparkSession.builder \
        .appName("SpicyFiesta-Model-Training") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .getOrCreate()
    
    try:
        # Train all models
        trainer = SpicyFiestaModelTraining(spark, DATA_PATH)
        models = trainer.train_all_models()
        
        # Save models
        trainer.save_models(MODEL_OUTPUT_PATH)
        
        # Print summary
        trainer.print_summary()
        
        print("\n" + "="*80)
        print("📍 NEXT STEP: Generate recommendations")
        print("   → python 03_spicy_fiesta_pipeline.py")
        print("="*80)
        
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

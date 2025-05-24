import sys
import os
import platform
import pandas as pd
import gc
import logging
from datetime import datetime
from typing import Tuple, Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, rand, when, isnan, isnull
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer, StandardScaler
from pyspark.ml.classification import LogisticRegression  # Changed from RandomForestClassifier
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# ──────────────────────────────── CONFIGURATION ────────────────────────────────

class Config:
    """Configuration class for the sentiment analysis pipeline"""
    
    # Data parameters
    MAX_SAMPLES_PER_CLASS = 3000  # Increased for better model performance
    TRAIN_RATIO = 0.8
    TEST_RATIO = 0.1
    VAL_RATIO = 0.1
    RANDOM_SEED = 42
    
    # Model parameters - Updated for Logistic Regression
    NUM_FEATURES = 1000  # Keep same as original
    MAX_ITER = 100       # For Logistic Regression iterations
    REG_PARAM = 0.01     # Regularization parameter
    ELASTIC_NET_PARAM = 0.1  # ElasticNet mixing parameter
    
    # Spark configuration
    DRIVER_MEMORY = "4g"
    EXECUTOR_MEMORY = "2g"
    SHUFFLE_PARTITIONS = "16"
    DEFAULT_PARALLELISM = "8"

# ──────────────────────────────── SETUP & LOGGING ────────────────────────────────

def setup_logging() -> logging.Logger:
    """Setup logging configuration"""
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_format)
    return logging.getLogger(__name__)

def get_project_paths() -> Dict[str, str]:
    """Get project directory paths"""
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    paths = {
        'project_root': project_root,
        'data_dir': os.path.join(project_root, "data"),
        'model_dir': os.path.join(project_root, "model", "best_model"),  # Keep same path
        'logs_dir': os.path.join(project_root, "logs"),
        'temp_dir': os.path.join(os.environ.get('TEMP', 'C:\\temp'), "spark-temp")
    }
    
    # Create directories
    for path in paths.values():
        os.makedirs(path, exist_ok=True)
    
    return paths

def create_spark_session(temp_dir: str) -> SparkSession:
    """Create and configure Spark session with optimized settings"""
    return SparkSession.builder \
        .appName("EnhancedSentimentClassifier") \
        .config("spark.driver.memory", Config.DRIVER_MEMORY) \
        .config("spark.executor.memory", Config.EXECUTOR_MEMORY) \
        .config("spark.sql.shuffle.partitions", Config.SHUFFLE_PARTITIONS) \
        .config("spark.default.parallelism", Config.DEFAULT_PARALLELISM) \
        .config("spark.memory.fraction", "0.7") \
        .config("spark.memory.storageFraction", "0.4") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.pyspark.python", sys.executable) \
        .config("spark.pyspark.driver.python", sys.executable) \
        .config("spark.python.worker.timeout", "300") \
        .config("spark.network.timeout", "600s") \
        .config("spark.driver.host", "localhost") \
        .config("spark.python.worker.reuse", "true") \
        .config("spark.local.dir", temp_dir) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

# ─────────────────────── DATA LOADING & PREPROCESSING ───────────────────────

def load_and_clean_data(spark: SparkSession, data_path: str, logger: logging.Logger) -> DataFrame:
    """Load and perform initial data cleaning"""
    logger.info("Loading data from CSV...")
    
    try:
        df = spark.read.csv(data_path, header=True, inferSchema=True)
        initial_count = df.count()
        logger.info(f"Initial data loaded: {initial_count:,} rows")
        
        # Data cleaning and filtering - Keep using lemmatized_text as in original
        df_cleaned = df.filter(
            (col("label").isin([0, 1, 2])) &
            (col("lemmatized_text").isNotNull()) &
            (col("lemmatized_text") != "")
        ).withColumn("label", col("label").cast("double"))
        
        # Remove rows with null or empty text
        df_cleaned = df_cleaned.filter(
            ~(isnull(col("lemmatized_text")) | 
              (col("lemmatized_text") == "") |
              isnan(col("lemmatized_text")))
        )
        
        cleaned_count = df_cleaned.count()
        logger.info(f"After cleaning: {cleaned_count:,} rows ({cleaned_count/initial_count*100:.1f}% retained)")
        
        return df_cleaned
        
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise

def balance_dataset(df: DataFrame, logger: logging.Logger) -> DataFrame:
    """Balance the dataset by sampling equal number of records per class"""
    logger.info("Balancing dataset...")
    
    # Show original distribution
    logger.info("Original class distribution:")
    original_dist = df.groupBy("label").count().orderBy("label")
    original_dist.show()
    
    # Balance classes
    balanced_dfs = []
    for label in [0.0, 1.0, 2.0]:
        class_df = df.filter(col("label") == label)
        class_count = class_df.count()
        
        if class_count > Config.MAX_SAMPLES_PER_CLASS:
            # Sample down if too many samples
            sampled_df = class_df.sample(
                fraction=Config.MAX_SAMPLES_PER_CLASS / class_count,
                seed=Config.RANDOM_SEED
            ).limit(Config.MAX_SAMPLES_PER_CLASS)
        else:
            sampled_df = class_df
        
        balanced_dfs.append(sampled_df)
        logger.info(f"Class {int(label)}: {sampled_df.count():,} samples")
    
    # Union all balanced classes
    balanced_df = balanced_dfs[0]
    for df_part in balanced_dfs[1:]:
        balanced_df = balanced_df.union(df_part)
    
    # Shuffle the data
    balanced_df = balanced_df.orderBy(rand(seed=Config.RANDOM_SEED))
    
    logger.info("Balanced class distribution:")
    balanced_df.groupBy("label").count().orderBy("label").show()
    
    return balanced_df

def split_data(df: DataFrame, logger: logging.Logger) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """Split data into train (80%), test (10%), and validation (10%) sets"""
    logger.info(f"Splitting data: {Config.TRAIN_RATIO*100}% train, {Config.TEST_RATIO*100}% test, {Config.VAL_RATIO*100}% validation")
    
    # First split: separate training from test+validation
    train_df, temp_df = df.randomSplit([Config.TRAIN_RATIO, Config.TEST_RATIO + Config.VAL_RATIO], seed=Config.RANDOM_SEED)
    
    # Second split: separate test from validation
    # Calculate the ratio for splitting temp_df into test and validation
    test_ratio_adjusted = Config.TEST_RATIO / (Config.TEST_RATIO + Config.VAL_RATIO)
    test_df, val_df = temp_df.randomSplit([test_ratio_adjusted, 1 - test_ratio_adjusted], seed=Config.RANDOM_SEED)
    
    # Log split sizes
    train_count = train_df.count()
    test_count = test_df.count()
    val_count = val_df.count()
    total_count = train_count + test_count + val_count
    
    logger.info(f"Training set: {train_count:,} rows ({train_count/total_count*100:.1f}%)")
    logger.info(f"Test set: {test_count:,} rows ({test_count/total_count*100:.1f}%)")
    logger.info(f"Validation set: {val_count:,} rows ({val_count/total_count*100:.1f}%)")
    
    return train_df, test_df, val_df

def export_datasets(train_df: DataFrame, test_df: DataFrame, val_df: DataFrame, 
                   data_dir: str, logger: logging.Logger):
    """Export datasets to various formats"""
    logger.info("Exporting datasets...")
    
    try:
        datasets = {
            'train': train_df,
            'test': test_df,
            'validation': val_df
        }
        
        for name, df in datasets.items():
            # Convert to Pandas and export
            pandas_df = df.toPandas()
            
            # Export to JSON
            json_path = os.path.join(data_dir, f"{name}_data.json")
            pandas_df.to_json(json_path, orient='records', lines=True)
            
            # Export to CSV
            csv_path = os.path.join(data_dir, f"{name}_data.csv")
            pandas_df.to_csv(csv_path, index=False)
            
            logger.info(f"Exported {name} dataset: {len(pandas_df):,} rows")
            
        gc.collect()  # Clean up memory
        
    except Exception as e:
        logger.error(f"Error exporting datasets: {str(e)}")
        raise

# ─────────────────────── MODEL BUILDING & TRAINING ───────────────────────

def create_pipeline() -> Pipeline:
    """Create the ML pipeline with feature engineering and Logistic Regression model"""
    return Pipeline(stages=[
        Tokenizer(inputCol="lemmatized_text", outputCol="words"),  # Keep using lemmatized_text
        StopWordsRemover(inputCol="words", outputCol="filtered_words"),
        HashingTF(inputCol="filtered_words", outputCol="tf", numFeatures=Config.NUM_FEATURES),
        IDF(inputCol="tf", outputCol="idf_features"),
        # Add StandardScaler for Logistic Regression (important for performance)
        StandardScaler(
            inputCol="idf_features",
            outputCol="features",
            withStd=True,
            withMean=False  # Keep False for sparse vectors
        ),
        StringIndexer(inputCol="label", outputCol="indexedLabel", handleInvalid="keep"),
        # Replace RandomForestClassifier with LogisticRegression
        LogisticRegression(
            featuresCol="features",
            labelCol="indexedLabel",
            maxIter=Config.MAX_ITER,
            regParam=Config.REG_PARAM,
            elasticNetParam=Config.ELASTIC_NET_PARAM,
            family="multinomial",  # For multiclass classification
            standardization=False,  # Already standardized above
            seed=Config.RANDOM_SEED
        )
    ])

def train_model(pipeline: Pipeline, train_df: DataFrame, logger: logging.Logger) -> PipelineModel:
    """Train the model pipeline"""
    logger.info("Training Logistic Regression model...")  # Updated message
    start_time = datetime.now()
    
    try:
        model = pipeline.fit(train_df)
        training_time = datetime.now() - start_time
        logger.info(f"Model training completed in {training_time}")
        return model
        
    except Exception as e:
        logger.error(f"Error during model training: {str(e)}")
        raise

# ─────────────────────── MODEL EVALUATION ───────────────────────

def evaluate_model(model: PipelineModel, test_df: DataFrame, val_df: DataFrame, 
                  logger: logging.Logger) -> Dict[str, Any]:
    """Comprehensive model evaluation"""
    logger.info("Evaluating model...")
    
    results = {}
    
    # Evaluate on both test and validation sets
    for dataset_name, dataset in [("test", test_df), ("validation", val_df)]:
        logger.info(f"Evaluating on {dataset_name} set...")
        
        predictions = model.transform(dataset)
        
        # Calculate accuracy
        correct_preds = predictions.filter(col("prediction") == col("indexedLabel")).count()
        total_preds = predictions.count()
        accuracy = correct_preds / total_preds if total_preds > 0 else 0
        
        # Calculate other metrics using built-in evaluator
        evaluator_accuracy = MulticlassClassificationEvaluator(
            labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
        evaluator_f1 = MulticlassClassificationEvaluator(
            labelCol="indexedLabel", predictionCol="prediction", metricName="f1")
        evaluator_precision = MulticlassClassificationEvaluator(
            labelCol="indexedLabel", predictionCol="prediction", metricName="weightedPrecision")
        evaluator_recall = MulticlassClassificationEvaluator(
            labelCol="indexedLabel", predictionCol="prediction", metricName="weightedRecall")
        
        metrics = {
            'accuracy': evaluator_accuracy.evaluate(predictions),
            'f1_score': evaluator_f1.evaluate(predictions),
            'precision': evaluator_precision.evaluate(predictions),
            'recall': evaluator_recall.evaluate(predictions),
            'manual_accuracy': accuracy
        }
        
        results[dataset_name] = metrics
        
        logger.info(f"{dataset_name.capitalize()} Set Metrics:")
        logger.info(f"  Accuracy: {metrics['accuracy']:.4f}")
        logger.info(f"  F1 Score: {metrics['f1_score']:.4f}")
        logger.info(f"  Precision: {metrics['precision']:.4f}")
        logger.info(f"  Recall: {metrics['recall']:.4f}")
        
        # Per-class metrics
        logger.info(f"\n{dataset_name.capitalize()} Set Per-Class Metrics:")
        calculate_per_class_metrics(predictions, logger)
        
        # Confusion matrix
        logger.info(f"\n{dataset_name.capitalize()} Set Confusion Matrix:")
        predictions.groupBy("indexedLabel", "prediction").count().orderBy("indexedLabel", "prediction").show()
    
    return results

def calculate_per_class_metrics(predictions: DataFrame, logger: logging.Logger):
    """Calculate and display per-class precision, recall, and F1 score"""
    labels = [float(row[0]) for row in predictions.select("indexedLabel").distinct().collect()]
    
    for label in sorted(labels):
        TP = predictions.filter((col("indexedLabel") == label) & (col("prediction") == label)).count()
        FP = predictions.filter((col("indexedLabel") != label) & (col("prediction") == label)).count()
        FN = predictions.filter((col("indexedLabel") == label) & (col("prediction") != label)).count()
        
        precision = TP / (TP + FP) if (TP + FP) > 0 else 0
        recall = TP / (TP + FN) if (TP + FN) > 0 else 0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
        
        logger.info(f"  Class {int(label)}: Precision={precision:.4f}, Recall={recall:.4f}, F1={f1:.4f}")

def save_model_and_results(model: PipelineModel, results: Dict[str, Any], 
                          model_dir: str, logger: logging.Logger):
    """Save the trained model and evaluation results"""
    try:
        # Save model
        model_path = os.path.join(model_dir, "SentimentModel")  # Keep same name
        model.write().overwrite().save(model_path)
        logger.info(f"Logistic Regression model saved to: {model_path}")  # Updated message
        
        # Save results
        results_path = os.path.join(model_dir, "evaluation_results.json")
        import json
        with open(results_path, 'w') as f:
            json.dump(results, f, indent=2)
        logger.info(f"Evaluation results saved to: {results_path}")
        
    except Exception as e:
        logger.error(f"Error saving model and results: {str(e)}")
        raise

def show_sample_predictions(model: PipelineModel, test_df: DataFrame, logger: logging.Logger):
    """Show sample predictions for inspection"""
    logger.info("Sample predictions:")
    predictions = model.transform(test_df.limit(10))
    predictions.select("lemmatized_text", "label", "prediction", "probability").show(10, truncate=50)

# ──────────────────────────────── MAIN EXECUTION ────────────────────────────────

def main():
    """Main execution function"""
    logger = setup_logging()
    paths = get_project_paths()
    spark = None
    
    try:
        logger.info("=== Enhanced Sentiment Analysis Pipeline Started (Logistic Regression) ===")  # Updated message
        
        # Initialize Spark
        spark = create_spark_session(paths['temp_dir'])
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created successfully")
        
        # Load and preprocess data
        data_path = os.path.join(paths['data_dir'], "cleaned_reviews.csv")
        df = load_and_clean_data(spark, data_path, logger)
        df_balanced = balance_dataset(df, logger)
        
        # Split data into train/test/validation
        train_df, test_df, val_df = split_data(df_balanced, logger)
        
        # Export datasets
        export_datasets(train_df, test_df, val_df, paths['data_dir'], logger)
        
        # Build and train model
        pipeline = create_pipeline()
        model = train_model(pipeline, train_df, logger)
        
        # Evaluate model
        results = evaluate_model(model, test_df, val_df, logger)
        
        # Save model and results
        save_model_and_results(model, results, paths['model_dir'], logger)
        
        # Show sample predictions
        show_sample_predictions(model, test_df, logger)
        
        logger.info("=== Logistic Regression Pipeline completed successfully! ===")  # Updated message
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
        
    finally:
        if spark:
            logger.info("Stopping Spark session...")
            spark.stop()
            logger.info("Spark session stopped.")
        
        # Final memory cleanup
        gc.collect()

if __name__ == "__main__":
    main()
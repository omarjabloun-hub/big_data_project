# pip install spark pyspark

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


from pyspark.sql.types import IntegerType, StringType, StructField, StructType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("income", IntegerType(), True),
    StructField("education", StringType(), True),
    StructField("region", StringType(), True),
    StructField("loyalty_status", StringType(), True),
    StructField("purchase_frequency", StringType(), True),
    StructField("purchase_amount", IntegerType(), True),
    StructField("product_category", StringType(), True),
    StructField("promotion_usage", IntegerType(), True),
    StructField("satisfaction_score", IntegerType(), True),
])


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.customer_data (
        id INT PRIMARY KEY,
        age INT,
        gender TEXT,
        income INT,
        education TEXT,
        region TEXT,
        loyalty_status TEXT,
        purchase_frequency TEXT,
        purchase_amount INT,
        product_category TEXT,
        promotion_usage INT,
        satisfaction_score INT);
    """)
    logging.info("Table created successfully!")



def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars', "./jars/slf4j-api-1.7.30.jar,./jars/spark-cassandra-connector_2.13-3.4.1.jar,./jars/snappy-java-1.1.10.1.jar") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
            .config("spark.ui.showConsoleProgress", "true") \
            .config("spark.executor.extraJavaOptions", "-verbose:class") \
            .config("spark.driver.extraJavaOptions", "-verbose:class") \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'customer_data_topic') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    return sel

def start_streaming_to_cassandra(selection_df):
    streaming_query = (selection_df.writeStream
                       .format("org.apache.spark.sql.cassandra")
                       .option('checkpointLocation', '/tmp/checkpoint')
                       .option('keyspace', 'spark_streams')
                       .option('table', 'customer_data')
                       .start())

    streaming_query.awaitTermination()

if __name__ == "__main__":
    spark_conn = create_spark_connection()  # Spark connection
    if spark_conn:
        spark_df = connect_to_kafka(spark_conn)  # Connect to Kafka
        if spark_df:
            selection_df = create_selection_df_from_kafka(spark_df)  # Process Kafka data
            session = create_cassandra_connection()  # Cassandra connection
            if session:
                create_keyspace(session)  # Create keyspace
                logging.info("Creating Table...")
                create_table(session)  # Create table
                start_streaming_to_cassandra(selection_df)  # Start streaming data to Cassandra

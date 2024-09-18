import os
from pyspark.sql import SparkSession
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit

# Function to get a Snowpark session
def get_snowpark_session():
    return Session.builder.configs({
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'role': os.getenv('SNOWFLAKE_ROLE'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'database': os.getenv('SNOWFLAKE_DATABASE'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA')
    }).create()

# Function to process CSV with Spark and interact with Snowpark
def process_csv_with_spark(file_path):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("CSV Processing") \
        .getOrCreate()

    # Read the CSV file into a Spark DataFrame
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    df.show()

    # Convert Spark DataFrame to Pandas DataFrame for Snowpark
    pandas_df = df.toPandas()

    # Get Snowpark session
    session = get_snowpark_session()

    # Create a Snowpark DataFrame from the Pandas DataFrame
    snowpark_df = session.create_dataframe(pandas_df)

    # Example transformation: Creating a table and inserting data
    snowpark_df.write.mode('overwrite').save_as_table('my_table')

    # Example transformation: Querying the table
    result = session.table('my_table').select(col('id'), col('name')).filter(col('id') == lit(1)).collect()
    print(result)

    # Example of creating a stage
    session.sql("""
    CREATE OR REPLACE STAGE my_stage
    """).collect()

    # Example of creating a pipe
    session.sql("""
    CREATE OR REPLACE PIPE my_pipe AS
    COPY INTO my_table FROM @my_stage
    """).collect()

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    FILE_PATH = 'data/output.csv'
    process_csv_with_spark(FILE_PATH)

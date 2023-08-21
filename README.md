import pyspark
from pyspark.sql import SparkSession
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def create_error_log_table(spark):
    spark.sql("""
        CREATE TABLE IF NOT EXISTS error_log (
            timestamp STRING,
            database STRING,
            table_name STRING,
            error_message STRING
        )
    """)

def log_error_to_table(spark, timestamp, database, table_name, error_message):
    spark.sql(f"""
        INSERT INTO error_log VALUES (
            '{timestamp}', '{database}', '{table_name}', '{error_message}'
        )
    """)

def process_tables(database_list, batch_size):
    spark = SparkSession \
        .builder \
        .appName("Hive metastore to Unity Catalog migration") \
        .config("spark.databricks.sync.command.enableManagedTable", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    new_catalog = ""
    old_catalog = "hive_metastore"
    sync_cmd_error_list = ['HIVE_SERDE', 'DBFS_ROOT_LOCATION', 'EXTERNAL_TABLE_IN_MANAGED_LOCATION',
                           'INVALID_DATASOURCE_FORMAT', 'NOT_EXTERNAL', 'READ_ONLY_CATALOG',
                           'TABLE_ALREADY_EXISTS', 'TIMEOUT']

    create_error_log_table(spark)

    try:
        for start_idx in range(0, len(database_list), batch_size):
            batch_databases = database_list[start_idx:start_idx + batch_size]

            for db in batch_databases:
                try:
                    table_list = [row.tableName for row in spark.sql(f"SHOW TABLES IN {db}").collect()]
                    for tbl in table_list:
                        try:
                            sync_query = f"""SYNC TABLE {new_catalog}.{db}.{tbl} FROM {old_catalog}.{db}.{tbl} DRY RUN;"""
                            df_sync = spark.sql(sync_query)
                            status_code = str(df_sync.first()['status_code'])
                            if status_code in sync_cmd_error_list:
                                logger.error(f"Sync error for table {tbl} in database {db}. Status code: {status_code}")
                                log_error_to_table(spark, str(df_sync.first()['timestamp']), db, tbl, f"Status code: {status_code}")
                            else:
                                logger.info(f"Sync successful for table {tbl} in database {db}")
                        except Exception as inner_ex:
                            error_message = str(inner_ex).replace("'", "''")  # Escape single quotes for SQL
                            logger.error(f"Error occurred while syncing table {tbl} in database {db}: {inner_ex}")
                            log_error_to_table(spark, str(df_sync.first()['timestamp']), db, tbl, error_message)
                except Exception as db_ex:
                    logger.error(f"Error occurred while processing database {db}: {db_ex}")

    except Exception as main_ex:
        logger.error(f"Error occurred during batch processing: {main_ex}")
    
    finally:
        spark.stop()
        logger.info("Spark session stopped")

def main():
    batch_size = 10  # Adjust the batch size as needed
    database_list = [row.databaseName for row in spark.sql("SHOW DATABASES").collect()]
    logger.info(f"Total databases to process: {len(database_list)}")
    process_tables(database_list, batch_size)

if __name__ == "__main__":
    main()

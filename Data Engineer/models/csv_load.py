import os
from pyspark.sql import SparkSession
import logging

logger = logging.getLogger()

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s"
)

DB_FORMAT = "com.microsoft.sqlserver.jdbc.spark"
DB_URL = "jdbc:sqlserver://{SERVER_ADDR};databaseName=emp;"
DB_USER = "user"
DB_PASSWORD = "password"
PATH = "path/"


def read_csv(filename, spark):
    df = spark.read.csv(filename)

    if df is None:
        return False

    return df

def get_csv_list(path):
    csv_list = os.listdir(path)

    if csv_list is None:
        return False

    return csv_list

def insert_df_record(df, table):
    df.write \
    .format(DB_FORMAT) \
    .mode("overwrite") \
    .option("url", DB_URL) \
    .option("dbtable", table) \
    .option("user", DB_USER) \
    .option("password", DB_PASSWORD) \
    .save()

def main():
    logger.info("Starting")
    
    spark = SparkSession.builder \
            .appName('csv_load') \
            .master("local[*]") \
            .getOrCreate() \

    csv_list = get_csv_list(PATH)
    logger.info("CSV list: %s", csv_list)

    for csv in csv_list:
        df = read_csv(csv, spark)
        logger.info("DF csv name: %s", df)
        df.printSchema()

        table_name = csv.split("/")[-1].split(".")[0]
        logger.info("Inserting into table: %s", table_name)
        insert_df_record(df, table_name)
        
    logger.info("Finished")


if __name__ == "__main__":
    main()
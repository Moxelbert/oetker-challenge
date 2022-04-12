import os
import sys
import json


from pyspark.sql import SparkSession

from connection.api_con import get_datasets
from transformations.data_cleansing import DataCleansing
from transformations.data_analyzing import DataAnalyzing
from connection.mysql_con import MySql


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# create Spark Session
try:
    spark = SparkSession.builder\
                .appName('MORITZ-CHALLENGE') \
                .config('spark.jars.packages', 'mysql:mysql-connector-java:8.0.28') \
                .getOrCreate()
    print('Spark Session successfully created')
except Exception as e:
    print('failed to create Spark Session')
    print(e)
    sys.exit()


# store data from the API locally
raw_data = get_datasets(os.environ['API_URL'])
json_string = json.dumps(raw_data)

with open('json_data.json', 'w') as outfile:
    outfile.write(json_string)

# load data into a dataframe
dataframe = spark.read.format("json").load("json_data.json", multiLine=True)

# transform data and store it in a new dataframe
try:
    cleansedDf = DataCleansing().cleanse_df(dataframe)
    print('transformation of data successful')
except Exception as e:
    print('failed to transform data')
    print(e)
    sys.exit()

# Analyze the data
try:
    data_analyzer = DataAnalyzing()
    df_grouped_by_country = data_analyzer.group_by_country(cleansedDf)
    df_grouped_by_country.show(500, truncate=False)
    df_grouped_by_date = data_analyzer.group_by_date(cleansedDf)
    df_grouped_by_date.show(500, truncate=False)
except Exception as e:
    print(e)
    sys.exit()

# Establish a connection to MySql (hosted in AWS)
my_sql = MySql(os.environ['MYSQL_USER'], os.environ['MYSQL_PW'])

# Write the data into the MySQL table
try:
    my_sql.write_to_db(df_grouped_by_country, os.environ['MYSQL_URL'], os.environ['MYSQL_TARGET_TABLE_COUNTRY'], 'append')
    my_sql.write_to_db(df_grouped_by_date, os.environ['MYSQL_URL'], os.environ['MYSQL_TARGET_TABLE_DATE'], 'append')
    print('transformed data successfully pushed to MySQL DB')
except Exception as e:
    print(e)
    sys.exit()

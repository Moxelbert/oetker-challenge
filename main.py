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

#create Spark Session
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


#store data from the API locally
raw_data = get_datasets()
json_string = json.dumps(raw_data)

with open('json_data.json', 'w') as outfile:
    outfile.write(json_string)

#load data into a dataframe
dataframe = spark.read.format("json").load("json_data.json", multiLine=True)

#transform data and write it into different table
try:
    cleansedDf = DataCleansing().cleanse_df(dataframe)
    print('transformation of data successful')
except Exception as e:
    print('failed to transform data')
    print(e)
    sys.exit()

#extract information from the data
try:
    data_analyzer = DataAnalyzing()
    df_grouped_by_country = data_analyzer.group_by_countries(cleansedDf)
    df_grouped_by_date = data_analyzer.group_by_date(cleansedDf)
except Exception as e:
    print(e)
    sys.exit()

my_sql = MySql(os.environ['MYSQL_USER'], os.environ['MYSQL_PW'])

try:
    my_sql.write_to_db(df_grouped_by_date, os.environ['MYSQL_URL'], os.environ['MYSQL_TARGET_TABLE'], 'append')
    print('transformed data successfully pushed to MySQL DB')
except Exception as e:
    print(e)
    sys.exit()


if __name__ == '__main__':
    main()
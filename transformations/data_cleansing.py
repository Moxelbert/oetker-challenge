from pyspark.sql.functions import to_date, col, initcap

class DataCleansing:

    def cleanse_df(self, dataframe):
        dataframe = dataframe.withColumn('date_ts', to_date(col('date'), 'dd/MM/yyyy')) \
                            .withColumn('first_name_initcap', initcap(col('first_name'))) \
                            .withColumn('last_name_initcap', initcap(col('last_name'))) \
                            .withColumn('country_initcap', initcap(col('country'))) \
                            .withColumnRenamed('first_name_initcap', 'first_name1') \
                            .withColumnRenamed('last_name_initcap', 'last_name1')
        dataframe = dataframe.select(
            'id',
            col('first_name1').alias('first_name'),
            col('last_name1').alias('last_name'),
            'gender',
            'country',
            'email',
            'ip_address',
            'date',
            'date_ts'
        )
        return dataframe
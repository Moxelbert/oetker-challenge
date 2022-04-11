from pyspark.sql.functions import col, countDistinct, coalesce, round

class DataAnalyzing:
    def group_by_countries(self, dataframe):
        df_female = dataframe.withColumnRenamed('country', 'country1') \
                    .groupBy('country1', 'gender') \
                    .agg(countDistinct('id').alias('female_occurrences')) \
                    .orderBy(col('female_occurrences').desc()) \
                    .filter(col('gender') == 'Female')
        df_male = dataframe.withColumnRenamed('country', 'country2') \
                    .groupBy('country2', 'gender') \
                    .agg(countDistinct('id').alias('male_occurrences')) \
                    .orderBy(col('male_occurrences').desc()) \
                    .filter(col('gender') == 'Male')
        df_mixed = df_male.join(df_female, df_male.country2 == df_female.country1, "outer")
        df_mixed = df_mixed.withColumn('country', coalesce(df_mixed.country1, df_mixed.country2)) \
                    .na.fill(value=0) \
                    .withColumn('total_occurrences', col('female_occurrences') + col('male_occurrences')) \
                    .withColumn('female_ratio', round(col('female_occurrences')/col('total_occurrences'), 2)) \
                    .withColumn('male_ratio', round(col('male_occurrences') / col('total_occurrences'), 2)) \

        df_mixed = df_mixed.select(
            'country',
            'female_occurrences',
            'male_occurrences',
            'total_occurrences',
            'female_ratio',
            'male_ratio'
        ).orderBy('total_occurrences', ascending=False)
        return df_mixed

    def group_by_date(self, dataframe):
        dataframe.show(20, truncate=False)
        dataframe = dataframe.groupBy('date_ts') \
                    .agg(countDistinct('id').alias('occurrence_per_date')) \
                    .orderBy('occurrence_per_date', ascending=False)
        return dataframe

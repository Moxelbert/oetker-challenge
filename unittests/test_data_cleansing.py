import unittest
from pyspark.sql import SparkSession
from transformations.data_cleansing import DataCleansing

class TestDataCleansing(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
            .appName('UNITTEST_CLEANSING') \
            .getOrCreate()
        cls.dataframe_bad = cls.spark.read.format("json").load("test_json.json", multiLine=True)

    def test_cleanse_df(self):
        data_cleanser = DataCleansing()
        dataframe_cleansed = data_cleanser.cleanse_df(self.dataframe_bad)
        self.assertEqual(dataframe_cleansed.collect()[0]['first_name'][0], 'B')
    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

if __name__ == '__main__':
    unittest.main()
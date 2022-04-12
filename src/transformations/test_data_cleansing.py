import unittest
from pyspark.sql import SparkSession
from data_cleansing import DataCleansing


class TestDataCleansing(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
            .appName('UNITTEST_CLEANSING') \
            .getOrCreate()
        cls.dataframe = cls.spark.read.format("json").load("test_json.json", multiLine=True)

    def test_cleanse_df(self):
        data_cleanser = DataCleansing()
        dataframe_cleansed = data_cleanser.cleanse_df(self.dataframe)
        self.assertEqual(dataframe_cleansed.collect()[0]['first_name'][0], 'B')
        self.assertEqual(dataframe_cleansed.collect()[0]['last_name'][0], 'K')
        self.assertEqual(dataframe_cleansed.collect()[0]['country'][0], 'F')
        self.assertEqual(dataframe_cleansed.collect()[0]['valid_ip'], 'no')

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()


if __name__ == '__main__':
    unittest.main()

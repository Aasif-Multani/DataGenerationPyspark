import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import os
from data_generation_pyspark import generate_testing_data


class GenerateTestDataTest(unittest.TestCase):
    """
    This test file imports the necessary modules, defines a test class GenerateTestDataTest, and then defines several test methods with different scenarios:
    test_generate_testing_data_csv: tests if the generated CSV data contains the expected header row
    test_generate_testing_data_json: tests if the generated JSON data starts with the expected key
    test_generate_testing_data_avro: tests if the generated AVRO data is None (since we can't easily check its contents)
    test_generate_testing_data_invalid_format: tests if the method returns None when an invalid format is provided
    test_generate_testing_data_write_csv: tests if the generated CSV file is written to disk when write parameter is True
    The setUpClass and tearDownClass methods are run once before and after all tests, respectively, to start and stop a SparkSession. The setUp method is run before each test and sets up the default parameters for the method. The tearDown method is run after each test and deletes the generated CSV file. Finally, the main method runs all the tests.
    """
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName('testing').getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.schema = StructType([
            StructField('id', IntegerType(), nullable=False),
            StructField('name', StringType(), nullable=False),
            StructField('age', IntegerType(), nullable=False),
            StructField('gender', StringType(), nullable=False)
        ])
        self.enumerations = {
            'name': ['John', 'Jane', 'Bob', 'Alice'],
            'gender': ['male', 'female', 'other']
        }
        self.dataset_size = 10
        self.format = 'CSV'
        self.write = False

    # def tearDown(self):
    #     os.remove('output.csv')
    #     os.remove('output.avro')

    def test_generate_testing_data_csv(self):
        data = generate_testing_data(self.format, self.schema, self.enumerations, self.dataset_size, self.write)
        self.assertIn('id,name,age,gender\n', data)

    def test_generate_testing_data_json(self):
        self.format = 'JSON'
        data = generate_testing_data(self.format, self.schema, self.enumerations, self.dataset_size, self.write)
        self.assertIn('{"id":', data[0])

    def test_generate_testing_data_avro(self):
        self.format = 'AVRO'
        data = generate_testing_data(self.format, self.schema, self.enumerations, self.dataset_size, self.write)
        self.assertEqual(data, None)

    def test_generate_testing_data_invalid_format(self):
        self.format = 'invalid_format'
        data = generate_testing_data(self.format, self.schema, self.enumerations, self.dataset_size, self.write)
        self.assertEqual(data, None)

    def test_generate_testing_data_write_csv(self):
        self.write = True
        generate_testing_data(self.format, self.schema, self.enumerations, self.dataset_size, self.write)
        self.assertTrue(os.path.exists('output.csv'))


if __name__ == '__main__':
    unittest.main()

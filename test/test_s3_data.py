import boto3
import unittest

class S3TestCase(unittest.TestCase):

    def setUp(self):
        self.bucket_name = 'my-test-bucket'
        self.s3 = boto3.client('s3')
        self.s3_location = 'your_s3_location'

    # Create an empty S3 bucket
    #self.s3.create_bucket(Bucket=self.bucket_name)
    def test_is_bucket_empty(self):
        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=self.s3_location)
        objects = response.get('Contents', [])
        self.assertEqual(len(objects), 0, "S3 location is not empty")

    def test_file_presence(self):
        file_extensions = ['.json', '.csv', '.avro']
        for extension in file_extensions:
            s3_key = f"{self.s3_location}/file{extension}"
            response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=s3_key)
            objects = response.get('Contents', [])
            self.assertTrue(len(objects) > 0, f"No {extension} file found in S3 location")

    def test_file_exists(self):
        # Upload a file to S3
        self.s3.upload_file('my_file.json', self.bucket_name, 'data/my_file.json')

        # Check if the file exists
        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix='data/my_file.json')
        self.assertTrue('Contents' in response)

        # Upload a file to S3
        self.s3.upload_file('my_file.csv', self.bucket_name, 'data/my_file.csv')

        # Check if the file exists
        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix='data/my_file.csv')
        self.assertTrue('Contents' in response)

        # Upload a file to S3
        self.s3.upload_file('my_file.avro', self.bucket_name, 'data/my_file.avro')

        # Check if the file exists
        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix='data/my_file.avro')
        self.assertTrue('Contents' in response)


if __name__ == '__main__':
    unittest.main()

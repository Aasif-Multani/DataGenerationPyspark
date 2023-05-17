import os
import shutil
import subprocess
import zipfile
import boto3

def clone_repo_from_bitbucket(repo_url, clone_path):
    # Clone the repository from Bitbucket using Git command line
    subprocess.run(['git', 'clone', repo_url, clone_path])

def create_zip_file(source_dir, zip_file_path):
    # Create a zip file of the source directory
    with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(source_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arc_name = os.path.relpath(file_path, source_dir)
                zipf.write(file_path, arcname=arc_name)

def upload_file_to_s3(bucket_name, file_path, s3_key):
    s3 = boto3.client('s3')
    s3.upload_file(file_path, bucket_name, s3_key)

# Configuration
repo_url = 'https://bitbucket.org/username/repo.git'
clone_path = '/path/to/clone/repo'
source_dir = '/path/to/clone/repo'
zip_file_path = '/path/to/zip/file.zip'
bucket_name = 'my-bucket'
s3_key = 'path/to/upload/file.zip'

# Clone the repository from Bitbucket
clone_repo_from_bitbucket(repo_url, clone_path)

# Create a zip file of the cloned repository
create_zip_file(source_dir, zip_file_path)

# Upload the zip file to S3 bucket
upload_file_to_s3(bucket_name, zip_file_path, s3_key)

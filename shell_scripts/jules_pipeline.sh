#!/bin/bash

# Set the BitBucket repository name and AWS region and S3 bucket name
REPO_NAME="your-repo-name"
AWS_REGION="your-aws-region"
S3_BUCKET_NAME="your-s3-bucket-name"

# Clone the BitBucket repository
git clone git@bitbucket.org:username/${REPO_NAME}.git

# Zip the repository contents
zip -r ${REPO_NAME}.zip ${REPO_NAME}

# Copy the zipped repository to S3
aws s3 cp ${REPO_NAME}.zip s3://${S3_BUCKET_NAME}/${REPO_NAME}.zip

# Remove the zipped repository
rm ${REPO_NAME}.zip

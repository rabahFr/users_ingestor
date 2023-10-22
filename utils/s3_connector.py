import boto3


class S3Connector:
    def __init__(self, config):
        self.aws_access_key = config['access_key_id']
        self.aws_secret_key = config['access_secret']

        # Initialize an S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key
        )

    def list_files(self, bucket, path):
        try:
            # List all objects in the specified S3 folder
            response = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=path
            )

            # Extract and return the list of file keys (paths)
            if 'Contents' in response:
                file_keys = [obj['Key'] for obj in response['Contents']]
                return file_keys

            return []
        except Exception as e:
            print(f"Error listing files: {str(e)}")
            return []

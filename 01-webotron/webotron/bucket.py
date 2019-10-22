# -*- coding: utf-8 -*-

"""Classes for s3 Bucket."""

from pathlib import Path
import mimetypes
from hashlib import md5
from botocore.exceptions import ClientError
import util


class BucketManager:
    """Manage an s3 bucket."""
    CHUNK_SIZE = 8388608

    def __init__(self, session):
        """Create a Bucket Manager object."""
        self.session = session
        self.s3 = session.resource('s3')
        self.transfer_config = boto.s3.transfer.TransferConfig(
            multipart_chunksize = CHUNK_SIZE,
            multipart_threshold = CHUNK_SIZE
            )
        self.manifest = {}

    def get_region_name(self, bucket):
        """Get the bucket's region name."""
        client = self.s3.meta.client
        bucket_location = client.get_bucket_location(Bucket=bucket.name)

        return bucket_location["LocationConstraint"] or 'us-east-1'

    def get_bucket_url(self, bucket):
        """Get the website URL for this bucket."""
        web_host = util.get_endpoint(self.get_region_name(bucket)).host
        return F"http://{bucket.name}.{web_host}"

    def all_buckets(self):
        """Get an iterator for all buckets."""
        return self.s3.buckets.all()

    def all_objects(self, bucket_name):
        """Get an iterator for a bucket."""
        return self.s3.Bucket(bucket_name).objects.all()

    def init_bucket(self, bucket_name):
        """Create a new bucket or return an existing one."""
        s3_bucket = None
        try:
            s3_bucket = self.s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={
                    'LocationConstraint': self.session.region_name
                    }
                    )
        except ClientError as error:
            # print(e.response)
            if error.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                s3_bucket = self.s3.Bucket(bucket_name)
            else:
                raise error
        return s3_bucket

    def set_policy(self, bucket):
        """Set bucket policy to be readbale by everyone."""
        policy = """{
            "Version":"2012-10-17",
            "Statement":[{
            "Sid":"PublicReadGetObject",
            "Effect":"Allow",
            "Principal": "*",
                "Action":["s3:GetObject"],
                "Resource":["arn:aws:s3:::%s/*"
                ]
                }
           ]
           }""" % bucket.name
        policy = policy.strip()
        pol = bucket.Policy()
        pol.put(Policy=policy)

    def configure_website(self, bucket):
        """Configure website for bucket."""
        bucket.Website().put(
            WebsiteConfiguration={
                'ErrorDocument': {
                    'Key': 'error.html'
                    },
                'IndexDocument': {
                    'Suffix': 'index.html'
                }
            })

    def load_maifest(self, bucket):
        """Load manifest for cahcing purposes."""
        paginator = self.s3.meta.client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket.name'):
            for obj in page.get('Contents',[]):
                self.manifest[obj['Key']] = obj['ETag']

    @staticmethod
    def hash_data(data):
        """Generate md5 hash for data."""
        hash = md5()
        hash.update(path)
        return hash

    def gen_etag(self, filename):
        """Get ETag for file."""
        hash = None
        with open(file, 'rb') as f:
            data = f.read()
            hash = self.hash_data(data)

            return F'"{hash.hexdigest()}"'

    @staticmethod
    def upload_file(bucket, path, key):
        """Upload an object to s3 Bucket."""
        content_type = mimetypes.guess_type(key)[0] or 'text/plain'
        return bucket.upload_file(
            path,
            key,
            ExtraArgs={
                'ContentType': content_type
                },
                Config=self.transfer_config
                )

    def sync(self, pathname, bucket_name):
        """Sync local website folder with s3 bucket."""
        bucket = self.s3.Bucket(bucket_name)
        self.load_maifest(bucket)
        root = Path(pathname).expanduser().resolve()

        def handle_directory(target):
            for path in target.iterdir():
                if path.is_dir():
                    handle_directory(path)
                if path.is_file():
                    self.upload_file(
                        bucket,
                        str(path),
                        str(path.relative_to(root))
                        )
                # print(F"Path: {p}\n key: {p.relative_to(root)}")
        handle_directory(root)

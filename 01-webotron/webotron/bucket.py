# -*- coding: utf-8 -*-

"""Classes for s3 Bucket."""

from pathlib import Path
import mimetypes
from hashlib import md5
import boto3
from botocore.exceptions import ClientError
from functools import reduce
from webotron import util


class BucketManager:
    """Manage an s3 bucket."""

    CHUNK_SIZE = 8388608

    def __init__(self, session):
        """Create a Bucket Manager object."""
        self.session = session
        self.s3 = session.resource('s3')
        self.transfer_config = boto3.s3.transfer.TransferConfig(
            multipart_chunksize=self.CHUNK_SIZE,
            multipart_threshold=self.CHUNK_SIZE
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
        # print ("Loading manifest")
        paginator = self.s3.meta.client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket.name):
            for obj in page.get('Contents', []):
                self.manifest[obj['Key']] = obj['ETag']

    @staticmethod
    def hash_data(data):
        """Generate md5 hash for data."""
        hash = md5()
        hash.update(data)
        return hash

    def gen_etag(self, path):
        """Get ETag for file."""
        hashes = []
        with open(path, 'rb') as f:
            while True:
                data = f.read(self.CHUNK_SIZE)
                if not data:
                    break
            hashes.append(self.hash_data(data))

        if not hashes:
            return
        elif len(hashes) == 1:
            return F'"{hashes[0].hexdigest()}"'
        else:
            hash = self.hash_data(
                        reduce(
                            lambda x, y: x + y,
                            (h.digest() for h in hashes)
                            )
                        )
            return F'"{hash.hexdigest()}-{len(hashes)}"'


    def upload_file(self, bucket, path, key):
        """Upload an object to s3 Bucket."""
        content_type = mimetypes.guess_type(key)[0] or 'text/plain'
        etag = self.gen_etag(path)
        if self.manifest.get(key, '') == etag:
            # print(F"Skipping {key}, etags match")
            return
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

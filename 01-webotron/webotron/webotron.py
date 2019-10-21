#!/usr/bin/python
# -*- coding: utf-8 -*-


"""Webotorn: Deploy websites with aws SDK Python.

Automates the process of deploying static websites.
- Configure AWS s3 buckets
    - Create AWS s3 BUCKET
    - Set them up for static websites
    - Deploy local files to them
- Configure DNS with AWS Route53
- Configure a content delivery Network and SSL with AWS CloudFront
"""

import boto3
import click

from bucket import BucketManager

session = boto3.Session(profile_name='default')
bucket_manager = BucketManager(session)


@click.group()
def cli():
    """Webotron deploys websites to AWS."""


@cli.command('list-buckets')
def list_buckets():
    """List all s3 buckets."""
    for bucket in bucket_manager.all_buckets():
        print(bucket)


@cli.command('list-bucket-objects')
@click.argument('bucket')
def list_bucket_objects(bucket):
    """List objects in an s3 bucket."""
    for obj in bucket_manager.all_objects(bucket):
        print(obj)


@cli.command('setup-bucket')
@click.argument('bucket')
def setup_bucket(bucket):
    """Create and configure S3 bucket, enable website."""
    s3_bucket = bucket_manager.init_bucket(bucket)
    bucket_manager.set_policy(s3_bucket)
    bucket_manager.configure_website(s3_bucket)


@cli.command('sync')
@click.argument('pathname', type=click.Path(exists=True))
@click.argument('bucket')
def sync(pathname, bucket):
    """Sync contents of PATHNAME to BUCKET."""
    bucket_manager.sync(pathname, bucket)


if __name__ == "__main__":
    cli()

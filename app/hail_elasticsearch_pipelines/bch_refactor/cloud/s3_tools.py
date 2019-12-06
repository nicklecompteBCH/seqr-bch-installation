import boto3

import os

def parse_vcf_s3_path(s3path):
    parsed = urlparse(s3path)
    bucket = parsed.netloc
    path = parsed.path[1:]
    object_list = path.split('/')
    filename = object_list[-1]
    return {
        "bucket" : bucket,
        "path" : path,
        "filename" : filename
    }

def add_vcf_to_hdfs(s3path_to_vcf):

    parts = parse_vcf_s3_path(s3path_to_vcf)
    s3buckets = boto3.resource('s3')
    s3bucket = s3buckets.Bucket(parts['bucket'])
    s3bucket.download_file(parts['path'], parts['filename'])
    os.system('hdfs dfs -put ' + parts['filename'])

from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'),encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='seqr-bch-installation',
    version='0.0.1',
    description='Scripts and source code for deploying Seqr on AWS'
    long_description=long_description,
    long_description_content_type='test/markdown'
    url='https://github.com/nicklecompteBCH/bch-seqr-installation/',
    author-'BCH Research Computing - Genomics, based on Seqr and hail-elasticsearch-pipelines by github.com/macacarthurlab',
    author_email='nicholas.lecompte@childrens.harvard.edu',
    packages=find_packages(),
    python_requires=">=3.6,<3.8",
    extras_require={
        'aws' : ['boto3','botocore']
    }
)
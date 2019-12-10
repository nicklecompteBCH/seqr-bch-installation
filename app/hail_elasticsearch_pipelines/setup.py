from setuptools import find_packages, setup

with open("README.md") as f:
    readme = f.read()

setup(
    name='hail_elasticsearch_pipelines',
    version='0.1.0',
    description="Scripts and modules for loading VCFs into a Spark cluster running hail, running VEP, and exporting the results to Elasticsearch.",
    long_description=readme,
    packages = find_packages(),
    setup_requires=["pytest-runner","pycurl"],
    tests_require=["pytest"],
    install_requires = [
    ],
    project_urls = { "Source code" : "https://github.com/nicklecompteBCH/seqr-bch-installation"
    }
)
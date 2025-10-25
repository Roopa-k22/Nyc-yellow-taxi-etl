from setuptools import setup, find_packages

setup(
    name='Nyc_yellow_taxi_etl_pipeline',
    version='1.0',
    packages=find_packages(),
    install_requires=[
        'apache-beam[gcp]==2.58.0',
        'google-cloud-bigquery==3.10.0',
        'google-cloud-storage==2.14.0'
    ],
)
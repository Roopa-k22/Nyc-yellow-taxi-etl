from setuptools import setup, find_packages

setup(
    name='Nyc_yellow_taxi_etl',
    version='1.0.0',
    packages=find_packages(),  # will find the 'etl' package automatically
    install_requires=[
        'apache-beam[gcp]==2.58.0',
        'google-cloud-bigquery==3.10.0',
        'google-cloud-storage>=2.16.0,<3',  # updated to avoid Beam conflicts
        'pytest==8.3.0'
    ],
    python_requires='>=3.9',
)

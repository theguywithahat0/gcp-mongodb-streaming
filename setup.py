from setuptools import setup, find_packages

setup(
    name="gcp-mongodb-streaming",
    version="0.1.0",
    description="A framework for streaming data from MongoDB to Google Cloud Pub/Sub using Apache Beam",
    author="PinguPower",
    packages=find_packages(include=["src", "src.*", "use_cases", "use_cases.*"]),
    install_requires=[
        "apache-beam[gcp]>=2.40.0",
        "pymongo>=4.0.0",
        "google-cloud-pubsub>=2.13.0",
        "python-dotenv>=0.20.0",
        "pyyaml>=6.0",
    ],
    python_requires=">=3.8",
) 
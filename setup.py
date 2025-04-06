from setuptools import setup, find_packages

setup(
    name="gcp-mongodb-streaming",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "motor",
        "python-dotenv",
        "apache-beam",
        "google-cloud-pubsub"
    ],
) 
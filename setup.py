from setuptools import setup, find_packages

setup(
    name="aws-utils",
    version="0.1.0",
    author="Christopher Little",
    author_email="chris@speedsheet.co.uk",
    description="Package for interacting with AWS",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/Oxford-Data-Processes/aws-utils",
    packages=find_packages(include=["aws_utils", "aws_utils.*"]),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.11",
    install_requires=["boto3", "jsonschema"],
)

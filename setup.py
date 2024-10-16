from setuptools import setup, find_packages

setup(
    name="aws-utils",  # Replace with your package name
    version="0.1.0",  # Initial version
    author="Christopher Little",
    author_email="chris@speedsheet.co.uk",
    description="Package for interacting with AWS",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/Oxford-Data-Processes/aws-utils",  # URL to your project
    packages=find_packages(),  # Automatically find packages in your project
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",  # Change to your license
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.11",  # Specify the Python version required
    install_requires=[
        "boto3",
    ],
)

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pymysql_helpers-AlwaysLaet", # Replace with your own username
    version="0.0.1",
    author="Thomas Laetsch",
    author_email="me@thomaslaetsch.com",
    description="Subjectively useful wrapper classes for pyMySql",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/AlwaysLaet/pyMySqlHelpers",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)

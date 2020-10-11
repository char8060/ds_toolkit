import setuptools

with open("README.md", "r") as fh:
        long_description = fh.read()

setuptools.setup(
    name="ds_toolkit", # Replace with your own username
    version="0.0.1",
    author="Charlie Mueller",
    author_email="charles.mueller@rackspace.com",
    description="toolkit for datascience",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/char8060/ds_toolkit",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)

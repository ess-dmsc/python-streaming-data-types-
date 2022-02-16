import os

from setuptools import find_packages, setup

from streaming_data_types._version import version

DESCRIPTION = "Python utilities for handling ESS streamed data"

here = os.path.abspath(os.path.dirname(__file__))

# Import the README and use it as the long-description.
try:
    with open(os.path.join(here, "README.md"), encoding="utf-8") as f:
        LONG_DESCRIPTION = "\n" + f.read()
except Exception as error:
    print("COULD NOT GET LONG DESC: {}".format(error))
    LONG_DESCRIPTION = DESCRIPTION

setup(
    name="ess_streaming_data_types",
    version=version,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    author="ScreamingUdder",
    url="https://github.com/ess-dmsc/python-streaming-data-types",
    license="BSD 2-Clause License",
    packages=find_packages(exclude=["tests", "tests.*"]),
    python_requires=">=3.6.0",
    install_requires=["flatbuffers==1.12", "numpy"],
    extras_require={"dev": ["flake8", "pre-commit", "pytest", "tox"]},
)

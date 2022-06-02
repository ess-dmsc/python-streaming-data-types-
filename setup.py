import os

from setuptools import find_packages, setup
import re

with open("streaming_data_types/_version.py", "r") as f:
    version_text = f.read()
    result = re.search(R'version\s=\s\"(?P<version_string>.*)\"$', version_text)
    version = result["version_string"]

DESCRIPTION = "Python utilities for handling ESS streamed data"

here = os.path.abspath(os.path.dirname(__file__))

# Import the README and use it as the long-description.
try:
    with open(os.path.join(here, "README.md"), encoding="utf-8") as f:
        LONG_DESCRIPTION = "\n" + f.read()
except Exception as error:
    print("COULD NOT GET LONG DESC: {}".format(error))
    LONG_DESCRIPTION = DESCRIPTION

ext_modules = None
try:
    from pybind11.setup_helpers import Pybind11Extension, build_ext
    ext_modules = [
        Pybind11Extension("fast_f142_serialiser",
                          ["cpp_src/f142_serialiser.cpp"],
                          depends=["cpp_src/f142_serialiser.h"],
                          include_dirs=["cpp_src",],
                          define_macros=[('VERSION_INFO', version)],
                          extra_compile_args=["-std=c++2a"],
                          ),
    ]
except ImportError:
    pass

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
    extras_require={"dev": ["flake8", "pre-commit", "pytest", "tox"], "cpp_f142": ["pybind11"]},
    ext_modules=ext_modules
)

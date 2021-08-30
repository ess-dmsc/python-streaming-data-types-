# Python Streaming Data Types
## For developers

### Install the commit hooks (important)
There are commit hooks for Black and Flake8.

The commit hooks are handled using [pre-commit](https://pre-commit.com).

To install the hooks for this project run:
```
pre-commit install
```

To test the hooks run:
```
pre-commit run --all-files
```
This command can also be used to run the hooks manually.

### Adding new schemas checklist (important)
* Generate Python bindings for the schema using FlatBuffers' `flatc` executable
* Add the generated bindings to the project
* Add unit-tests (see existing tests for an example)
* Update `fbschemas.__init__.py` to include the new serialiser and deserialiser
* Check whether the serialised data produced by the new code can be verified in C++
  * There is a helper program in the [FlatBufferVerification](https://github.com/ess-dmsc/FlatBufferVerification) repository
  * Don't worry if it fails verification - it seems to be an inherent FlatBuffers issue
* Add the schema and verifiability result to the table of schemas in `README.md`

### Tox
Tox allows the unit tests to be run against multiple versions of Python.
See the tox.ini file for which versions are supported.
From the top directory:
```
tox
```

### Installing the development version locally
First, uninstall any existing versions of the Python streaming data types package:

```
pip uninstall ess-streaming-data-types
```
Then, from the _python-streaming-data-types_ root directory, run the following command:

```
pip install --user -e ./
```

### Building the package locally and deploying it to PyPI

#### Requirements
* A [PyPi](https://pypi.org/) account
* A [TestPyPi](https://test.pypi.org/) account (this is separate to the PyPi account)
* Permission to push to the ess-streaming-data-types project on TestPyPi and PyPi
* Installed all requirements in `requirements-dev.txt`

#### Steps

***First update the __version__ number in streaming_data_types/__init__.py and push the update to the repository.***

Uninstall streaming_data_types if you have previously installed it from PyPi:
```
pip uninstall ess_streaming_data_types
```

Delete any old builds you may have (IMPORTANT!):
```
rm -rf build dist
```

Build it locally:
```
python setup.py sdist bdist_wheel
```

Check dist files:
```
twine check dist/*
```

Push to test.pypi.org for testing:
```
twine upload --repository-url https://test.pypi.org/legacy/ dist/*  
```

The new module can then be installed from test.pypi.org like so:
```
pip uninstall ess-streaming-data-types    # Remove old version if present
pip install -i https://test.pypi.org/simple/ ess-streaming-data-types
```
Unfortunately, flatbuffers is not on test.pypi.org so the following error may occur:
```
ERROR: Could not find a version that satisfies the requirement flatbuffers
```
The workaround is to install flatbuffers manually first using `pip install flatbuffers` and then rerun the previous command.

Test the module using the existing test-suite (from project root):
```
rm -rf streaming_data_types    # Rename the local source directory
pytest    # The tests will be run against the pip installed module
git reset --hard origin/main    # Put everything back to before
```

After testing installing from test.pypi.org works, push to PyPI:
```
twine upload dist/*
```
Finally, create a tag on the GitHub repository with the appropriate name, e.g. `v0.7.0`.

### Build and upload conda package

The conda package is used by ESS DMSC DRAM group for the Scipp library.
Please create the release version tag on github before creating the conda package as it gets the version number from the tag.

Note: anecdotal evidence suggests that this works better on Linux than on MacOS.

#### Steps

You must first have a conda installation, for example `conda` via pip, or [miniconda](https://docs.conda.io/en/latest/miniconda.html).

From the directory of the ess-streaming-data-types repository, build the package with
```sh
conda install -c conda-forge conda-build anaconda-client
conda build -c conda-forge ./conda
```

To upload the package, first login
```sh
anaconda login
```
use the ESS-DMSC-ECDC account or personal account linked to ESS-DMSC organisation.

Find the path for the built package using
```sh
conda build ./conda --output
```

Then upload
```sh
anaconda upload --user ESS-DMSC /path/to/package
```

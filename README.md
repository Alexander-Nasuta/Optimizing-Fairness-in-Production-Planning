![FAIRWork Logo](https://fairwork-project.eu/assets/images/2022.09.15-FAIRWork-Logo-V1.0-color.svg)
# Optimizing Fairness in Production Planning: A Human-Centric Approach to Machine and Workforce Allocation

## Setup 
Most scripts contain a `if __name__ == "__main__":` block, which can be used to run the script as a standalone script. 
This block showcases the main functionality of the script and can be used to test the script.
For local development the REST API can be run locally.

1. Clone the repository
2. Install the requirements by running `pip install -r requirements_dev.txt` (dev contains the requirements for testing and development)
3. install tox by running `pip install tox`
4. Install the project locally by running `pip install -e .`

## Examples

Code blocks showcasing the individual layers of the approach will be added here soon.

## Testing

### Unit Tests with `pytest`
Unit tests are implemented using `pytest`. 
To run the tests, run `pytest` in the root directory of the repository.
Note, that you have to install the 'requirements_dev.txt' (`pip install -r requirements_dev.txt`) to run the tests.
The configuration for the tests is located in the `pyproject.toml` file.


### Unit Tests with `tox`
Tox is a generic virtualenv management and test command line tool you can use for checking that your package installs correctly with different Python versions and interpreters.
To run the tests with `tox`, run `tox` in the root directory of the repository.
The configuration for the tests is located in the `tox.ini` file.


# FAIRWork Project
Development Repository for AI Services for the FAIRWork Project

“This work has been supported by the FAIRWork project (www.fairwork-project.eu) and has been funded within the European Commission’s Horizon Europe Programme under contract number 101049499. This paper expresses the opinions of the authors and not necessarily those of the European Commission. The European Commission is not liable for any use that may be made of the information contained in this presentation.”

Copyright © RWTH of FAIRWork Consortium

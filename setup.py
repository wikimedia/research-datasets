
from setuptools import (
    find_packages,
    setup
)


setup(
    name="wmf_research_datasets",
    version="0.1",
    install_requires=[
        "findspark",
        "wmfdata @ git+https://github.com/wikimedia/wmfdata-python.git@release",
    ],
    packages=find_packages(),
    python_requires=">=3",
    author="Wikimedia Foundation Research team",
    license="BSD 3-Clause"
)

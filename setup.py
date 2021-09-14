
from setuptools import (
    find_packages,
    setup
)


setup(
    name="wmf_research",
    version="0.1",
    install_requires=[
        "apache-airflow",       
        "findspark",
        "pillow",
        "wmfdata @ git+https://github.com/wikimedia/wmfdata-python.git@release",
    ],
    packages=find_packages(),
    python_requires=">=3",
    author="Wikimedia Foundation Research team",
    license="BSD 3-Clause"
)

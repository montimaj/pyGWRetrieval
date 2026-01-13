"""Setup script for pyGWRetrieval package."""

from setuptools import setup, find_packages

if __name__ == "__main__":
    setup(
        packages=find_packages(exclude=["tests", "tests.*", "docs", "examples"]),
        entry_points={
            'console_scripts': [
                'pygwretrieval=pyGWRetrieval.cli:main',
            ],
        },
    )

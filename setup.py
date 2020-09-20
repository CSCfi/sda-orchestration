"""Settings for building package."""

from setuptools import setup
from sda_orchestrator import __author__, __title__, __version__


setup(
    name=__title__,
    version=__version__,
    url="",
    project_urls={
        "Source": "",
    },
    license="Apache 2.0",
    author=__author__,
    author_email="",
    description="SDA orchestrator",
    long_description="",
    packages=["sda_orchestrator", "sda_orchestrator/utils"],
    # If any package contains *.json, include them:
    package_data={"": ["*.html"]},
    entry_points={
        "console_scripts": [
            "sdainbox=sda_orchestrator.inbox_consume:main",
            "sdaverified=sda_orchestrator.verified_consume:main",
            "sdacomplete=sda_orchestrator.complete_consume:main",
        ]
    },
    platforms="any",
    classifiers=[  # Optional
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        "Development Status :: 4 - Beta",
        # Indicate who your project is intended for
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        # Pick your license as you wish
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.7",
    ],
    install_requires=["asyncpg", "psycopg2", "amqpstorm", "aiohttp-jinja2", "jinja2", "aiohttp"],
    extras_require={
        "test": ["coverage", "coveralls", "pytest", "pytest-cov", "tox"],
    },
)

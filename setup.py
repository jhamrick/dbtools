#!/usr/bin/env python

from distutils.core import setup
import shutil

shutil.copy("README.md", "README")

setup(
    name='dbtools',
    version='0.03',
    description='Lightweight SQLite interface',
    author='Jessica B. Hamrick',
    author_email='jhamrick@berkeley.edu',
    url='https://github.com/jhamrick/dbtools',
    packages=['dbtools'],
    keywords='sqlite pandas dataframe',
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: SQL",
        "Topic :: Database :: Front-Ends",
        "Topic :: Utilities",
    ],
    install_requires=[
        'pandas',
        'numpy'
    ]
)

from setuptools import setup

setup(
    name='aiopipes',
    version='0.3.1',
    packages=['aiopipes'],
    url='https://github.com/orf/aio-pipes',
    license='',
    author='Orf',
    author_email='tom@tomforb.es',
    description='This module provides a set of pipe-like data structures for use with asyncio-based applications. See the GitHub repo for examples',
    install_requires=["asyncio", "decorator"],
)

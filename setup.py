# setup.py

from setuptools import setup, find_packages

setup(
    name='flow-lang',
    version='1.0.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'click',
        'lark',
        'pandas',
        'sqlalchemy',
        'psycopg2-binary',
    ],
    entry_points={
        'console_scripts': [
            'flow = src.cli:cli',
        ],
    },
)
from distutils.core import setup
from setuptools import find_packages


required = [
    "psycopg2",
    "SQLAlchemy==0.9.4",
    "db.py",
    "boto",
    "pandas",
]

setup(
    name="metrics_utils",
    version="0.4.4",
    author="Manuel Garrido, Richard Fernandez, Luigi Patruno, Attila Maczak",
    author_email="dev@namely.com",
    url="https://github.com/namely/metrics_utils",
    license="",
    packages=find_packages(),
    install_requires=[
       "psycopg2",
       "SQLAlchemy==0.9.4",
       "db.py",
       "boto",
       "pandas",
       "pyyaml",
       "pymssql"
    ],
    package_dir={"metrics_utils": "metrics_utils"},
    description="Metrics script utils",
    long_description=open("README.md").read(),
    classifiers=[
        # Maturity
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',
        # Versions supported
        'Programming Language :: Python :: 2.7',
    ],

)

from setuptools import find_packages
from setuptools import setup

NAME = "server"
VERSION = "1.0.0"
# To install the library, run the following
#
# python setup.py install
#
# prerequisite: setuptools
# http://pypi.python.org/pypi/setuptools

REQUIRES = [
    "connexion[swagger-ui]==2.14.1",
    "dnspython==2.1.0",
    "python_dateutil==2.8.1",
    "minio==7.0.3",
    "flask==2.2.5",
    "flask-cors==3.0.10",
    "python-magic==0.4.24",
    "pandas==1.2.4",
    "tika==1.24",
    "pymongo==3.11.3",
    "Werkzeug>=2.3.3",
    "openpyxl==3.0.7",
    "pytz==2021.1",
    "validators==0.18.2",
    "python-jose[cryptography]",
    "selenium>=4.10.0",
    "pika==1.2.0",
    "dicttoxml==1.7.4",
    "sendgrid==6.8.1",
    "xmltodict==0.12.0",
    "webdriver-manager==3.4.2",
    "natsort==7.1.1",
    "openapi-spec-validator==0.4.0",
    "openapi-schema-validator<0.3.0,>=0.2.0",
]

setup(
    name=NAME,
    version=VERSION,
    description="NLM Service API",
    author_email="info@nlmatics.com",
    url="",
    scripts=["re_extract.py", "re_ingest.py", "update.sh"],
    keywords=["Swagger", "NLM Service API"],
    install_requires=REQUIRES,
    packages=find_packages(),
    package_data={"": ["swagger/swagger.yaml"]},
    include_package_data=True,
    entry_points={"console_scripts": ["server=server.__main__:main"]},
    long_description="""\
    API specification for nlm-service
    """,
)

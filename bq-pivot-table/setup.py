
import setuptools

REQUIRED_PACKAGES = ['apache-beam[gcp]']
PACKAGE_NAME = 'bq-pivot-table'
PACKAGE_VERSION = '0.0.1'

setuptools.setup(
    name = PACKAGE_NAME,
    version = PACKAGE_VERSION,
    description = 'Write to Big Query via Dataflow w/ dynamic pivoted schema',
    author = 'Mark Porath',
    author_email = 'm.rath.oh@gmail.com',
    install_requires = REQUIRED_PACKAGES,
    packages = setuptools.find_packages(),
)

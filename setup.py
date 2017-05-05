from distutils.util import convert_path
from setuptools import find_packages
from setuptools import setup

ns = {}
version_path = convert_path('bigquery/version.py')
with open(version_path) as version_file:
    exec(version_file.read(), ns)

setup_args = dict(
    name='BigQuery-Python',
    description='Simple Python client for interacting with Google BigQuery.',
    url='https://github.com/tylertreat/BigQuery-Python',
    version=ns['__version__'],
    license='Apache',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'google-api-python-client',
        'httplib2',
        'python-dateutil'
    ],
    author='Tyler Treat',
    author_email='ttreat31@gmail.com',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],
)

if __name__ == '__main__':
    setup(**setup_args)

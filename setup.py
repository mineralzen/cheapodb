from setuptools import setup

install_requires = [
    'boto3==1.9.169'
]

setup(
    name='cheapodb',
    version='0.0.1',
    packages=['cheapodb'],
    install_requires=install_requires,
    url='https://github.com/mineralzen/cheapodb',
    license='MIT',
    author='Cole Howard',
    author_email='cole@mineralzen.com',
    description='Opinionated implementation of AWS Glue'
)

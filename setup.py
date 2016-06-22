from setuptools import setup, find_packages

setup(
    name='kafka-rest-client',
    version='0.1',
    packages=find_packages(),
    url='https://github.com/bergundy/python-kafka-rest-client',
    author='Roey Berman',
    author_email='roey.berman@gmail.com',
    description='Convenience wrapper for Kafka REST proxy',
    install_requires=['requests']
)

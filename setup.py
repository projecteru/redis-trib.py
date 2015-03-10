from setuptools import setup

_URI = 'https://github.com/neuront/redis-trib.py'

setup(
    name='redis-trib',
    version='0.2.0',
    author='Neuron Teckid',
    author_email='lene13@gmail.com',
    license='MIT',
    keywords='Redis Cluster',
    url=_URI,
    description='Redis Cluster tools in Python2',
    packages=['redistrib'],
    long_description='Visit ' + _URI + ' for details please.',
    install_requires=[
        'hiredis',
        'retrying',
    ],
    zip_safe=False,
    entry_points=dict(
        console_scripts=[
            'redis-trib.py=redistrib.console:main',
        ],
    ),
)

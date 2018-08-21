import redistrib
from setuptools import setup

requirements = [
    'click==6.7',
    'hiredis==0.2.0',
    'retrying==1.3.3',
    'six==1.11.0',
    'Werkzeug==0.14.1',
]

setup(
    name='redis-trib',
    version=redistrib.__version__,
    author='Neuron Teckid',
    author_email='lene13@gmail.com',
    license='MIT',
    keywords='Redis Cluster',
    url=redistrib.REPO,
    description='Redis Cluster tools in Python',
    packages=['redistrib'],
    long_description='Visit ' + redistrib.REPO + ' for details please.',
    install_requires=requirements,
    zip_safe=False,
    entry_points=dict(
        console_scripts=[
            'redis-trib.py=redistrib.console:main',
        ], ),
)

from setuptools import setup
import redistrib


with open('requirements.txt', 'r') as reqin:
    requirements = [r.strip() for r in reqin.readlines()]

setup(
    name='redis-trib',
    version=redistrib.__version__,
    author='Neuron Teckid',
    author_email='lene13@gmail.com',
    license='MIT',
    keywords='Redis Cluster',
    url=redistrib.REPO,
    description='Redis Cluster tools in Python2',
    packages=['redistrib'],
    long_description='Visit ' + redistrib.REPO + ' for details please.',
    install_requires=requirements,
    zip_safe=False,
    entry_points=dict(
        console_scripts=[
            'redis-trib.py=redistrib.console:main',
        ],
    ),
)

from setuptools import setup

def readme():
    with open('README.md') as f:
        return f.read()

setup(
    name='t2db_daemon',
    version='0.1.0',
    description='Worker manager for t2db',
    long_description = readme(),
    classifiers=[
      'Programming Language :: Python :: 3.2',
    ],
    url='http://github.com/ptorrest/t2db_daemon',
    author='Pablo Torres',
    author_email='pablo.torres@deri.org',
    license='GNU',
    packages=['t2db_daemon', 't2db_daemon.tests'],
    install_requires=[
	't2db_buffer >= 0.1.2',
        't2db_objects >= 0.5.4',
	    't2db_worker >= 0.3.0',
    ],
    entry_points = {
        'console_scripts':[
            't2db_daemon = t2db_daemon.run:main',
	    't2db_daemon-d = t2db_daemon.run:main_daemon',
        ]
    },
    test_suite='t2db_daemon.tests',
    zip_safe = False
)

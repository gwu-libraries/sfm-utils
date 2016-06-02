from setuptools import setup

setup(
    name='sfmutils',
    version='0.4.1',
    url='https://github.com/gwu-libraries/sfm-utils',
    author='Justin Littman',
    author_email='justinlittman@gmail.com',
    packages=['sfmutils'],
    description="Utilities to support Social Feed Manager.",
    platforms=['POSIX'],
    test_suite='tests',
    scripts=['sfmutils/stream_consumer.py', 'sfmutils/find_warcs.py'],
    install_requires=['requests>=2.9.1',
                      'kombu>=3.0.33',
                      'supervisor>=3.2.0',
                      'urllib3>=1.12',
                      'warc>=0.2.1',
                      'petl>=1.1.0',
                      'openpyxl>=2.3.3',
                      'iso8601>=0.1.11'],
    tests_require=['mock>=1.3.0'],
    classifiers=[
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python :: 2.7',
        'Development Status :: 4 - Beta',
    ],
)

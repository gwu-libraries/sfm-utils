from setuptools import setup

setup(
    name='sfmutils',
    version='0.1.1',
    url='https://github.com/gwu-libraries/sfm-utils',
    author='Justin Littman',
    author_email='justinlittman@gmail.com',
    packages=['sfmutils'],
    description="Utilities to support Social Feed Manager.",
    platforms=['POSIX'],
    test_suite='tests',
    scripts=['sfmutils/stream_consumer.py'],
    install_requires=['requests>=2.7.0',
                      'kombu>=3.0.29',
                      'supervisor>=3.1.3',
                      'urllib3>=1.12',
                      'warc>=0.2.1'],
    tests_require=['mock>=1.3.0'],
    classifiers=[
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python :: 2.7',
        'Development Status :: 4 - Beta',
    ],
)

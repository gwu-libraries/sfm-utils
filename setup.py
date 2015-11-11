from setuptools import setup

setup(
    name='sfmutils',
    version='0.1.0',
    url='https://github.com/gwu-libraries/sfm-utils',
    author='Justin Littman',
    author_email='justinlittman@gmail.com',
    packages=['sfmutils'],
    # py_modules=['sfmutils',],
    description="Utilities to support Social Feed Manager.",
    platforms=['POSIX'],
    test_suite='tests',
    install_requires=['requests>=2.7.0', 'warcprox'],
    dependency_links=['git+https://github.com/gwu-libraries/warcprox.git@master#egg=warcprox'],
    classifiers=[
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python :: 2.7',
        'Development Status :: 4 - Beta',
    ],
)

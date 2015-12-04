# sfm-utils
Utilities for Social Feed Manager.

[![Build Status](https://travis-ci.org/gwu-libraries/sfm-utils.svg?branch=master)](https://travis-ci.org/gwu-libraries/sfm-utils)

Most significantly:

* BaseConsumer: Base class for consuming messages from Rabbit.
* BaseHarvester: Base class for a harvester, allowing harvesting from a queue or from a file.
* StreamConsumer: A consumer intended to control stream harvests using Supervisor.
* warced: An entry/exit wrapper for warcprox.

## Installing
    git clone https://github.com/gwu-libraries/sfm-utils.git
    cd sfm-utils
    pip install -r requirements.txt

Alternatively, it can be included as a dependency by adding the following to `requirements.txt`:

    #Replace 0.1.0 with the version that you want.
    git+https://github.com/gwu-libraries/sfm-utils.git@0.1.0#egg=sfmutils
    #If using any of the consumers or harvesters
    librabbitmq==1.6.1
    #If using BaseHarvester    
    git+https://github.com/gwu-libraries/warcprox.git@master#egg=warcprox-gwu


## Unit tests

    python -m unittest discover

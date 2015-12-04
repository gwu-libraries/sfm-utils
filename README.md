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

## Unit tests

    python -m unittest discover
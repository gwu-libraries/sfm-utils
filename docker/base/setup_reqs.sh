#!/usr/bin/env bash

echo "Upgrading common.txt"
pip install -r requirements/common.txt --upgrade

if [ $SFM_REQS != "release" ]; then
    echo "Uninstalling requirements.txt."
    pip uninstall -r requirements/release.txt -y
    echo "Installing $SFM_REQS.txt"
    pip install -r requirements/$SFM_REQS.txt
fi

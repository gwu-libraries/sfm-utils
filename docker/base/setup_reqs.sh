#!/usr/bin/env bash

groupadd -r sfm --gid=$SFM_GID && useradd -r -m -g sfm --uid=$SFM_UID sfm

export CONTAINER_DIR=/sfm-data/containers/$HOSTNAME
if [ ! -d $CONTAINER_DIR ]; then
    echo "Creating container directory"
    mkdir -p $CONTAINER_DIR
    chown sfm:sfm $CONTAINER_DIR
fi

echo "Upgrading common.txt"
pip install -r requirements/common.txt --upgrade

if [ $SFM_REQS != "release" ]; then
    echo "Uninstalling requirements.txt."
    pip uninstall -r requirements/release.txt -y
    echo "Installing $SFM_REQS.txt"
    pip install -r requirements/$SFM_REQS.txt
fi

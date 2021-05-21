#!/usr/bin/env bash

groupadd -r sfm --gid=$SFM_GID && useradd -r -m -g sfm --uid=$SFM_UID sfm

export CONTAINER_DIR=/sfm-containers-data/containers/$HOSTNAME
if [ ! -d $CONTAINER_DIR ]; then
    echo "Creating container directory"
    mkdir -p $CONTAINER_DIR
    chown sfm:sfm $CONTAINER_DIR
fi

if [ ${SFM_UPGRADE_REQS=True} != "False" ]; then
    echo "Upgrading common.txt"
    pip install -r requirements/common.txt --upgrade
fi

if [ $SFM_REQS != "release" ]; then
    echo "Uninstalling requirements.txt."
    pip uninstall -r requirements/release.txt -y
    echo "Installing $SFM_REQS.txt"
    pip install -r requirements/$SFM_REQS.txt
fi

#!/bin/bash
# command : update_yaml_version.sh <src_file> <dst_file>
# version comes from CI_COMMIT_TAG, otherwise, use latest

NEW_VERSION="${CI_COMMIT_TAG:=vlatest}"
NEW_VERSION="${NEW_VERSION:1}" # remove v from v0.2.2
SRC_FILE="$1"
DST_FILE="$2"

echo "creating yaml plugins for tag-${NEW_VERSION} ...."

echo "updating $SRC_FILE"
mkdir -p "$(dirname "${DST_FILE}")"
touch "${DST_FILE}"
sed "s/{{.version}}/${NEW_VERSION}/" $SRC_FILE  >> "${DST_FILE}"

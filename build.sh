#! /bin/bash

set -x

# Pull the cache
dvc pull

# Checkout the dvc project
dvc checkout

# Run the project
dvc repro

# Update the remote cache
dvc push

# Update dvc.lock in git repo
git add dvc.lock
git commit -am "Update dvc.lock"
git remote add df ${GIT_REPO}
git push df ${GIT_BRANCH}

# Send output
cp dvc.lock /output

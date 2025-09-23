#!/bin/bash

set -eu

# Request password from the user
echo "Provide the password to your secrets.yaml file:"
read -s password

# Decrypt github oauth token and export it
GITHUB_TOKEN=$(openssl aes-256-cbc -d -a -pbkdf2 -in cluster/secrets.yaml.enc -pass pass:$password | python3 -c "import sys, yaml; print(yaml.safe_load(sys.stdin)['GITHUB_TOKEN'])")

# Remove the password from the environment
unset password

export GITHUB_TOKEN
echo Token successfully loaded!

#!/bin/bash

# Set error handling
set -euo pipefail
# Set environment variables
source cluster/set_env.sh
# Source the common script for argument parsing
source cluster/common.sh

# Check if GITHUB_TOKEN is set. If not, attempt to load it.
if [ -z "${GITHUB_TOKEN:-}" ]
then
    echo "GITHUB_TOKEN is not set. Attempting to load from secrets..."
    source cluster/set_secrets.sh
fi

# Check if necessary environment variables are set
for var in GIT_REPO_NAME SCRATCH GIT_COMMIT
do
    if [ -z "${!var:-}" ]
    then
        echo "$var is not set. Please set this variable and try again."
        exit 1
    fi
done

# Get current timestamp
TIMESTAMP=`date +%Y%m%d%H%M%S`

# Define the config file path
CONFIG_FILE="params.yaml"

# Check if yq is installed
if ! command -v yq &> /dev/null
then
    echo "yq is not installed. Please install it to parse YAML files."
    exit 1
fi

# We're only interested in the combined_inference stage
STAGE="combined_inference"

# Get all steps (job names) for the combined_inference stage
STEPS=$(yq -r ".stages.${STAGE} | keys | .[]" ${CONFIG_FILE})

# Loop through each step in the combined_inference stage
for step in $STEPS; do
    echo "Processing job: $step"

    # Get GPU count from model config
    GPU_COUNT=$(yq e ".stages.${STAGE}.${step}.gpus" ${CONFIG_FILE})
    TIME_LIMIT=$(yq e ".stages.${STAGE}.${step}.time_limit" ${CONFIG_FILE})

    # Get memory requirements - defaulting to 48g if not specified
    MEM="48g"

    # Submit the job
    sbatch \
        --export=GITHUB_TOKEN,HF_TOKEN \
        --job-name=${step} \
        --nodes=1 \
        --ntasks=1 \
        --cpus-per-task=8 \
        --ntasks-per-node=1 \
        --mem=${MEM} \
        --partition=long \
        --gres=gpu:${GPU_COUNT} \
        --time=${TIME_LIMIT} \
        --output=${SCRATCH}/${GIT_REPO_NAME}/logs/${TIMESTAMP}_job_%j_%N_${GIT_COMMIT}.log \
        cluster/runner.sh --stage ${STAGE}@${step}
done

# Check for successful job submission
if [ $? -eq 0 ]
then
    echo "Job submitted successfully."
else
    echo "Failed to submit job."
    exit 1
fi

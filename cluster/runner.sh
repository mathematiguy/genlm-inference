#!/bin/bash
# Set error handling
set -exuo pipefail

# Source environment variables
source cluster/set_env.sh

# Source the common script for argument parsing
source cluster/common.sh

# Parse arguments
parse_arguments "$@"

# Load singularity module
module load singularity
module load python/3.10
module load cuda/12.4.1/cudnn

# Function to check if a command exists
function command_exists() {
  command -v $1 >/dev/null 2>&1
}

# Function to check if an environment variable is set
function env_var_exists() {
    if [ -z "${!1:-}" ]; then
        echo "Error: Environment variable $1 is not set."
        exit 1
    fi
}

# Function to check if an environment variable is set (with warning only)
function env_var_exists_warn() {
    if [ -z "${!1:-}" ]; then
        echo "Warning: Environment variable $1 is not set."
        return 1
    fi
    return 0
}

# Function to check if a file or directory exists
function path_exists() {
  if [[ ! -e $1 ]]; then
    echo "Error: $1 does not exist."
    exit 1
  fi
}

# Check if required commands are available
command_exists git || { echo "git command not found. Please install git and try again."; exit 1; }
command_exists singularity || { echo "singularity command not found. Please load the singularity module and try again."; exit 1; }

# Check if required environment variables are set
env_var_exists GIT_REPO_NAME
env_var_exists GIT_BRANCH
env_var_exists SLURM_TMPDIR
env_var_exists GITHUB_TOKEN
env_var_exists SCRATCH
env_var_exists ARCHIVE
env_var_exists CUDA_HOME
env_var_exists HF_TOKEN

# Check for RUN_GROUP_ID (warn if missing, but don't fail)
if env_var_exists_warn RUN_GROUP_ID; then
    echo "RUN_GROUP_ID is set to: ${RUN_GROUP_ID}"
else
    echo "RUN_GROUP_ID not provided - build.sh will generate a fallback group name"
    export RUN_GROUP_ID="unknown"
fi

# Turn off shell debugging
set +x

# Get the Git Repo URL
export GIT_REPO_URL=`git config --get remote.origin.url`

# Embed the token into the GIT_REPO_URL
export GIT_TOKEN_URL=$(echo ${GIT_REPO_URL} | sed 's|git@github.com:|https://github.com/|' | sed 's|.git$|.git|' | sed "s|://|://oauth2:${GITHUB_TOKEN}@|")

# Turn on shell debugging
set -x

# Check if the singularity image file exists before trying to copy it
path_exists "container.sif"

# Clone the git repository
echo git clone -b ${GIT_BRANCH} --single-branch ${GIT_REPO_URL} ${SLURM_TMPDIR}/${GIT_REPO_NAME}
set +x
git clone -b ${GIT_BRANCH} --single-branch ${GIT_TOKEN_URL} ${SLURM_TMPDIR}/${GIT_REPO_NAME}
set -x

# Check if the git clone was successful
path_exists "${SLURM_TMPDIR}/${GIT_REPO_NAME}"

# Copy the singularity container to the cloned repo
cp container.sif ${SLURM_TMPDIR}/${GIT_REPO_NAME}

# Move working directory to $SLURM_TMPDIR
cd ${SLURM_TMPDIR}/${GIT_REPO_NAME}

# Modify the .gitmodules file to replace git URLs with the token URL
sed -i -E "s|git@github.com:([^ ]*)|https://oauth2:${GITHUB_TOKEN}@github.com/\1|g" .gitmodules

# Clone submodules
git submodule init && git submodule update

# Set the dvc cache directory
export DVC_CACHE_DIR=$SCRATCH/dvc-cache

# Build the singularity command dynamically using an array
SINGULARITY_CMD=(
    singularity exec
    --nv
    -B "$(pwd)":/code
    --pwd /code
    -B ${DVC_CACHE_DIR}
    -B submodules/surya:/pkg/surya
    -B submodules/reo-toolkit:/pkg/reo-toolkit
    -B /network/weights
    -B /network/scratch/c/caleb.moses/hf_cache
    -B /cvmfs
    --env PYTHONPATH=/pkg/code:/pkg/surya:/pkg/reo-toolkit:/code:
    --env HF_HOME=/network/scratch/c/caleb.moses/hf_cache
    --env MPLCONFIGDIR=/code/.matplotlib
    --env NUMBA_CACHE_DIR=/code/.numba
    --env HF_TOKEN=${HF_TOKEN}
    --env RUN_GROUP_ID=${RUN_GROUP_ID}
    --env LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu:/opt/conda/lib:/opt/conda/compiler_compat
    container.sif
    bash cluster/build.sh
)

# Include stage argument if provided
if [ -n "$STAGE" ]; then
    SINGULARITY_CMD+=(--stage "$STAGE")
fi

# Debug: Print the RUN_GROUP_ID being passed to singularity
echo "Passing RUN_GROUP_ID to singularity: ${RUN_GROUP_ID}"

# Execute the singularity command
"${SINGULARITY_CMD[@]}"

# Check if the singularity execution was successful
if [[ $? -ne 0 ]]; then
  echo "Error: Singularity execution failed."
  exit 1
fi
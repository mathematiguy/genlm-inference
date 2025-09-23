#!/bin/bash

# Show commands (-x). Removed the -e flag.
set -x

# Capture start time
start_time=$(date +%s)

# Source environment variables
source cluster/set_env.sh

# Create a new branch for this job
export JOB_NAME="job_${SLURM_JOB_ID}"
git checkout -b ${JOB_NAME} || { echo "Failed to create new branch. Exiting."; exit 1; }

# Error and exit handling
trap 'handle_exit' EXIT

handle_exit() {
  local exit_code=$?
  if [ $exit_code -eq 0 ]; then
    echo "Job succeeded."
  else
    echo "Job failed or was terminated. Please check the branch ${JOB_NAME} for details."
  fi

  # Merge and cleanup job branch
  git checkout main
  git diff --quiet ${JOB_NAME} main || {
    git merge --no-ff ${JOB_NAME} || git push origin ${JOB_NAME} && echo "Failed to merge. Please merge manually."
    git branch -d ${JOB_NAME} || echo "Failed to delete the branch. Please delete manually."
  }

  # Calculate and print the elapsed time
  end_time=$(date +%s)
  elapsed=$((end_time - start_time))
  echo "Total duration: $elapsed seconds."

  exit $exit_code
}

# Set the dvc cache directory
dvc cache dir --local ${DVC_CACHE_DIR}

# Checkout dvc.lock
dvc checkout || true

# Run the job
bash run.sh

# Print duration
end_time=$(date +%s)
elapsed=$(( end_time - start_time ))
hours=$(( elapsed / 3600 ))
minutes=$(( (elapsed % 3600) / 60 ))
seconds=$(( elapsed % 60 ))

printf "Time taken (run.sh): %02d:%02d:%02d (hh:mm:ss).\n" $hours $minutes $seconds

# Print completion time
echo ${JOB_NAME} completed at `date +"%m-%d-%Y %H:%M:%S"`

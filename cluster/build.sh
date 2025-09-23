#!/bin/bash
# cluster/build.sh
# Show commands (-x). Removed the -e flag.
set -x
# Capture start time
start_time=$(date +%s)
# Source environment variables
source cluster/set_env.sh
# Source the common script for argument parsing
source cluster/common.sh
# Parse arguments
parse_arguments "$@"

# Calculate branch name BEFORE creating it
if [[ -n "${RUN_GROUP_ID:-}" ]]; then
    # Use provided run group ID
    GROUP_PART="${RUN_GROUP_ID}"
else
    # Fallback to date-based group if no explicit run group
    GROUP_PART="run_$(date +%m%d_%H%M)"
fi

if [[ -n "${STAGE:-}" ]]; then
    # Clean up stage name for git branch (replace @ and special chars)
    STAGE_CLEAN=$(echo "$STAGE" | sed 's/@/_/g' | sed 's/[^a-zA-Z0-9_-]/_/g')
    export JOB_NAME="${GROUP_PART}_${STAGE_CLEAN}"
else
    # Fallback if no stage specified
    export JOB_NAME="${GROUP_PART}_job_${SLURM_JOB_ID}"
fi

echo "Creating git branch: $JOB_NAME (Run Group: ${RUN_GROUP_ID:-unknown})"

# Create a new branch for this job
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
  git checkout ${GIT_BRANCH}
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

# Main script logic
if [ -n "$STAGE" ]; then
    bash run.sh --stage "$STAGE"
else
    bash run.sh
fi

# Print duration
end_time=$(date +%s)
elapsed=$(( end_time - start_time ))
hours=$(( elapsed / 3600 ))
minutes=$(( (elapsed % 3600) / 60 ))
seconds=$(( elapsed % 60 ))
printf "Time taken (run.sh): %02d:%02d:%02d (hh:mm:ss).\n" $hours $minutes $seconds

# Print completion time
echo ${JOB_NAME} completed at `date +"%m-%d-%Y %H:%M:%S"`
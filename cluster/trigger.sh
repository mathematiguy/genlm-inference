#!/bin/bash
# Enhanced trigger.sh - Submit individual jobs with configurable parameters
# Can be called directly or by the Python pipeline submitter

set -euo pipefail

#==============================================================================
# DEFAULT VALUES
#==============================================================================

DEFAULT_GPUS=1
DEFAULT_TIME="00:30:00"
DEFAULT_MEMORY="48g"
DEFAULT_CPUS=8
DEFAULT_PARTITION="long"
DEFAULT_NODES=1
DEFAULT_NTASKS=1
DEFAULT_NTASKS_PER_NODE=1

#==============================================================================
# LOGGING UTILITIES
#==============================================================================

# Create log directory structure
setup_log_directories() {
    local base_log_dir="${SCRATCH}/${GIT_REPO_NAME}/logs"
    local job_log_dir="${base_log_dir}/jobs"
    local status_dir="${base_log_dir}/status"
    local archive_dir="${base_log_dir}/archive"

    mkdir -p "$job_log_dir" "$status_dir" "$archive_dir"

    # Create symlink to latest logs
    local latest_dir="${base_log_dir}/latest"
    if [[ ! -L "$latest_dir" ]]; then
        ln -sf "$job_log_dir" "$latest_dir"
    fi

    echo "$job_log_dir"
}

# Create job metadata file
create_job_metadata() {
    local job_id="$1"
    local stage="$2"
    local log_file="$3"
    local metadata_file="${SCRATCH}/${GIT_REPO_NAME}/logs/status/${job_id}.meta"

    cat > "$metadata_file" << EOF
JOB_ID=$job_id
STAGE=$stage
STAGE_NAME=${stage%@*}
STEP_NAME=${stage#*@}
SUBMIT_TIME="$(date '+%Y-%m-%d %H:%M:%S')"
SUBMIT_TIMESTAMP=$(date +%s)
LOG_FILE=$log_file
STATUS=SUBMITTED
USER=$(whoami)
PARTITION=$PARTITION
NODES=$NODES
CPUS=$CPUS
MEMORY=$MEMORY
GPUS=$GPUS
TIME_LIMIT=$TIME_LIMIT
GIT_COMMIT=$GIT_COMMIT
DEPENDENCY=${DEPENDENCY:-none}
HOSTNAME=$(hostname)
WORKING_DIR=$(pwd)
RUN_GROUP_ID=${RUN_GROUP_ID:-unknown}
EOF

    echo "Job metadata created: $metadata_file"
}

#==============================================================================
# ARGUMENT PARSING
#==============================================================================

show_help() {
    cat << EOF
Usage: $0 [OPTIONS] --stage STAGE@STEP

Submit a single job to SLURM with configurable parameters.

REQUIRED:
    --stage STAGE@STEP    Stage and step in format 'stage_name@step_name'

OPTIONS:
    --gpus N             Number of GPUs (default: $DEFAULT_GPUS)
    --time TIME          Time limit in HH:MM:SS format (default: $DEFAULT_TIME)
    --memory MEM         Memory limit (default: $DEFAULT_MEMORY)
    --cpus N             CPUs per task (default: $DEFAULT_CPUS)
    --partition PART     SLURM partition (default: $DEFAULT_PARTITION)
    --nodes N            Number of nodes (default: $DEFAULT_NODES)
    --ntasks N           Number of tasks (default: $DEFAULT_NTASKS)
    --ntasks-per-node N  Tasks per node (default: $DEFAULT_NTASKS_PER_NODE)
    --dependency DEP     SLURM dependency string (e.g., --dependency=afterok:12345)
    --job-name NAME      Custom job name (default: based on stage@step)
    --log-prefix PREFIX  Custom log filename prefix
    --run-group GROUP    Run group identifier for related jobs
    -h, --help           Show this help message

EXAMPLES:
    $0 --stage surya_inference@step1
    $0 --stage combined_inference@step2 --gpus 2 --time 01:00:00
    $0 --stage evaluation@eval1 --dependency afterok:12345:12346
    $0 --stage multi_node@job --nodes 2 --ntasks 4 --ntasks-per-node 2

LOGGING & TRACKING:
    Logs are stored in: \${SCRATCH}/\${GIT_REPO_NAME}/logs/jobs/
    Track jobs with: cluster/track_jobs.sh [command]

    Quick commands after job submission:
        cluster/track_jobs.sh list          # List recent jobs
        cluster/track_jobs.sh active        # Show running jobs
        cluster/track_jobs.sh logs JOB_ID   # View job logs
        cluster/track_jobs.sh tail JOB_ID   # Tail job logs
EOF
}

# Initialize variables
STAGE=""
GPUS=$DEFAULT_GPUS
TIME_LIMIT=$DEFAULT_TIME
MEMORY=$DEFAULT_MEMORY
CPUS=$DEFAULT_CPUS
PARTITION=$DEFAULT_PARTITION
NODES=$DEFAULT_NODES
NTASKS=$DEFAULT_NTASKS
NTASKS_PER_NODE=$DEFAULT_NTASKS_PER_NODE
DEPENDENCY=""
JOB_NAME=""
LOG_PREFIX=""
RUN_GROUP_ID=""  # Initialize as empty

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --stage)
            STAGE="$2"
            shift 2
            ;;
        --gpus)
            GPUS="$2"
            shift 2
            ;;
        --time)
            TIME_LIMIT="$2"
            shift 2
            ;;
        --memory)
            MEMORY="$2"
            shift 2
            ;;
        --cpus)
            CPUS="$2"
            shift 2
            ;;
        --partition)
            PARTITION="$2"
            shift 2
            ;;
        --nodes)
            NODES="$2"
            shift 2
            ;;
        --ntasks)
            NTASKS="$2"
            shift 2
            ;;
        --ntasks-per-node)
            NTASKS_PER_NODE="$2"
            shift 2
            ;;
        --dependency)
            DEPENDENCY="$2"
            shift 2
            ;;
        --job-name)
            JOB_NAME="$2"
            shift 2
            ;;
        --log-prefix)
            LOG_PREFIX="$2"
            shift 2
            ;;
        --run-group)
            RUN_GROUP_ID="$2"
            shift 2
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            # Check if it's the old-style positional argument (for backward compatibility)
            if [[ -z "$STAGE" && "$1" =~ ^[a-zA-Z_]+@[a-zA-Z0-9_-]+$ ]]; then
                STAGE="$1"
                shift
            else
                echo "Unknown option: $1"
                show_help
                exit 1
            fi
            ;;
    esac
done

# Validate required arguments
if [[ -z "$STAGE" ]]; then
    echo "ERROR: --stage is required"
    show_help
    exit 1
fi

# Validate stage format
if [[ ! "$STAGE" =~ ^[a-zA-Z_]+@[a-zA-Z0-9_/.-]+$ ]]; then
    echo "ERROR: Stage must be in format 'stage_name@step_name'"
    exit 1
fi

#==============================================================================
# ENVIRONMENT SETUP
#==============================================================================

# Set environment variables
source cluster/set_env.sh

# Source the common script for argument parsing (if it exists)
if [[ -f cluster/common.sh ]]; then
    source cluster/common.sh
fi

# CRITICAL: Export RUN_GROUP_ID as environment variable if it was provided
if [[ -n "$RUN_GROUP_ID" ]]; then
    export RUN_GROUP_ID
    echo "Set RUN_GROUP_ID environment variable: $RUN_GROUP_ID"
else
    echo "No RUN_GROUP_ID provided - jobs will use fallback naming"
fi

# Handle GitHub token - either from environment or try to load from secrets
if [[ -z "${GITHUB_TOKEN:-}" ]]; then
    echo "GITHUB_TOKEN is not set in environment."

    # Check if we're running interactively (can prompt for password)
    if [[ -t 0 ]] && [[ -f cluster/set_secrets.sh ]]; then
        echo "Attempting to load from secrets interactively..."
        source cluster/set_secrets.sh
        if [[ -n "${GITHUB_TOKEN:-}" ]]; then
            echo "Successfully loaded GITHUB_TOKEN from secrets"
        fi
    else
        echo "WARNING: Running non-interactively and no GITHUB_TOKEN provided"
        echo "Jobs may fail if they require GitHub access"
        # Continue anyway - some jobs might not need the token
    fi
else
    echo "GITHUB_TOKEN provided via environment"
fi

# Handle HF_TOKEN similarly if needed
if [[ -z "${HF_TOKEN:-}" ]]; then
    echo "INFO: HF_TOKEN not set (may be needed for some models)"
else
    echo "HF_TOKEN provided via environment"
fi

# Check if necessary environment variables are set
for var in GIT_REPO_NAME SCRATCH GIT_COMMIT; do
    if [[ -z "${!var:-}" ]]; then
        echo "ERROR: $var is not set. Please set this variable and try again."
        exit 1
    fi
done

#==============================================================================
# LOGGING SETUP
#==============================================================================

# Setup log directories
LOG_DIR=$(setup_log_directories)

# Get current timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Extract stage and step names for job naming and logging
STAGE_NAME="${STAGE%@*}"
STEP_NAME="${STAGE#*@}"

# Set job name if not provided
if [[ -z "$JOB_NAME" ]]; then
    JOB_NAME="${STEP_NAME}_${STAGE_NAME}"
fi

# Generate log filename with custom prefix if provided
if [[ -n "$LOG_PREFIX" ]]; then
    LOG_FILENAME="${LOG_PREFIX}_${TIMESTAMP}_${STAGE_NAME}_${STEP_NAME}_%j.log"
else
    LOG_FILENAME="${TIMESTAMP}_${STAGE_NAME}_${STEP_NAME}_%j.log"
fi

LOG_PATH="${LOG_DIR}/${LOG_FILENAME}"

#==============================================================================
# JOB SUBMISSION
#==============================================================================

# Build sbatch command
SBATCH_CMD=(
    sbatch
    --job-name="$JOB_NAME"
    --nodes="$NODES"
    --ntasks="$NTASKS"
    --cpus-per-task="$CPUS"
    --ntasks-per-node="$NTASKS_PER_NODE"
    --mem="$MEMORY"
    --partition="$PARTITION"
    --time="$TIME_LIMIT"
    --output="$LOG_PATH"
)

# Build the export list dynamically based on what's available
export_vars=()
[[ -n "${GITHUB_TOKEN:-}" ]] && export_vars+=(GITHUB_TOKEN)
[[ -n "${HF_TOKEN:-}" ]] && export_vars+=(HF_TOKEN)
[[ -n "${RUN_GROUP_ID:-}" ]] && export_vars+=(RUN_GROUP_ID)

if [[ ${#export_vars[@]} -gt 0 ]]; then
    # Join array elements with commas
    export_string=$(IFS=','; echo "${export_vars[*]}")
    SBATCH_CMD+=(--export="$export_string")
    echo "Exporting environment variables to SLURM job: $export_string"
else
    echo "No environment variables to export to SLURM job"
fi

# Add GPU specification
if [[ "$GPUS" -gt 0 ]]; then
    SBATCH_CMD+=(--gres=gpu:"$GPUS")
fi

# Add dependency if specified
if [[ -n "$DEPENDENCY" ]]; then
    SBATCH_CMD+=(--dependency="$DEPENDENCY")
fi

# Add the runner script and stage
SBATCH_CMD+=(cluster/runner.sh --stage "$STAGE")

# Print what we're about to execute (for debugging)
echo "Submitting job: $STAGE"
echo "=================="
echo "  Job name: $JOB_NAME"
echo "  GPUs: $GPUS"
echo "  Time limit: $TIME_LIMIT"
echo "  Memory: $MEMORY"
echo "  CPUs: $CPUS"
echo "  Partition: $PARTITION"
echo "  Nodes: $NODES"
echo "  Tasks: $NTASKS"
echo "  Tasks per node: $NTASKS_PER_NODE"
if [[ -n "$DEPENDENCY" ]]; then
    echo "  Dependency: $DEPENDENCY"
fi
if [[ -n "$RUN_GROUP_ID" ]]; then
    echo "  Run Group ID: $RUN_GROUP_ID"
fi
echo "  Log file: $LOG_PATH"
echo ""

# Submit the job and capture the job ID
echo "Executing: ${SBATCH_CMD[*]}"
JOB_OUTPUT=$("${SBATCH_CMD[@]}" 2>&1)
SUBMIT_EXIT_CODE=$?

# Check for successful job submission
if [[ $SUBMIT_EXIT_CODE -eq 0 ]]; then
    # Extract job ID from sbatch output
    JOB_ID=$(echo "$JOB_OUTPUT" | grep -oE '[0-9]+' | head -1)

    if [[ -n "$JOB_ID" ]]; then
        echo "Job submitted successfully with ID: $JOB_ID"
        echo "$JOB_OUTPUT"

        # Create job metadata for tracking
        ACTUAL_LOG_PATH="${LOG_DIR}/${LOG_FILENAME//%j/$JOB_ID}"
        create_job_metadata "$JOB_ID" "$STAGE" "$ACTUAL_LOG_PATH"

        echo ""
        echo "Job Tracking Commands:"
        echo "====================="
        echo "  View this job:    cluster/track_jobs.sh status $JOB_ID"
        echo "  View logs:        cluster/track_jobs.sh logs $JOB_ID"
        echo "  Tail logs:        cluster/track_jobs.sh tail $JOB_ID"
        echo "  List all jobs:    cluster/track_jobs.sh list"
        echo "  Active jobs:      cluster/track_jobs.sh active"
        echo ""
        echo "Log file: $ACTUAL_LOG_PATH"
    else
        echo "Job submitted but could not extract job ID from output:"
        echo "$JOB_OUTPUT"
    fi
else
    echo "Failed to submit job:"
    echo "$JOB_OUTPUT"
    exit 1
fi
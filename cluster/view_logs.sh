#!/bin/bash
# Simple fast log viewer for SLURM jobs
# Usage: ./view_logs.sh [--skip-slurm]
set -euo pipefail

# Parse arguments
SKIP_SLURM=false
if [[ "${1:-}" == "--skip-slurm" ]]; then
    SKIP_SLURM=true
fi

# Try to source environment if available
if [[ -f cluster/set_env.sh ]]; then
    source cluster/set_env.sh
fi

# Set paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(dirname "$SCRIPT_DIR")"
if [[ -n "${SCRATCH:-}" && -n "${GIT_REPO_NAME:-}" ]]; then
    LOG_BASE_DIR="${SCRATCH}/${GIT_REPO_NAME}/logs"
else
    LOG_BASE_DIR="${BASE_DIR}/logs"
fi
STATUS_DIR="$LOG_BASE_DIR/status"

# Check if directory exists
if [[ ! -d "$STATUS_DIR" ]]; then
    echo "Error: Status directory not found: $STATUS_DIR"
    exit 1
fi

# Get all SLURM job statuses in one call (much faster than individual calls)
declare -A slurm_statuses
if [[ "$SKIP_SLURM" == "false" ]] && command -v squeue >/dev/null 2>&1; then
    echo "Fetching SLURM job statuses..." >&2
    while IFS=' ' read -r job_id status; do
        [[ -n "$job_id" && "$job_id" != "JOBID" ]] && slurm_statuses["$job_id"]="$status"
    done < <(timeout 15s squeue -h -o "%i %T" 2>/dev/null || true)
fi

# Print header
printf "%-8s %-25s %-12s %-19s %-8s %-8s\n" "JOB_ID" "STAGE" "STATUS" "SUBMITTED" "LOG" "SIZE"
echo "================================================================================="

# Process each job
for meta_file in "$STATUS_DIR"/*.meta; do
    [[ ! -f "$meta_file" ]] && continue
    
    # Extract key info using simple grep/awk
    job_id=$(grep "^JOB_ID=" "$meta_file" 2>/dev/null | cut -d'=' -f2 | tr -d '"' || echo "")
    stage_name=$(grep "^STAGE_NAME=" "$meta_file" 2>/dev/null | cut -d'=' -f2 | tr -d '"' || echo "unknown")
    step_name=$(grep "^STEP_NAME=" "$meta_file" 2>/dev/null | cut -d'=' -f2 | tr -d '"' || echo "unknown")
    submit_time=$(grep "^SUBMIT_TIME=" "$meta_file" 2>/dev/null | cut -d'=' -f2 | tr -d '"' || echo "N/A")
    log_file=$(grep "^LOG_FILE=" "$meta_file" 2>/dev/null | cut -d'=' -f2 | tr -d '"' || echo "")
    
    [[ -z "$job_id" ]] && continue
    
    # Get current status from SLURM or default to COMPLETED
    if [[ "$SKIP_SLURM" == "true" ]]; then
        current_status="UNKNOWN"
    else
        current_status="${slurm_statuses[$job_id]:-COMPLETED}"
    fi
    
    # Check log file
    if [[ -f "$log_file" ]]; then
        log_exists="✅"
        log_size=$(du -h "$log_file" 2>/dev/null | cut -f1 || echo "0")
    else
        log_exists="❌"
        log_size="N/A"
    fi
    
    # Format stage info
    stage_info="${stage_name}@${step_name}"
    
    # Print job info
    printf "%-8s %-25s %-12s %-19s %-8s %-8s\n" \
        "$job_id" "$stage_info" "$current_status" "$submit_time" "$log_exists" "$log_size"
        
done | sort -k1,1n  # Sort by job ID

echo ""
echo "Total jobs: $(find "$STATUS_DIR" -name "*.meta" | wc -l)"

if [[ "$SKIP_SLURM" == "true" ]]; then
    echo ""
    echo "Note: SLURM status checking was skipped. Use without --skip-slurm for live status."
fi
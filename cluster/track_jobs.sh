#!/bin/bash
# Job tracking utility for SLURM jobs submitted via trigger.sh

set -euo pipefail

#==============================================================================
# CONFIGURATION
#==============================================================================

# Try to source environment if available
if [[ -f cluster/set_env.sh ]]; then
    source cluster/set_env.sh
fi

# Set default paths if environment variables aren't available
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(dirname "$SCRIPT_DIR")"

if [[ -n "${SCRATCH:-}" && -n "${GIT_REPO_NAME:-}" ]]; then
    LOG_BASE_DIR="${SCRATCH}/${GIT_REPO_NAME}/logs"
else
    # Fallback to relative path
    LOG_BASE_DIR="${BASE_DIR}/logs"
fi

STATUS_DIR="$LOG_BASE_DIR/status"
JOBS_DIR="$LOG_BASE_DIR/jobs"

#==============================================================================
# UTILITY FUNCTIONS
#==============================================================================

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Print colored output
print_color() {
    local color=$1
    shift
    echo -e "${color}$*${NC}"
}

# Check if directories exist
check_directories() {
    if [[ ! -d "$STATUS_DIR" ]]; then
        echo "Status directory not found: $STATUS_DIR"
        echo "Have you submitted any jobs with the enhanced trigger.sh?"
        exit 1
    fi
}

# Safe metadata parsing function
parse_metadata() {
    local meta_file="$1"
    
    # Clear all variables first
    unset JOB_ID STAGE STAGE_NAME STEP_NAME SUBMIT_TIME SUBMIT_TIMESTAMP LOG_FILE STATUS USER PARTITION NODES CPUS MEMORY GPUS TIME_LIMIT GIT_COMMIT DEPENDENCY HOSTNAME WORKING_DIR
    
    # Read variables safely
    while IFS='=' read -r key value; do
        # Skip empty lines and comments
        [[ -z "$key" || "$key" =~ ^[[:space:]]*# ]] && continue
        
        # Remove leading/trailing whitespace and quotes
        key=$(echo "$key" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
        value=$(echo "$value" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//;s/^"//;s/"$//')
        
        case "$key" in
            JOB_ID) JOB_ID="$value" ;;
            STAGE) STAGE="$value" ;;
            STAGE_NAME) STAGE_NAME="$value" ;;
            STEP_NAME) STEP_NAME="$value" ;;
            SUBMIT_TIME) SUBMIT_TIME="$value" ;;
            SUBMIT_TIMESTAMP) SUBMIT_TIMESTAMP="$value" ;;
            LOG_FILE) LOG_FILE="$value" ;;
            STATUS) STATUS="$value" ;;
            USER) USER="$value" ;;
            PARTITION) PARTITION="$value" ;;
            NODES) NODES="$value" ;;
            CPUS) CPUS="$value" ;;
            MEMORY) MEMORY="$value" ;;
            GPUS) GPUS="$value" ;;
            TIME_LIMIT) TIME_LIMIT="$value" ;;
            GIT_COMMIT) GIT_COMMIT="$value" ;;
            DEPENDENCY) DEPENDENCY="$value" ;;
            HOSTNAME) HOSTNAME="$value" ;;
            WORKING_DIR) WORKING_DIR="$value" ;;
        esac
    done < "$meta_file"
}

# Get current SLURM job status
get_slurm_status() {
    local job_id="$1"
    if command -v squeue >/dev/null 2>&1; then
        squeue -j "$job_id" -h -o "%T" 2>/dev/null || echo "COMPLETED"
    else
        echo "UNKNOWN"
    fi
}

# Format duration from seconds
format_duration() {
    local seconds="$1"
    local days=$((seconds / 86400))
    local hours=$(((seconds % 86400) / 3600))
    local minutes=$(((seconds % 3600) / 60))
    
    if [[ $days -gt 0 ]]; then
        echo "${days}d ${hours}h ${minutes}m"
    elif [[ $hours -gt 0 ]]; then
        echo "${hours}h ${minutes}m"
    else
        echo "${minutes}m"
    fi
}

#==============================================================================
# COMMAND FUNCTIONS
#==============================================================================

show_help() {
    cat << EOF
Usage: $0 [COMMAND] [OPTIONS]

Job tracking utility for SLURM jobs submitted via trigger.sh

COMMANDS:
    list                List recent jobs (default)
    status [JOB_ID]     Show status of specific job or all jobs
    search PATTERN      Search jobs by stage/step name
    active              Show only running/pending jobs
    failed              Show only failed jobs
    completed           Show only completed jobs
    logs JOB_ID         Show log file for job
    tail JOB_ID [LINES] Tail log file for job (default: 50 lines)
    follow JOB_ID       Follow log file in real-time
    clean [DAYS]        Clean old job metadata (default: 30 days)
    summary             Show job summary statistics
    
OPTIONS:
    -v, --verbose       Show verbose output
    -n, --count N       Limit number of jobs shown (default: 20)
    -h, --help          Show this help

EXAMPLES:
    $0                          # List recent jobs
    $0 list -n 50              # List last 50 jobs
    $0 status 12345            # Show specific job status
    $0 search "inference"      # Search for jobs containing "inference"
    $0 active                  # Show running/pending jobs
    $0 logs 12345              # Show full log for job 12345
    $0 tail 12345 100          # Show last 100 lines of job 12345
    $0 follow 12345            # Follow job logs in real-time
    $0 summary                 # Show job statistics

CONFIGURATION:
    Log directory: $LOG_BASE_DIR
    Status files:  $STATUS_DIR
EOF
}

list_jobs() {
    local max_jobs=${1:-20}
    local verbose=${2:-false}
    
    check_directories
    
    if [[ ! -d "$STATUS_DIR" ]] || [[ -z "$(ls -A "$STATUS_DIR"/*.meta 2>/dev/null)" ]]; then
        print_color $YELLOW "No jobs found. Submit some jobs with trigger.sh first!"
        return 0
    fi
    
    print_color $CYAN "Recent Jobs (showing last $max_jobs):"
    print_color $CYAN "$(printf '=%.0s' {1..80})"
    
    if [[ "$verbose" == "true" ]]; then
        printf "%-8s %-25s %-12s %-8s %-15s %-12s %s\n" \
            "JOB_ID" "STAGE" "STATUS" "RUNTIME" "SUBMITTED" "RESOURCES" "LOG"
    else
        printf "%-8s %-25s %-12s %-8s %-15s\n" \
            "JOB_ID" "STAGE" "STATUS" "RUNTIME" "SUBMITTED"
    fi
    
    print_color $CYAN "$(printf '=%.0s' {1..80})"
    
    # Collect job info and sort by job ID (newest first)
    declare -a job_list=()
    
    for meta_file in "$STATUS_DIR"/*.meta; do
        if [[ -f "$meta_file" ]]; then
            # Use safe metadata parsing
            parse_metadata "$meta_file"
            
            # Skip if essential fields are missing
            [[ -z "${JOB_ID:-}" ]] && continue
            
            # Get current status from SLURM
            CURRENT_STATUS=$(get_slurm_status "$JOB_ID")
            
            # Calculate runtime if available
            RUNTIME="N/A"
            if [[ -n "${SUBMIT_TIMESTAMP:-}" ]]; then
                local current_time=$(date +%s)
                local elapsed=$((current_time - SUBMIT_TIMESTAMP))
                RUNTIME=$(format_duration $elapsed)
            fi
            
            # Color code status
            local status_color=""
            case "$CURRENT_STATUS" in
                "RUNNING"|"R") status_color=$GREEN ;;
                "PENDING"|"PD") status_color=$YELLOW ;;
                "COMPLETED"|"CD") status_color=$BLUE ;;
                "FAILED"|"F"|"CANCELLED"|"CA") status_color=$RED ;;
                *) status_color=$NC ;;
            esac
            
            # Format resources
            local resources="${GPUS:-0}GPU/${CPUS:-0}CPU/${MEMORY:-N/A}"
            
            # Store job info for sorting
            job_list+=("$JOB_ID|${STAGE_NAME:-unknown}@${STEP_NAME:-unknown}|$CURRENT_STATUS|$RUNTIME|${SUBMIT_TIME:-N/A}|$resources|${LOG_FILE:-N/A}|$status_color")
        fi
    done
    
    # Sort by job ID (newest first) and display
    printf '%s\n' "${job_list[@]}" | sort -t'|' -k1,1nr | head -"$max_jobs" | while IFS='|' read -r job_id stage status runtime submit_time resources log_file status_color; do
        if [[ "$verbose" == "true" ]]; then
            printf "%-8s %-25s ${status_color}%-12s${NC} %-8s %-15s %-12s %s\n" \
                "$job_id" "$stage" "$status" \
                "$runtime" "$(echo "$submit_time" | cut -d' ' -f2)" \
                "$resources" "$(basename "$log_file")"
        else
            printf "%-8s %-25s ${status_color}%-12s${NC} %-8s %-15s\n" \
                "$job_id" "$stage" "$status" \
                "$runtime" "$(echo "$submit_time" | cut -d' ' -f2)"
        fi
    done
}

search_jobs() {
    local pattern="$1"
    
    check_directories
    
    print_color $CYAN "Jobs matching '$pattern':"
    print_color $CYAN "$(printf '=%.0s' {1..50})"
    
    local found=false
    for meta_file in "$STATUS_DIR"/*.meta; do
        if [[ -f "$meta_file" ]]; then
            if grep -q "$pattern" "$meta_file" 2>/dev/null; then
                parse_metadata "$meta_file"
                local current_status=$(get_slurm_status "${JOB_ID:-unknown}")
                
                print_color $GREEN "Job ${JOB_ID:-unknown}: ${STAGE_NAME:-unknown}@${STEP_NAME:-unknown}"
                echo "  Status: $current_status"
                echo "  Log: ${LOG_FILE:-N/A}"
                echo "  Submitted: ${SUBMIT_TIME:-N/A}"
                echo ""
                found=true
            fi
        fi
    done
    
    if [[ "$found" == "false" ]]; then
        print_color $YELLOW "No jobs found matching '$pattern'"
    fi
}

show_active_jobs() {
    check_directories
    
    print_color $CYAN "Active Jobs (Running/Pending):"
    print_color $CYAN "$(printf '=%.0s' {1..50})"
    
    if command -v squeue >/dev/null 2>&1; then
        local user=$(whoami)
        if squeue -u "$user" -h >/dev/null 2>&1; then
            print_color $GREEN "SLURM Queue Status:"
            squeue -u "$user" -o "%.8i %.20j %.12T %.10M %.6D %.20S"
        else
            print_color $YELLOW "No active jobs in SLURM queue"
        fi
    else
        print_color $YELLOW "SLURM not available - showing last known status"
        local found=false
        for meta_file in "$STATUS_DIR"/*.meta; do
            if [[ -f "$meta_file" ]]; then
                parse_metadata "$meta_file"
                if [[ "${STATUS:-}" =~ ^(RUNNING|PENDING|SUBMITTED)$ ]]; then
                    echo "Job ${JOB_ID:-unknown}: ${STAGE_NAME:-unknown}@${STEP_NAME:-unknown} - ${STATUS:-unknown}"
                    found=true
                fi
            fi
        done
        
        if [[ "$found" == "false" ]]; then
            print_color $YELLOW "No active jobs found"
        fi
    fi
}

show_failed_jobs() {
    check_directories
    
    print_color $RED "Failed Jobs:"
    print_color $RED "$(printf '=%.0s' {1..30})"
    
    local found=false
    for meta_file in "$STATUS_DIR"/*.meta; do
        if [[ -f "$meta_file" ]]; then
            parse_metadata "$meta_file"
            local current_status=$(get_slurm_status "${JOB_ID:-unknown}")
            
            if [[ "$current_status" =~ ^(FAILED|CANCELLED|TIMEOUT|F|CA|TO)$ ]]; then
                print_color $RED "Job ${JOB_ID:-unknown}: ${STAGE_NAME:-unknown}@${STEP_NAME:-unknown} - $current_status"
                echo "  Log: ${LOG_FILE:-N/A}"
                echo "  Submitted: ${SUBMIT_TIME:-N/A}"
                echo ""
                found=true
            fi
        fi
    done
    
    if [[ "$found" == "false" ]]; then
        print_color $GREEN "No failed jobs found!"
    fi
}

show_completed_jobs() {
    check_directories
    
    print_color $GREEN "Completed Jobs:"
    print_color $GREEN "$(printf '=%.0s' {1..35})"
    
    local found=false
    for meta_file in "$STATUS_DIR"/*.meta; do
        if [[ -f "$meta_file" ]]; then
            parse_metadata "$meta_file"
            local current_status=$(get_slurm_status "${JOB_ID:-unknown}")
            
            if [[ "$current_status" =~ ^(COMPLETED|CD)$ ]]; then
                echo "Job ${JOB_ID:-unknown}: ${STAGE_NAME:-unknown}@${STEP_NAME:-unknown} - $current_status"
                echo "  Submitted: ${SUBMIT_TIME:-N/A}"
                echo ""
                found=true
            fi
        fi
    done
    
    if [[ "$found" == "false" ]]; then
        print_color $YELLOW "No completed jobs found"
    fi
}

show_job_status() {
    local job_id="$1"
    local meta_file="$STATUS_DIR/${job_id}.meta"
    
    if [[ ! -f "$meta_file" ]]; then
        print_color $RED "Job $job_id not found"
        return 1
    fi
    
    parse_metadata "$meta_file"
    local current_status=$(get_slurm_status "${JOB_ID:-$job_id}")
    
    print_color $CYAN "Job Details: ${JOB_ID:-$job_id}"
    print_color $CYAN "$(printf '=%.0s' {1..30})"
    
    echo "Stage: ${STAGE_NAME:-unknown}@${STEP_NAME:-unknown}"
    echo "Status: $current_status"
    echo "Submitted: ${SUBMIT_TIME:-N/A}"
    echo "User: ${USER:-N/A}"
    echo "Partition: ${PARTITION:-N/A}"
    echo "Resources: ${GPUS:-0} GPUs, ${CPUS:-0} CPUs, ${MEMORY:-N/A} memory"
    echo "Nodes: ${NODES:-1}, Tasks: ${NTASKS:-1}"
    echo "Time Limit: ${TIME_LIMIT:-N/A}"
    echo "Git Commit: ${GIT_COMMIT:-N/A}"
    echo "Log File: ${LOG_FILE:-N/A}"
    
    if [[ -n "${DEPENDENCY:-}" && "$DEPENDENCY" != "none" ]]; then
        echo "Dependency: $DEPENDENCY"
    fi
}

show_job_logs() {
    local job_id="$1"
    local meta_file="$STATUS_DIR/${job_id}.meta"
    
    if [[ ! -f "$meta_file" ]]; then
        print_color $RED "Job $job_id not found"
        return 1
    fi
    
    parse_metadata "$meta_file"
    
    if [[ -f "${LOG_FILE:-}" ]]; then
        print_color $CYAN "=== Log for Job ${JOB_ID:-$job_id} (${STAGE_NAME:-unknown}@${STEP_NAME:-unknown}) ==="
        cat "$LOG_FILE"
    else
        print_color $YELLOW "Log file not found or not yet created: ${LOG_FILE:-N/A}"
        print_color $YELLOW "Job may still be pending or log file may not exist yet."
    fi
}

tail_job_logs() {
    local job_id="$1"
    local lines="${2:-50}"
    local meta_file="$STATUS_DIR/${job_id}.meta"
    
    if [[ ! -f "$meta_file" ]]; then
        print_color $RED "Job $job_id not found"
        return 1
    fi
    
    parse_metadata "$meta_file"
    
    if [[ -f "${LOG_FILE:-}" ]]; then
        print_color $CYAN "=== Last $lines lines of Job ${JOB_ID:-$job_id} (${STAGE_NAME:-unknown}@${STEP_NAME:-unknown}) ==="
        tail -n "$lines" "$LOG_FILE"
    else
        print_color $YELLOW "Log file not found: ${LOG_FILE:-N/A}"
    fi
}

follow_job_logs() {
    local job_id="$1"
    local meta_file="$STATUS_DIR/${job_id}.meta"
    
    if [[ ! -f "$meta_file" ]]; then
        print_color $RED "Job $job_id not found"
        return 1
    fi
    
    parse_metadata "$meta_file"
    
    print_color $CYAN "=== Following log for Job ${JOB_ID:-$job_id} (${STAGE_NAME:-unknown}@${STEP_NAME:-unknown}) ==="
    print_color $YELLOW "Press Ctrl+C to stop following"
    
    if [[ -f "${LOG_FILE:-}" ]]; then
        tail -f "$LOG_FILE"
    else
        print_color $YELLOW "Waiting for log file to be created: ${LOG_FILE:-N/A}"
        # Wait for log file to appear, then follow it
        while [[ ! -f "${LOG_FILE:-}" ]]; do
            sleep 2
        done
        tail -f "$LOG_FILE"
    fi
}

show_summary() {
    check_directories
    
    local total=0
    local running=0
    local pending=0
    local completed=0
    local failed=0
    
    for meta_file in "$STATUS_DIR"/*.meta; do
        if [[ -f "$meta_file" ]]; then
            parse_metadata "$meta_file"
            [[ -z "${JOB_ID:-}" ]] && continue
            
            local current_status=$(get_slurm_status "$JOB_ID")
            
            ((total++))
            case "$current_status" in
                "RUNNING"|"R") ((running++)) ;;
                "PENDING"|"PD") ((pending++)) ;;
                "COMPLETED"|"CD") ((completed++)) ;;
                "FAILED"|"F"|"CANCELLED"|"CA"|"TIMEOUT"|"TO") ((failed++)) ;;
            esac
        fi
    done
    
    print_color $CYAN "Job Summary:"
    print_color $CYAN "$(printf '=%.0s' {1..25})"
    echo "Total jobs: $total"
    print_color $GREEN "Completed: $completed"
    print_color $YELLOW "Running: $running"
    print_color $BLUE "Pending: $pending"
    print_color $RED "Failed: $failed"
}

clean_old_jobs() {
    local days="${1:-30}"
    
    check_directories
    
    print_color $YELLOW "Cleaning job metadata older than $days days..."
    
    local count=0
    find "$STATUS_DIR" -name "*.meta" -mtime +$days -type f | while read file; do
        rm "$file"
        ((count++))
    done
    
    print_color $GREEN "Cleanup complete. Removed $count old metadata files."
}

#==============================================================================
# MAIN COMMAND PROCESSING
#==============================================================================

# Default values
VERBOSE=false
MAX_JOBS=20

# Parse options
while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -n|--count)
            MAX_JOBS="$2"
            shift 2
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        -*)
            echo "Unknown option: $1"
            show_help
            exit 1
            ;;
        *)
            break
            ;;
    esac
done

# Main command processing
COMMAND="${1:-list}"

case "$COMMAND" in
    list)
        list_jobs "$MAX_JOBS" "$VERBOSE"
        ;;
    status)
        if [[ -n "${2:-}" ]]; then
            show_job_status "$2"
        else
            list_jobs "$MAX_JOBS" true
        fi
        ;;
    search)
        if [[ -n "${2:-}" ]]; then
            search_jobs "$2"
        else
            print_color $RED "Please provide a search pattern"
            echo "Usage: $0 search PATTERN"
            exit 1
        fi
        ;;
    active)
        show_active_jobs
        ;;
    failed)
        show_failed_jobs
        ;;
    completed)
        show_completed_jobs
        ;;
    logs)
        if [[ -n "${2:-}" ]]; then
            show_job_logs "$2"
        else
            print_color $RED "Please provide a job ID"
            echo "Usage: $0 logs JOB_ID"
            exit 1
        fi
        ;;
    tail)
        if [[ -n "${2:-}" ]]; then
            tail_job_logs "$2" "${3:-50}"
        else
            print_color $RED "Please provide a job ID"
            echo "Usage: $0 tail JOB_ID [LINES]"
            exit 1
        fi
        ;;
    follow)
        if [[ -n "${2:-}" ]]; then
            follow_job_logs "$2"
        else
            print_color $RED "Please provide a job ID"
            echo "Usage: $0 follow JOB_ID"
            exit 1
        fi
        ;;
    clean)
        clean_old_jobs "${2:-30}"
        ;;
    summary)
        show_summary
        ;;
    *)
        print_color $RED "Unknown command: $COMMAND"
        show_help
        exit 1
        ;;
esac
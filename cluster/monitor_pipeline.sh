#!/bin/bash
# Simple pipeline monitoring script

USER=$(whoami)
REFRESH_INTERVAL=${1:-30}  # Default 30 seconds

echo "Pipeline Monitor for $USER (refreshing every ${REFRESH_INTERVAL}s)"
echo "Press Ctrl+C to stop"
echo ""

while true; do
    clear
    echo "=== PIPELINE STATUS @ $(date) ==="
    echo ""
    
    # Job summary
    echo "JOB SUMMARY:"
    echo "============"
    RUNNING=$(squeue -u $USER -t RUNNING -h | wc -l)
    PENDING=$(squeue -u $USER -t PENDING -h | wc -l)
    TOTAL_ACTIVE=$(squeue -u $USER -h | wc -l)
    
    echo "Running: $RUNNING"
    echo "Pending: $PENDING" 
    echo "Total Active: $TOTAL_ACTIVE"
    echo ""
    
    if [ $TOTAL_ACTIVE -gt 0 ]; then
        echo "ACTIVE JOBS:"
        echo "============"
        squeue -u $USER -o "%.8i %.25j %.12T %.10M %.8r"
        echo ""
        
        # Show recent completions
        echo "RECENT COMPLETIONS (last 10):"
        echo "============================="
        sacct -u $USER -S today -o "JobID,JobName,State,ExitCode,Elapsed" --noheader | \
            grep -E "(COMPLETED|FAILED|CANCELLED)" | tail -10
        echo ""
        
        # Show resource usage for running jobs
        if [ $RUNNING -gt 0 ]; then
            echo "RESOURCE USAGE (running jobs):"
            echo "=============================="
            RUNNING_JOBS=$(squeue -u $USER -t RUNNING -h -o "%i" | tr '\n' ',' | sed 's/,$//')
            if [ ! -z "$RUNNING_JOBS" ]; then
                sstat -j $RUNNING_JOBS --format=JobID,MaxRSS,AveCPU 2>/dev/null || echo "Resource data not yet available"
            fi
            echo ""
        fi
    else
        echo "No active jobs found."
        echo ""
        echo "RECENT COMPLETIONS:"
        echo "=================="
        sacct -u $USER -S today -o "JobID,JobName,State,ExitCode,Elapsed" --noheader | \
            grep -E "(COMPLETED|FAILED|CANCELLED)" | tail -5
    fi
    
    echo ""
    echo "Next refresh in ${REFRESH_INTERVAL}s... (Ctrl+C to stop)"
    sleep $REFRESH_INTERVAL
done

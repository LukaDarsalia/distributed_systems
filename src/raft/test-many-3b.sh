#!/usr/bin/env bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 numTrials"
    exit 1
fi

trap 'kill -INT -$pid; exit 1' INT

# Create or clear the failed_logs file
failed_logs="failed_logs"
> "$failed_logs"

runs=$1
f=0

for i in $(seq 1 $runs); do
    OUTPUT=$(go test -run 3D)
    if echo "$OUTPUT" | grep -q "FAIL"; then
        echo "*** FAILED TESTS IN TRIAL $i"
        f=$((f + 1))
        echo "$OUTPUT" >> "$failed_logs"  # Save the failed logs to the file
        echo "$OUTPUT"
    fi
    echo "*** TOTAL TRIALS: $i"
    echo "*** TOTAL FAILS: $f"
done

echo "*** TOTAL FAILS: $f"
if [ $f -eq 0 ]; then
    echo "*** PASSED ALL $runs TESTING TRIALS"
else
    echo "*** FAILED $f OUT OF $runs TRIALS"
    echo "Failed logs are saved in $failed_logs"
fi
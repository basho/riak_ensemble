#!/bin/bash

## Run each module separately to provide isolation
## Assumption: 1 test per module

ran=0
failed=0
failures=""

for name in $(grep -v '#' test/TESTS); do
    ./rebar -C rebar.test.config skip_deps=true suites=$name eunit
    if [ $? -ne 0 ]; then
        failed=$((failed+1))
        failures="$failures $name"
    fi
    ran=$((ran+1))
done

echo "Results: ran $ran tests of which $failed failed".
if [ $failed -gt 0 ]; then
    echo "Failed:"
    for name in $failures; do
        echo "  $name"
    done
    exit 1
fi

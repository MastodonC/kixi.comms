#!/usr/bin/env bash

type aws
if (($? > 0)); then
    printf '%s\n' 'No AWS CLI installed' >&2
    exit 1
fi

type jq
if (($? > 0)); then
    printf '%s\n' 'No jq installed' >&2
    exit 1
fi

aws dynamodb list-tables | jq .TableNames[] | grep "kixi-comms-test-app*" | xargs -n1 aws dynamodb delete-table --table-name

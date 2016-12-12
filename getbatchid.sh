#!/bin/bash

export AWS_ACCESS_KEY_ID=AKIAI2WSQ5635ISWITIQ
export AWS_SECRET_ACCESS_KEY=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
export AWS_DEFAULT_REGION=us-west-2

s3path="s3://tr-search-data/1.5/dev/incrementals/"

epoch=$(aws s3 ls $s3path | grep '^\s*PRE 1' | tail -1 | sed -e 's/.*PRE //' -e 's#/$##')

echo $epoch
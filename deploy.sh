#!/usr/bin/env bash

ssh <your_server> 'rm -rf aws-redshift-to-rds; mkdir -p aws-redshift-to-rds'

rsync -av * <your_server>:aws-redshift-to-rds

ssh <your_server> 'cd aws-redshift-to-rds; sudo docker build --rm=true -t aws-redshift-to-rds .'

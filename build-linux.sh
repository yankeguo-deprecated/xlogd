#!/bin/bash

CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "-X main.Version=`date +%Y-%m-%d_%T\(%z\)`" -o xlogd-linux-amd64

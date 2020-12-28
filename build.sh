#!/bin/bash
echo "Started Build Process"
env GOOS=windows go build -o cloudwatch-log-aggregator-windows.exe
echo "Done building for Windows"
env GOOS=darwin go build -o cloudwatch-log-aggregator-mac
echo "Done building for Mac"
env GOOS=linux go build -o cloudwatch-log-aggregator-linux
echo "Done building for Linux"
if [ ! -d "./binaries" ]; then
    mkdir binaries
fi
mv cloudwatch-log-aggregator* binaries/

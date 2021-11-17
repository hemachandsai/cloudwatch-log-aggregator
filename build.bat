@echo off
echo "Started Build Process"
set GOOS=darwin
go build -o cloudwatch-log-aggregator-mac
echo "Done building for Mac"
set GOOS=linux
go build -o cloudwatch-log-aggregator-linux
echo "Done building for Linux"
set GOOS=windows
go build -o cloudwatch-log-aggregator-windows.exe
echo "Done building for Windows"

if not exist ".\binaries" mkdir .\binaries
move .\cloudwatch-log-aggregator* .\binaries
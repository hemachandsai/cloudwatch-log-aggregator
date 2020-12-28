package logs

import (
	"fmt"
	"os"
	"path"
	"runtime"
	"strings"
	"time"
)

var (
	logChannel     = make(chan string)
	dateOnlyFormat = "2006-01-02"
	reset          = "\033[0m"
	red            = "\033[31m"
	Debug          bool
)

func Init() {
	initializeColorVariables()
	writeLogToFile()
}

//LogError write the input msg to stdout
func LogError(msg string) {
	fmt.Println(red + msg + reset)
}

//LogToFile writes logdata to a file
func LogToFile(logData string) {
	logChannel <- logData
}

func writeLogToFile() {
	currentDir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	localDateString := time.Now().UTC().Add(-(5*60 + 30) * time.Minute).Format(dateOnlyFormat)
	file, err := os.OpenFile(path.Join(currentDir, strings.Replace(localDateString, ":", "-", -1)+"-cloudwatch-debug.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		LogError("Error: Failed opening file: " + err.Error())
		os.Exit(1)
	}

	defer file.Close()
	for {
		logInput := <-logChannel
		if Debug {
			_, err = file.WriteString(logInput + "\n")
			if err != nil {
				LogError("Error: Failed writing to log file: " + err.Error())
				os.Exit(1)
			}
		}
	}
}

func initializeColorVariables() {
	if runtime.GOOS == "windows" {
		reset = ""
		red = ""
	}
}

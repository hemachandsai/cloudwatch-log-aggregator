package files

import (
	"cloudwatch-log-aggregator/modules/logs"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	MapMutex          *sync.RWMutex
	QueryOutputMap    *map[string]string
	InputDateFormat   *string
	FieldHeaderString *string
)

func WriteOutputToFiles() {
	queryMap := *QueryOutputMap
	currentDir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	datesArray := []string{}

	//map locks are necessary to prevent simultaneois reads and writes in high concurrent environments
	MapMutex.Lock()
	for key := range queryMap {
		datesArray = append(datesArray, key)
	}

	sort.Strings(datesArray)
	logs.LogToFile(fmt.Sprintf("%v", datesArray))

	for index, val := range datesArray {
		logs.LogToFile(fmt.Sprintf("Records date: %v length: %v", val, len(strings.Split(queryMap[val], "\n"))))
		//append to the output csv file
		localDateString := time.Now().UTC().Add(-(5*60 + 30) * time.Minute).Format(*InputDateFormat)
		file, err := os.OpenFile(path.Join(currentDir, strings.Replace(localDateString, ":", "-", -1)+"-cloudwatch-output.csv"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			logs.LogError("Error: Failed opening file: " + err.Error())
			os.Exit(1)
		}

		defer file.Close()
		if index == 0 {
			_, err = file.WriteString(*FieldHeaderString + queryMap[val])
		} else {
			_, err = file.WriteString(queryMap[val])
		}
		if err != nil {
			logs.LogError("Error: Failed writing to file: " + err.Error())
			os.Exit(1)
		}
	}
	//unlocking the previous lock
	MapMutex.Unlock()
}

package files

import (
	"cloudwatch-log-aggregator/modules/logs"
	"cloudwatch-log-aggregator/modules/types"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"regexp"
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
	FilterColumnName  *string
	splitIndex        = -1
	waitGroup         = sync.WaitGroup{}
)

func WriteOutputToFiles() string {
	var filepath string
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
		localDateString := time.Now().UTC().Add((5*60 + 30) * time.Minute).Format(*InputDateFormat)
		filename := strings.Replace(localDateString, ":", "-", -1) + "-cloudwatch-output.csv"
		filepath = path.Join(currentDir, filename)
		file, err := os.OpenFile(path.Join(currentDir, filename), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
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
	return filepath
}

func WriteFilteredOutputToFiles(filepath string) {
	duplicateTrackingMap := map[string]types.DuplicateTrackerInnerStruct{}
	localDuplicateCounter := 0
	if *FilterColumnName == "" {
		return
	}
	fmt.Println("Started writing filtered output to files...")
	fileData, err := ioutil.ReadFile(filepath)
	if err != nil {
		panic(err)
	}
	fileDataString := string(fileData)
	if len(strings.Split(fileDataString, "\n")) < 2 {
		logs.LogError("Error: Zero records found in the output file. Skipping the filtering process...")
		return
	}
	csvHeaderData := strings.Split(fileDataString, "\n")[0]
	commaSplitValues := strings.Split(csvHeaderData, ",")
	regex, err := regexp.Compile("(?i)" + *FilterColumnName)
	if err != nil {
		panic(err)
	}
	for index, val := range commaSplitValues {
		if regex.MatchString(val) {
			splitIndex = index
		}
	}
	if splitIndex == -1 {
		logs.LogError("Error: Given filter column name doesnt exist in the query output. Please check the filter column name and input query...")
		return
	}
	splitFileData := strings.Split(fileDataString, "\n")
	for index, record := range splitFileData {
		if record != "" {
			uuid := strings.Split(record, ",")[splitIndex]
			if _, exists := duplicateTrackingMap[uuid]; exists {
				valRef := duplicateTrackingMap[uuid]
				valRef.DuplicateCounter++
				existingRecordTime, err := time.Parse(*InputDateFormat, strings.Replace(strings.Split(valRef.LatestValue, ",")[0], " ", "T", 1))
				if err != nil {
					fmt.Println(err)
				}
				currentRecordTime, err := time.Parse(*InputDateFormat, strings.Replace(strings.Split(record, ",")[0], " ", "T", 1))
				if err != nil {
					fmt.Println(err)
				}
				if currentRecordTime.UnixNano() > existingRecordTime.UnixNano() {
					valRef.LatestValue = record
				} else if currentRecordTime.UnixNano() == existingRecordTime.UnixNano() {
					splitFileData[index] = ""
				}
				duplicateTrackingMap[uuid] = valRef
			} else {
				duplicateTrackingMap[uuid] = types.DuplicateTrackerInnerStruct{
					DuplicateCounter: 0,
					LatestValue:      record,
				}
			}
		}
	}
	for index, record := range splitFileData {
		if record == "" {
			localDuplicateCounter++
			continue
		}
		uuid := strings.Split(record, ",")[splitIndex]
		if _, exists := duplicateTrackingMap[uuid]; exists && duplicateTrackingMap[uuid].DuplicateCounter > 0 {
			if duplicateTrackingMap[uuid].LatestValue != record {
				localDuplicateCounter++
				splitFileData[index] = ""
			}
		}
	}
	filteredArray := []string{}
	filterArray("", &splitFileData, &filteredArray)
	ioutil.WriteFile(strings.Replace(filepath, ".csv", "-sorted.csv", 1), []byte(strings.Join(filteredArray, "\n")), os.ModePerm)
	fmt.Println("Done writing filtered output to file... Duplicate Count: ", localDuplicateCounter-1)
}

func filterArray(filter string, srcArrayPointer, destArrayPointer *[]string) {
	for _, value := range *srcArrayPointer {
		if value != filter {
			*destArrayPointer = append(*destArrayPointer, value)
		}
	}
}

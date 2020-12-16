package main

import (
	"cloudwatch-log-aggregator/modules/colors"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
)

// parse toml
// hanlde different envs of different os
// make sure the command line input parsing is robust and as versatile as possible
// struture output propely with good naming conventions
// handle go lang multiprocessing code neatly

var (
	cloudWatchSession       *cloudwatchlogs.CloudWatchLogs
	waitGroup               = &sync.WaitGroup{}
	startQueryChannel       = make(chan []int64)
	getQueryOpChannel       = make(chan []interface{})
	mapMutex                = sync.RWMutex{}
	programInput            programInputTOML
	inputDateFormat         = "2006-01-02T15:04:05"
	dayInSeconds            = int64(24 * 60 * 60)
	queryOutputMap          = map[string]string{}
	firstQuerycounter       = 0
	secondQuerycounter      = 0
	totalTasks              = 0
	completedTasks          = 0
	maxConcurrentCallsToAWS = 8
	totalRecordsMatched     = 0.0
	totalRecordsScanned     = 0.0
	fieldHeaderString       string
	emptyHeaderString       bool
)

type programInputTOML struct {
	StartTime    string
	EndTime      string
	LogGroupName string
	LogQuery     string
	AWSRegion    string
}

func main() {
	fmt.Println("Started Execution")
	programStartTime := time.Now()
	parseToml()
	validateTomlData()

	//starting a goroutine which takes care of the logging process. Initialzing a head for high availability
	go printProgressToTerminal(false)

	newSession := session.Must(session.NewSession())
	cloudWatchSession = cloudwatchlogs.New(newSession, aws.NewConfig().WithRegion(programInput.AWSRegion))

	startTime, err := time.Parse(inputDateFormat, programInput.StartTime)
	if err != nil {
		logError("Error: Invalid StartTime format. Please use yyyy-mm-ddThh:mm:ss format in config.toml file")
		os.Exit(1)
	}
	endTime, err := time.Parse(inputDateFormat, programInput.EndTime)
	if err != nil {
		logError("Error: Invalid EndTime format. Please use yyyy-mm-ddThh:mm:ss format in config.toml file")
		os.Exit(1)
	}

	if startTime.After(endTime) || startTime.Equal(endTime) {
		logError("Error: Invalid Input Time. StartTime should be less than EndTime")
		os.Exit(1)
	}

	startTimeEpoch := startTime.Unix()
	endTimeEpoch := endTime.Unix()

	//initiate go routines
	go initializeGoRoutines()

	for startTimeEpoch+dayInSeconds-1 <= endTimeEpoch {
		startQueryChannel <- []int64{startTimeEpoch, startTimeEpoch + dayInSeconds - 1}
		startTimeEpoch += dayInSeconds
		totalTasks++
	}
	if startTimeEpoch+dayInSeconds > endTimeEpoch && startTimeEpoch < endTimeEpoch {
		startQueryChannel <- []int64{startTimeEpoch, endTimeEpoch}
		totalTasks++
	}

	//this call is blocking and it prevents the main gorutine from exiting untill all others finishes
	waitGroup.Wait()

	//log complete progress to terminal
	printProgressToTerminal(true)
	fmt.Println("Completing...Writing Output to Files")

	writeOutputToFiles()
	fmt.Println("Completed fetching logs from clodwatch for the given time span")
	fmt.Println(fmt.Sprintf("Execution Stats:\nTotal Time Taken: %v\nTotal Records Scanned: %v\nTotal Records Matched: %v", time.Since(programStartTime), totalRecordsScanned, totalRecordsMatched))

}

func parseToml() {
	byteData, err := ioutil.ReadFile("./config.toml")
	if err != nil {
		logError("Config.toml file doesn't exist.Please create a config.toml file in the same directory")
		os.Exit(1)
	}
	stringFileData := string(byteData)
	if _, err := toml.Decode(stringFileData, &programInput); err != nil {
		logError("Error in TOML file: " + err.Error())
		os.Exit(1)
	}
}

func validateTomlData() {
	isInvalid := false
	if programInput.StartTime == "" {
		isInvalid = true
		logError("Error in config.toml file data. StartTime cannot be empty")
	}
	if programInput.EndTime == "" {
		isInvalid = true
		logError("Error in config.toml file data. EndTime cannot be empty")
	}
	if programInput.LogGroupName == "" {
		isInvalid = true
		logError("Error in config.toml file data. LogGroupName cannot be empty")
	}
	if programInput.LogQuery == "" {
		isInvalid = true
		logError("Error in config.toml file data. LogQuery cannot be empty")
	}
	if programInput.AWSRegion == "" {
		isInvalid = true
		logError("Error in config.toml file data. AWSRegion cannot be empty")
	}
	if isInvalid {
		os.Exit(1)
	}
}

func printProgressToTerminal(isCompleted bool) {
	for completedTasks < totalTasks || totalTasks == 0 {
		//fmt.Println("invoked", completedTasks, totalTasks)
		if completedTasks != 0 && totalTasks != 0 {
			fmt.Print("\033[2K\r" + getProgressString(false))
		}
		time.Sleep(time.Second * 1)
	}
	if isCompleted {
		fmt.Print("\033[2K\r" + getProgressString(true))
	}
}

func getProgressString(isCompleted bool) string {
	currentProgress := ((float64(completedTasks) / float64(totalTasks)) * 100)
	var text string
	if isCompleted {
		text = "Completed ["
	} else {
		text = "Ongoing ["
	}
	for i := 0; i <= int(math.Floor(currentProgress)); i++ {
		text += "="
	}
	text += ">] " + strconv.Itoa(int(math.Floor(currentProgress))) + "%"
	if isCompleted {
		text += "\n"
	}
	return text
}

func initializeGoRoutines() {
	go func() {
		for {
			val, _ := <-startQueryChannel
			// add 1 to waitGroup counter as one goroutine is going to be initialized
			waitGroup.Add(1)
			go func() {
				// fmt.Println("counter1", firstQuerycounter, secondQuerycounter)
				for firstQuerycounter >= maxConcurrentCallsToAWS {
					time.Sleep(time.Millisecond * 300)
				}
				firstQuerycounter++
				defer func() {
					waitGroup.Done()
				}()
				startLogsQuery(val)
			}()
		}
	}()
	go func() {
		for {
			val, _ := <-getQueryOpChannel
			// add 1 to waitGroup counter as one goroutine is going to be initialized
			waitGroup.Add(1)
			go func() {
				// fmt.Println("counter", firstQuerycounter, secondQuerycounter)
				secondQuerycounter++
				defer func() {
					secondQuerycounter--
					waitGroup.Done()
				}()
				getLogsQueryOutput(val)
			}()
		}
	}()
}

func startLogsQuery(queryInputData []int64) {
	if queryInputData[0] == 0 || queryInputData[1] == 0 {
		panic("Invalid input recieved through channel")
	}
	//fmt.Println("Querying from ", time.Unix(queryInputData[0], 0).UTC().Format(inputDateFormat), "to", time.Unix(queryInputData[1], 0).UTC().Format(inputDateFormat))
	queryInputStruct := &cloudwatchlogs.StartQueryInput{
		LogGroupName: aws.String(programInput.LogGroupName),
		StartTime:    aws.Int64(queryInputData[0]),
		EndTime:      aws.Int64(queryInputData[1]),
		Limit:        aws.Int64(10000),
		QueryString:  aws.String(programInput.LogQuery),
	}
	queryOutput, err := cloudWatchSession.StartQuery(queryInputStruct)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == credentials.ErrNoValidProvidersFoundInChain.Code() {
				logError("AWS authentication failed. Please configure aws-cli in the system or load the access_key and secret_access_token to environment variables.\nPlease refer to https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html#specifying-credentials for more info")
			} else if awsErr.Code() == cloudwatchlogs.ErrCodeResourceNotFoundException {
				logError("No Resource found with the given LogGroupName. Please make sure the LogGroupName and AWSRegion are correctly mentioned in the config.toml file")
			} else if awsErr.Code() == cloudwatchlogs.ErrCodeMalformedQueryException {
				logError("Invalid LogQuery. Please double check the LogQuery in the config.toml file")
			} else {
				logError("Error: " + awsErr.Code() + " " + awsErr.Message())
			}
			os.Exit(1)
		} else {
			//99 percent it will never hit this block but leaving it as suggested by aws sdk docs
			panic(err.Error())
		}
	}
	getQueryOpChannel <- []interface{}{queryOutput.QueryId, []int64{queryInputData[0], queryInputData[1]}, 0}
	//this sleep command here is a sort of wierd fix to prevent race condition to delay the return of this function thereby delaying the defer statement in initializeGoRoutines method first anonymous block
	time.Sleep(time.Millisecond * 100)
}

func getLogsQueryOutput(queryOutputData []interface{}) {
	queryInputStruct := &cloudwatchlogs.GetQueryResultsInput{
		QueryId: queryOutputData[0].(*string),
	}

	queryOutput, err := cloudWatchSession.GetQueryResults(queryInputStruct)
	if err != nil {
		panic(err.Error())
	}

	if *queryOutput.Status != "Complete" {
		time.Sleep(time.Millisecond * 500)
		//if the query status is not complete we push the value back to channel after waiting for some time
		getQueryOpChannel <- []interface{}{queryOutputData[0].(*string), []int64{queryOutputData[1].([]int64)[0], queryOutputData[1].([]int64)[1]}}
		//this sleep command here is a sort of wierd fix to prevent race condition to delay the return of this function thereby delaying the defer statement in initializeGoRoutines method first anonymous block
		time.Sleep(time.Millisecond * 100)
		return
	}

	//proceed if query status is completed
	totalRecordsMatched += *queryOutput.Statistics.RecordsMatched
	totalRecordsScanned += *queryOutput.Statistics.RecordsScanned

	if *queryOutput.Statistics.RecordsMatched > float64(10000) {
		//if number of output records are more than 10000 then split the timeframe to half and push to channel
		difference := queryOutputData[1].([]int64)[1] - queryOutputData[1].([]int64)[0]
		if difference < 5 {
			panic("Error: Too many records are present in a shorttime frame. Please query manually..")
		}

		startQueryChannel <- []int64{queryOutputData[1].([]int64)[0], int64(math.Floor(float64(queryOutputData[1].([]int64)[1] - (difference / 2))))}
		startQueryChannel <- []int64{int64(math.Floor(float64(queryOutputData[1].([]int64)[1]-(difference/2)))) + 1, queryOutputData[1].([]int64)[1]}

		//this sleep command here is a sort of wierd fix to prevent race condition to delay the return of this function thereby delaying the defer statement in initializeGoRoutines method first anonymous block
		time.Sleep(time.Millisecond * 100)
		totalTasks = totalTasks + 2
	} else {
		for _, eachRecord := range queryOutput.Results {
			var date string
			var dataString string
			// var emptyHeaderString bool
			for sindex, field := range eachRecord {
				if *field.Field == "@ptr" {
					//skip this type ones as we dont need them
					continue
				}
				if *field.Field == "@timestamp" {
					date = strings.Split(*field.Value, " ")[0]
				}
				if fieldHeaderString == "" || emptyHeaderString {
					emptyHeaderString = true
					fieldHeaderString += *field.Field + ","
					if sindex == len(eachRecord)-2 {
						fieldHeaderString = fieldHeaderString[:len(fieldHeaderString)-1] + "\n"
						emptyHeaderString = false
					}
				}
				dataString += *field.Value + ","

				// here we are subtracting 2 considering @ptr recorded as unnecessary
				if sindex == len(eachRecord)-2 {
					dataString = dataString[:len(dataString)-1] + "\n"
				}
			}
			queryOutputMap[date] += dataString
		}
	}
	//we are considering the task as completed only upon recieving the actual outut for the submitted query id
	firstQuerycounter--
	completedTasks++
}

func writeOutputToFiles() {
	currentDir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	datesArray := []string{}

	//map locks are necessary to prevent simultaneois reads and writes in high concurrent environments
	mapMutex.Lock()
	for key := range queryOutputMap {
		datesArray = append(datesArray, key)
	}

	sort.Strings(datesArray)
	for _, val := range queryOutputMap {
		// err := ioutil.WriteFile(path.Join(currentDir, key+".csv"), []byte(val), 0644)
		// if err != nil {
		// 	fmt.Println(err)
		// 	os.Exit(1)
		// }
		//append to the output csv file
		localDateString := time.Now().UTC().Add(-(5*60 + 30) * time.Minute).Format(inputDateFormat)
		file, err := os.OpenFile(path.Join(currentDir, localDateString+"-cloudwatch-output.csv"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			logError("Error: Failed opening file: " + err.Error())
			os.Exit(1)
		}

		defer file.Close()
		_, err = file.WriteString(fieldHeaderString + val)
		if err != nil {
			logError("Error: Failed writing to file: " + err.Error())
			os.Exit(1)
		}
	}
	//unlocking the previous lock
	mapMutex.Unlock()
}

func logError(msg string) {
	fmt.Println(colors.Red + msg + colors.Reset)
}

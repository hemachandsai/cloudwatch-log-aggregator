package main

import (
	"cloudwatch-log-aggregator/modules/files"
	"cloudwatch-log-aggregator/modules/logs"
	"cloudwatch-log-aggregator/modules/memutils"
	"cloudwatch-log-aggregator/modules/types"
	"cloudwatch-log-aggregator/modules/validations"
	"fmt"
	"math"
	"math/big"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

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
	programInput            types.ProgramInputTOML
	inputDateFormat         = "2006-01-02T15:04:05"
	dayInSeconds            = int64(24 * 60 * 60)
	queryOutputMap          = map[string]string{}
	firstQuerycounter       = 0
	secondQuerycounter      = 0
	totalTasks              = 0
	completedTasks          = 0
	maxConcurrentCallsToAWS = 10
	totalRecordsMatched     = 0.0
	totalRecordsScanned     = big.NewFloat(0.0)
	clearANSISequence       = "\033[H\033[2J\033[3J"
	fieldHeaderString       string
	emptyHeaderString       bool
	debug                   = true
	isWindows               bool
)

func main() {
	memutils.CheckSystemMemory()

	fmt.Println("Started Execution")
	programStartTime := time.Now()

	initVariablesForSubModules()
	validations.DoValidations()

	go logs.Init()
	//starting a goroutine which takes care of the logging process. Initialzing a head for high availability
	go printProgressToTerminal(false)

	newSession := session.Must(session.NewSession())
	cloudWatchSession = cloudwatchlogs.New(newSession, aws.NewConfig().WithRegion(programInput.AWSRegion))

	startTime, err := time.Parse(inputDateFormat, programInput.StartTime)
	if err != nil {
		logs.LogError("Error: Invalid StartTime format. Please use yyyy-mm-ddThh:mm:ss format in config.toml file")
		os.Exit(1)
	}
	endTime, err := time.Parse(inputDateFormat, programInput.EndTime)
	if err != nil {
		logs.LogError("Error: Invalid EndTime format. Please use yyyy-mm-ddThh:mm:ss format in config.toml file")
		os.Exit(1)
	}

	if startTime.After(endTime) || startTime.Equal(endTime) {
		logs.LogError("Error: Invalid Input Time. StartTime should be less than EndTime")
		os.Exit(1)
	}

	startTimeEpoch := startTime.Unix()
	endTimeEpoch := endTime.Unix()

	//initiate go routines
	go initializeGoRoutines()

	for startTimeEpoch+dayInSeconds-1 <= endTimeEpoch {
		startQueryChannel <- []int64{startTimeEpoch, startTimeEpoch + dayInSeconds}
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

	files.WriteOutputToFiles()
	fmt.Println("Completed fetching logs from clodwatch for the given time span")
	fmt.Printf("Execution Stats:\n\tTotal Time Taken: %v\n\tTotal Records Scanned: %.2f\n\tTotal Records Matched: %v\n", time.Since(programStartTime), totalRecordsScanned, totalRecordsMatched)
}

func initVariablesForSubModules() {
	validations.ProgramInput = &programInput
	logs.Debug = debug
	files.MapMutex = &mapMutex
	files.QueryOutputMap = &queryOutputMap
	files.InputDateFormat = &inputDateFormat
	files.FieldHeaderString = &fieldHeaderString
	if runtime.GOOS == "windows" {
		isWindows = true
	}
}

func clearTerminal() {
	if isWindows {
		cmd := exec.Command("cmd", "/c", "cls")
		cmd.Stdout = os.Stdout
		cmd.Run()
	} else {
		fmt.Print(clearANSISequence)
	}
}

func printProgressToTerminal(isCompleted bool) {
	for completedTasks < totalTasks || totalTasks == 0 {
		if totalTasks != 0 {
			clearTerminal()
			fmt.Print(memutils.GetMemUsageString() + "\n" + getProgressString(false))
		}
		if isWindows {
			time.Sleep(time.Second * 2)
		} else {
			time.Sleep(time.Second * 1)
		}
	}
	if isCompleted {
		clearTerminal()
		fmt.Print(memutils.GetMemUsageString() + "\n" + getProgressString(true))
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
	text += ">"
	for i := 0; i < 100-int(math.Floor(currentProgress)); i++ {
		text += " "
	}
	text += "] " + strconv.Itoa(int(math.Floor(currentProgress))) + "%"
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
				logs.LogToFile(fmt.Sprintf("counter1	%v, %v", firstQuerycounter, secondQuerycounter))
				for firstQuerycounter >= maxConcurrentCallsToAWS-1 {
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
				logs.LogToFile(fmt.Sprintf("counter1	%v, %v", firstQuerycounter, secondQuerycounter))
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
	logs.LogToFile(fmt.Sprintf("Querying from %v to %v", time.Unix(queryInputData[0], 0).UTC().Format(inputDateFormat), time.Unix(queryInputData[1], 0).UTC().Format(inputDateFormat)))
	queryInputStruct := &cloudwatchlogs.StartQueryInput{
		LogGroupName: aws.String(programInput.LogGroupName),
		StartTime:    aws.Int64(queryInputData[0]),
		EndTime:      aws.Int64(queryInputData[1]),
		QueryString:  aws.String(programInput.LogQuery + "\n| limit 10000"),
	}
	queryOutput, err := cloudWatchSession.StartQuery(queryInputStruct)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == credentials.ErrNoValidProvidersFoundInChain.Code() {
				logs.LogError("AWS authentication failed. Please configure aws-cli in the system or load the access_key_id, aws_secret_access_key to AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY environment variables.\nPlease refer to https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html#specifying-credentials for more info")
			} else if awsErr.Code() == cloudwatchlogs.ErrCodeResourceNotFoundException {
				logs.LogError("No Resource found with the given LogGroupName. Please make sure the LogGroupName and AWSRegion are correctly mentioned in the config.toml file")
			} else if awsErr.Code() == cloudwatchlogs.ErrCodeMalformedQueryException {
				logs.LogError("Invalid LogQuery. Please double check the LogQuery in the config.toml file")
			} else if awsErr.Code() == cloudwatchlogs.ErrCodeLimitExceededException {
				logs.LogError("AWS Concurrency Limit Exceeded. Might be some other people are using queries through aws dashboard. Please try again")
			} else {
				logs.LogError("Error: " + awsErr.Code() + " " + awsErr.Message())
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
	if *queryOutput.Status != "Complete" && *queryOutput.Status != "Failed" {
		time.Sleep(time.Millisecond * 500)
		//if the query status is not complete we push the value back to channel after waiting for some time
		getQueryOpChannel <- []interface{}{queryOutputData[0].(*string), []int64{queryOutputData[1].([]int64)[0], queryOutputData[1].([]int64)[1]}}
		//this sleep command here is a sort of wierd fix to prevent race condition to delay the return of this function thereby delaying the defer statement in initializeGoRoutines method first anonymous block
		time.Sleep(time.Millisecond * 100)
		return
	}
	logs.LogToFile(fmt.Sprintf("Query Output Recieved %v to %v, length: %v", time.Unix(queryOutputData[1].([]int64)[0], 0).UTC().Format(inputDateFormat), time.Unix(queryOutputData[1].([]int64)[1], 0).UTC().Format(inputDateFormat), len(queryOutput.Results)))
	if *queryOutput.Statistics.RecordsMatched > float64(10000) || *queryOutput.Status == "Failed" {
		//if number of output records are more than 10000 then split the timeframe to half and push to channel
		difference := queryOutputData[1].([]int64)[1] - queryOutputData[1].([]int64)[0]
		if difference < 5 {
			panic("Error: Too many records are present in a shorttime frame. Please query manually..")
		}

		startQueryChannel <- []int64{queryOutputData[1].([]int64)[0], int64(math.Floor(float64(queryOutputData[1].([]int64)[1] - (difference / 2))))}
		startQueryChannel <- []int64{int64(math.Floor(float64(queryOutputData[1].([]int64)[1] - (difference / 2)))), queryOutputData[1].([]int64)[1]}

		//this sleep command here is a sort of wierd fix to prevent race condition to delay the return of this function thereby delaying the defer statement in initializeGoRoutines method first anonymous block
		time.Sleep(time.Millisecond * 100)
		totalTasks = totalTasks + 2
	} else {
		//proceed if query status is completed
		totalRecordsMatched += *queryOutput.Statistics.RecordsMatched
		totalRecordsScanned = totalRecordsScanned.Add(big.NewFloat(*queryOutput.Statistics.RecordsScanned), totalRecordsScanned)
		var dataString string
		var date string
		for _, eachRecord := range queryOutput.Results {
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
		}
		mapMutex.Lock()
		queryOutputMap[date] += dataString
		mapMutex.Unlock()
	}
	//we are considering the task as completed only upon recieving the actual outut for the submitted query id
	firstQuerycounter--
	completedTasks++
}

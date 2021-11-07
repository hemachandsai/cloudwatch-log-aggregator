package validations

import (
	"cloudwatch-log-aggregator/modules/logs"
	"cloudwatch-log-aggregator/modules/types"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"

	"github.com/BurntSushi/toml"
)

var (
	ProgramInput *types.ProgramInputTOML
	reset        = "\033[0m"
	red          = "\033[31m"
)

//DoValidations run toml and other validations essential for program
func DoValidations() {
	parseToml()
	validateTomlData()
	checkForLimitValueInQueryString()
}

func parseToml() {
	byteData, err := ioutil.ReadFile("./config.toml")
	if err != nil {
		logs.LogError("Config.toml file doesn't exist.Please create a config.toml file in the same directory")
		os.Exit(1)
	}
	stringFileData := string(byteData)
	if _, err := toml.Decode(stringFileData, &ProgramInput); err != nil {
		logs.LogError("Error in TOML file: " + err.Error())
		os.Exit(1)
	}
}

func checkForLimitValueInQueryString() {
	matched, err := regexp.MatchString(`limit`, ProgramInput.LogQuery)
	if err != nil {
		fmt.Println(err)
	}
	if matched {
		logError("Please remove limit variable from query string in config.toml file...")
		os.Exit(1)
	}
}

func validateTomlData() {
	isInvalid := false
	if ProgramInput.StartTime == "" {
		isInvalid = true
		logError("Error during config.toml validation. StartTime cannot be empty")
	}
	if ProgramInput.EndTime == "" {
		isInvalid = true
		logError("Error during config.toml validation. EndTime cannot be empty")
	}
	if ProgramInput.LogGroupName == "" {
		isInvalid = true
		logError("Error during config.toml validation. LogGroupName cannot be empty")
	}
	if ProgramInput.LogQuery == "" {
		isInvalid = true
		logError("Error during config.toml validation. LogQuery cannot be empty")
	}
	if ProgramInput.AWSRegion == "" {
		isInvalid = true
		logError("Error during config.toml validation. AWSRegion cannot be empty")
	}
	if isInvalid {
		os.Exit(1)
	}
}

func logError(msg string) {
	fmt.Println(red + msg + reset)
}

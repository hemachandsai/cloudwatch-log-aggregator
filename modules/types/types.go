package types

type ProgramInputTOML struct {
	StartTime    string
	EndTime      string
	LogGroupName string
	LogQuery     string
	AWSRegion    string
}

type DuplicateTrackerInnerStruct struct {
	DuplicateCounter int
	LatestValue      string
}

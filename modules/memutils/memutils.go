package memutils

import (
	"fmt"
	"os"
	"runtime"

	"github.com/shirou/gopsutil/mem"
)

var (
	reset = "\033[0m"
	red   = "\033[31m"
)

func CheckSystemMemory() {
	v, _ := mem.VirtualMemory()
	if v.Free < 1*1024*1024*1024 {
		logError("Available system memory less than 1GB. Please try again")
		os.Exit(1)
	}
}

func GetMemUsageString() string {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return fmt.Sprintf("Memory Stats:\n") +
		fmt.Sprintf("\tCurrent System Memory = %v MiB\n", bToMb(m.Sys)) +
		fmt.Sprintf("\tAllocated Memory= %v MiB\n", bToMb(m.Alloc)) +
		fmt.Sprintf("\tTotal Allocated Memory= %v MiB\n", bToMb(m.TotalAlloc))
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func logError(msg string) {
	fmt.Println(red + msg + reset)
}

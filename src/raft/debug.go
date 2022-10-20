package raft

import (
	"os"
	"log"
	"time"
	"fmt"
	"strconv"
)
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

type logTopic string

const (
	dClient logTopic = "CLNT"
	dVote   logTopic = "VOTE"
	dLeader logTopic = "LEAD"
	dTimer  logTopic = "TIMR"
	dLog	logTopic = "LOGE"
	dDecode logTopic = "DECO"
)

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Printo(topic logTopic, format string, fm ...interface{}) {
	if debugVerbosity >= 0 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, fm...)
	}
}
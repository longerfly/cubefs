package contentsummary

import (
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/log"
	gopath "path"
	"strings"
	"sync"
)

var once sync.Once

func parseLogLevel(loglvl string) log.Level {
	var level log.Level
	switch strings.ToLower(loglvl) {
	case "debug":
		level = log.DebugLevel
	case "info":
		level = log.InfoLevel
	case "warn":
		level = log.WarnLevel
	case "error":
		level = log.ErrorLevel
	default:
		level = log.ErrorLevel
	}
	return level
}

func InitLog(logDir string, logLevel string) {
	if logDir != "" {
		level := parseLogLevel(logLevel)
		log.InitLog(logDir, "getSummary", level, nil, log.DefaultLogLeftSpaceLimitRatio)
	}
}

func GetSummary(volName string, masters string, logDir string, logLevel string, enableSummary bool, goroutineNum int32, path string) (*meta.SummaryInfo, error) {
	var err error
	once.Do(func() { InitLog(logDir, logLevel) })

	// init metawrapper
	var mw *meta.MetaWrapper
	if mw, err = meta.NewMetaWrapper(&meta.MetaConfig{
		Volume:        volName,
		Masters:       strings.Split(masters, ","),
		ValidateOwner: false,
		EnableSummary: enableSummary,
	}); err != nil {
		return nil, err
	}
	defer mw.Close()
	ino, err := mw.LookupPath(absPath(path))
	if err != nil {
		return nil, err
	}
	// log.LogInfof("GetSummary_ll : [ %v ]", absPath(path))
	summaryInfo, err := mw.GetSummary_ll(ino, goroutineNum)
	if err != nil {
		return nil, err
	}

	// flush log
	log.LogFlush()

	return &summaryInfo, nil
}

// internals

func absPath(path string) string {
	p := gopath.Clean(path)
	if !gopath.IsAbs(p) {
		p = gopath.Join("/", p)
	}
	return gopath.Clean(p)
}

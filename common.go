package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
	"unicode"
)

type EventProcessor interface {
	EventName() string
	Process(line []byte)
	Start()
	Stop()
	WriteOutput(path string) error
	Entries() interface{}
	ResultCount() int
}

type nameOnly struct {
	Name string `json:"name"`
}

func truncateToStep(ts string, stepMinutes int) (string, error) {
	t, err := time.Parse("2006-01-02T15:04:05.999999", ts)
	if err != nil {
		return "", err
	}
	step := time.Duration(stepMinutes) * time.Minute
	return t.Truncate(step).Format("2006-01-02T15:04"), nil
}

func parseFileHour(path string) (time.Time, error) {
	base := filepath.Base(path)
	name := base[:len(base)-len(filepath.Ext(base))]
	if len(name) != 8 {
		return time.Time{}, fmt.Errorf("имя файла не соответствует формату YYMMDDHH: %s", name)
	}
	return time.Parse("060102_15", name[:6]+"_"+name[6:])
}

func parsePeriodArg(s string) (time.Time, error) {
	now := time.Now()
	switch len(s) {
	case 1, 2:
		hourStr := fmt.Sprintf("%02s", s)
		return time.Parse("20060102_15", fmt.Sprintf("%04d%02d%02d_%s",
			now.Year(), int(now.Month()), now.Day(), hourStr))
	case 8:
		return time.Parse("060102_15", s[:6]+"_"+s[6:])
	default:
		return time.Time{}, fmt.Errorf("некорректный формат %q: ожидается HH или YYMMDDHH", s)
	}
}

func collectFiles(root string, begin, end time.Time, hasBegin, hasEnd bool) ([]string, error) {
	var files []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if filepath.Ext(path) != ".log" {
			return nil
		}
		ft, err := parseFileHour(path)
		if err != nil {
			return nil
		}
		if hasBegin && ft.Before(begin) {
			return nil
		}
		if hasEnd && ft.After(end) {
			return nil
		}
		files = append(files, path)
		return nil
	})
	return files, err
}

func trimLeftNonPrintable(s string) string {
	for i, r := range s {
		if unicode.IsPrint(r) {
			return s[i:]
		}
	}
	return ""
}

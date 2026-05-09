package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"
)

type args struct {
	inputDir     string
	outputFile   string
	workers      int
	step         int
	begin        time.Time
	end          time.Time
	hasBegin     bool
	hasEnd       bool
	activeEvents map[string]bool
	zip          bool
}

func parseArgs() *args {
	inputDir := flag.String("dir", ".", "Путь к папке с файлами логов (поиск рекурсивный)")
	outputFile := flag.String("out", "result.json", "Путь к выходному файлу")
	workers := flag.Int("workers", 4, "Количество воркеров для параллельного чтения файлов")
	step := flag.Int("step", 3, "Шаг агрегации в минутах")
	beginStr := flag.String("begin", "", "Начало периода: HH или YYMMDDHH")
	endStr := flag.String("end", "", "Конец периода: HH или YYMMDDHH")
	eventsStr := flag.String("events", "", "Список событий через запятую (CALL,DBPOSTGRS,DBMSSQL,CLSTR). По умолчанию все")
	zipFlag := flag.Bool("zip", false, "Сжать выходной файл в zip-архив")
	flag.Parse()

	if *step < 1 {
		log.Fatalf("Ошибка: шаг агрегации должен быть не менее 1 минуты")
	}

	supportedEvents := supportedEventNames()
	activeEvents := make(map[string]bool)
	if *eventsStr != "" {
		for _, e := range strings.Split(*eventsStr, ",") {
			e = strings.TrimSpace(strings.ToUpper(e))
			if !supportedEvents[e] {
				log.Fatalf("Ошибка: неподдерживаемое событие %q. Поддерживаемые: %s", e, strings.Join(eventNamesList(), ", "))
			}
			activeEvents[e] = true
		}
	} else {
		activeEvents = supportedEvents
	}

	var begin, end time.Time
	var hasBegin, hasEnd bool
	var err error

	if *beginStr != "" {
		begin, err = parsePeriodArg(*beginStr)
		if err != nil {
			log.Fatalf("Ошибка: некорректный -begin %q: %v", *beginStr, err)
		}
		hasBegin = true
	}

	if *endStr != "" {
		end, err = parsePeriodArg(*endStr)
		if err != nil {
			log.Fatalf("Ошибка: некорректный -end %q: %v", *endStr, err)
		}
		hasEnd = true
	}

	if hasBegin && hasEnd && end.Before(begin) {
		log.Fatalf("Ошибка: -end не может быть раньше -begin")
	}

	a := &args{
		inputDir:     *inputDir,
		outputFile:   *outputFile,
		workers:      *workers,
		step:         *step,
		begin:        begin,
		end:          end,
		hasBegin:     hasBegin,
		hasEnd:       hasEnd,
		activeEvents: activeEvents,
		zip:          *zipFlag,
	}
	a.printInfo()
	return a
}

func (a *args) printInfo() {
	fmt.Printf("Сканирование файлов логов в папке: %s\n", a.inputDir)
	if a.hasBegin {
		fmt.Printf("Начало периода: %s\n", a.begin.Format("2006-01-02 15h"))
	} else {
		fmt.Println("Начало периода: не задано (без фильтра)")
	}
	if a.hasEnd {
		fmt.Printf("Конец периода:  %s\n", a.end.Format("2006-01-02 15h"))
	} else {
		fmt.Println("Конец периода:  не задано (без фильтра)")
	}
	fmt.Printf("Шаг агрегации: %d мин.\n", a.step)
}

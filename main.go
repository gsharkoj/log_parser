package main

import (
	"archive/zip"
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

func parseFile(path string, processors map[string]EventProcessor) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("не удалось открыть файл %s: %w", path, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	buf := make([]byte, 8*1024*1024)
	scanner.Buffer(buf, len(buf))

	first := true
	for scanner.Scan() {
		var line []byte
		if first {
			s := trimLeftNonPrintable(scanner.Text())
			line = []byte(s)
			first = false
		} else {
			line = scanner.Bytes()
		}

		if len(line) == 0 {
			continue
		}

		var hdr nameOnly
		if err := json.Unmarshal(line, &hdr); err != nil {
			continue
		}
		if p, ok := processors[hdr.Name]; ok {
			p.Process(line)
		}
	}
	return scanner.Err()
}

func worker(files <-chan string, processors map[string]EventProcessor, wg *sync.WaitGroup) {
	defer wg.Done()
	for path := range files {
		if err := parseFile(path, processors); err != nil {
			log.Printf("Ошибка чтения файла: %v", err)
		}
	}
}

type processorFactory func(int) EventProcessor

var processorFactories = []processorFactory{
	func(step int) EventProcessor { return NewCallProcessor(step) },
	func(step int) EventProcessor { return NewDBPostgrsProcessor(step) },
	func(step int) EventProcessor { return NewDBMssqlProcessor(step) },
	func(step int) EventProcessor { return NewClstrPerfProcessor(step) },
}

func supportedEventNames() map[string]bool {
	m := map[string]bool{}
	for _, factory := range processorFactories {
		m[factory(0).EventName()] = true
	}
	return m
}

func eventNamesList() []string {
	names := make([]string, 0, len(processorFactories))
	for _, factory := range processorFactories {
		names = append(names, factory(0).EventName())
	}
	return names
}

func createProcessors(activeEvents map[string]bool, step int) map[string]EventProcessor {
	processors := map[string]EventProcessor{}
	for _, factory := range processorFactories {
		p := factory(step)
		if activeEvents[p.EventName()] {
			processors[p.EventName()] = p
		}
	}
	return processors
}

func main() {
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

	fmt.Printf("Сканирование файлов логов в папке: %s\n", *inputDir)
	if hasBegin {
		fmt.Printf("Начало периода: %s\n", begin.Format("2006-01-02 15h"))
	} else {
		fmt.Println("Начало периода: не задано (без фильтра)")
	}
	if hasEnd {
		fmt.Printf("Конец периода:  %s\n", end.Format("2006-01-02 15h"))
	} else {
		fmt.Println("Конец периода:  не задано (без фильтра)")
	}
	fmt.Printf("Шаг агрегации: %d мин.\n", *step)

	files, err := collectFiles(*inputDir, begin, end, hasBegin, hasEnd)
	if err != nil {
		log.Fatalf("Ошибка сканирования директории: %v", err)
	}
	fmt.Printf("Найдено файлов: %d\n", len(files))
	if len(files) == 0 {
		fmt.Println("Файлы для обработки не найдены.")
		return
	}

	processors := createProcessors(activeEvents, *step)

	for _, p := range processors {
		p.Start()
	}

	fileCh := make(chan string, len(files))
	for _, f := range files {
		fileCh <- f
	}
	close(fileCh)

	var wg sync.WaitGroup
	n := *workers
	if n < 1 {
		n = 1
	}
	fmt.Printf("Запуск %d воркер(ов)...\n", n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go worker(fileCh, processors, &wg)
	}

	wg.Wait()

	for _, p := range processors {
		p.Stop()
	}

	for name, p := range processors {
		fmt.Printf("Агрегировано %s: %d\n", name, p.ResultCount())
	}

	output := make(map[string]interface{})
	for name, p := range processors {
		output[name] = p.Entries()
	}

	if *zipFlag {
		zf, err := os.Create(*outputFile + ".zip")
		if err != nil {
			log.Fatalf("Ошибка создания zip-архива: %v", err)
		}
		defer zf.Close()

		w := zip.NewWriter(zf)
		defer w.Close()

		fw, err := w.Create(*outputFile)
		if err != nil {
			log.Fatalf("Ошибка создания записи в zip: %v", err)
		}

		enc := json.NewEncoder(fw)
		enc.SetIndent("", "  ")
		if err := enc.Encode(output); err != nil {
			log.Fatalf("Ошибка записи результата в zip: %v", err)
		}
		w.Close()
		fmt.Printf("Результат сохранён в файл: %s.zip\n", *outputFile)
	} else {
		f, err := os.Create(*outputFile)
		if err != nil {
			log.Fatalf("Ошибка создания выходного файла: %v", err)
		}
		defer f.Close()

		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		if err := enc.Encode(output); err != nil {
			log.Fatalf("Ошибка записи результата: %v", err)
		}
		fmt.Printf("Результат сохранён в файл: %s\n", *outputFile)
	}
}

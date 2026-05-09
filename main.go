package main

import (
	"archive/zip"
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
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

func main() {
	cfg := parseArgs()

	files, err := collectFiles(cfg.inputDir, cfg.begin, cfg.end, cfg.hasBegin, cfg.hasEnd)
	if err != nil {
		log.Fatalf("Ошибка сканирования директории: %v", err)
	}
	fmt.Printf("Найдено файлов: %d\n", len(files))
	if len(files) == 0 {
		fmt.Println("Файлы для обработки не найдены.")
		return
	}

	processors := createProcessors(cfg.activeEvents, cfg.step)

	for _, p := range processors {
		p.Start()
	}

	fileCh := make(chan string, len(files))
	for _, f := range files {
		fileCh <- f
	}
	close(fileCh)

	var wg sync.WaitGroup
	n := cfg.workers
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

	if cfg.zip {
		zf, err := os.Create(cfg.outputFile + ".zip")
		if err != nil {
			log.Fatalf("Ошибка создания zip-архива: %v", err)
		}
		defer zf.Close()

		w := zip.NewWriter(zf)
		defer w.Close()

		fw, err := w.Create(cfg.outputFile)
		if err != nil {
			log.Fatalf("Ошибка создания записи в zip: %v", err)
		}

		enc := json.NewEncoder(fw)
		enc.SetIndent("", "  ")
		if err := enc.Encode(output); err != nil {
			log.Fatalf("Ошибка записи результата в zip: %v", err)
		}
		w.Close()
		fmt.Printf("Результат сохранён в файл: %s.zip\n", cfg.outputFile)
	} else {
		f, err := os.Create(cfg.outputFile)
		if err != nil {
			log.Fatalf("Ошибка создания выходного файла: %v", err)
		}
		defer f.Close()

		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		if err := enc.Encode(output); err != nil {
			log.Fatalf("Ошибка записи результата: %v", err)
		}
		fmt.Printf("Результат сохранён в файл: %s\n", cfg.outputFile)
	}
}

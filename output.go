package main

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"log"
	"os"
)

func writeOutput(path string, data map[string]interface{}, useZip bool) {
	if useZip {
		writeZip(path, data)
	} else {
		writeJSON(path, data)
	}
}

func writeZip(path string, data map[string]interface{}) {
	zf, err := os.Create(path + ".zip")
	if err != nil {
		log.Fatalf("Ошибка создания zip-архива: %v", err)
	}
	defer zf.Close()

	w := zip.NewWriter(zf)
	defer w.Close()

	fw, err := w.Create(path)
	if err != nil {
		log.Fatalf("Ошибка создания записи в zip: %v", err)
	}

	enc := json.NewEncoder(fw)
	enc.SetIndent("", "  ")
	if err := enc.Encode(data); err != nil {
		log.Fatalf("Ошибка записи результата в zip: %v", err)
	}
	w.Close()
	fmt.Printf("Результат сохранён в файл: %s.zip\n", path)
}

func writeJSON(path string, data map[string]interface{}) {
	f, err := os.Create(path)
	if err != nil {
		log.Fatalf("Ошибка создания выходного файла: %v", err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(data); err != nil {
		log.Fatalf("Ошибка записи результата: %v", err)
	}
	fmt.Printf("Результат сохранён в файл: %s\n", path)
}

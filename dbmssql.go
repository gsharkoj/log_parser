package main

import (
	"encoding/json"
	"log"
	"os"
)

type DBMssqlEntry struct {
	Ts           string `json:"ts"`
	Name         string `json:"name"`
	ProcessName  string `json:"p:processName"`
	Usr          string `json:"Usr"`
	SessionID    string `json:"SessionID"`
	Context      string `json:"Context"`
	Duration     int64  `json:"duration,string"`
	Sql          string `json:"Sql"`
	RowsAffected int64  `json:"RowsAffected,string"`
}

type DBMssqlGroupKey struct {
	TsWindow    string
	Context     string
	ProcessName string
	Usr         string
	SessionID   string
}

type DBMssqlAggregatedEntry struct {
	TsWindow     string `json:"ts_window"`
	ProcessName  string `json:"processName"`
	Usr          string `json:"Usr"`
	SessionID    string `json:"SessionID"`
	Context      string `json:"Context"`
	Duration     int64  `json:"duration"`
	Sql          string `json:"Sql"`
	RowsAffected int64  `json:"RowsAffected"`
	Count        int64  `json:"Count"`
}

type DBMssqlProcessor struct {
	ch     chan DBMssqlEntry
	result map[DBMssqlGroupKey]*DBMssqlAggregatedEntry
	step   int
	done   chan struct{}
}

func NewDBMssqlProcessor(step int) *DBMssqlProcessor {
	return &DBMssqlProcessor{
		ch:   make(chan DBMssqlEntry, 10000),
		step: step,
		done: make(chan struct{}),
	}
}

func (p *DBMssqlProcessor) EventName() string { return "DBMSSQL" }

func (p *DBMssqlProcessor) Process(line []byte) {
	var entry DBMssqlEntry
	if err := json.Unmarshal(line, &entry); err != nil {
		log.Printf("Ошибка парсинга DBMSSQL: %v", err)
		return
	}
	if entry.Name != "DBMSSQL" {
		return
	}
	p.ch <- entry
}

func (p *DBMssqlProcessor) Start() {
	go func() {
		p.result = make(map[DBMssqlGroupKey]*DBMssqlAggregatedEntry)
		for e := range p.ch {
			window, err := truncateToStep(e.Ts, p.step)
			if err != nil {
				log.Printf("Ошибка: некорректная временная метка %q: %v", e.Ts, err)
				continue
			}
			key := DBMssqlGroupKey{TsWindow: window, Context: e.Context, ProcessName: e.ProcessName, Usr: e.Usr, SessionID: e.SessionID}
			agg, ok := p.result[key]
			if !ok {
				agg = &DBMssqlAggregatedEntry{
					TsWindow:    window,
					Context:     e.Context,
					ProcessName: e.ProcessName,
					Usr:         e.Usr,
					SessionID:   e.SessionID,
					Sql:         e.Sql,
				}
				p.result[key] = agg
			}
			agg.Duration += e.Duration
			agg.RowsAffected += e.RowsAffected
			agg.Count++
		}
		close(p.done)
	}()
}

func (p *DBMssqlProcessor) Stop() {
	close(p.ch)
	<-p.done
}

func (p *DBMssqlProcessor) WriteOutput(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	entries := make([]*DBMssqlAggregatedEntry, 0, len(p.result))
	for _, v := range p.result {
		entries = append(entries, v)
	}
	return enc.Encode(entries)
}

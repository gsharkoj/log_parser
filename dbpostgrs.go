package main

import (
	"encoding/json"
	"log"
	"os"
)

type DBPostgrsEntry struct {
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

type DBPostgrsGroupKey struct {
	TsWindow    string
	Context     string
	ProcessName string
	Usr         string
	SessionID   string
}

type DBPostgrsAggregatedEntry struct {
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

type DBPostgrsProcessor struct {
	ch     chan DBPostgrsEntry
	result map[DBPostgrsGroupKey]*DBPostgrsAggregatedEntry
	step   int
	done   chan struct{}
}

func NewDBPostgrsProcessor(step int) *DBPostgrsProcessor {
	// При step=0 создаётся облегчённый объект только для получения EventName()
	if step == 0 {
		return &DBPostgrsProcessor{step: step}
	}
	return &DBPostgrsProcessor{
		ch:   make(chan DBPostgrsEntry, 10000),
		step: step,
		done: make(chan struct{}),
	}
}

func (p *DBPostgrsProcessor) EventName() string { return "DBPOSTGRS" }

func (p *DBPostgrsProcessor) Process(line []byte) {
	var entry DBPostgrsEntry
	if err := json.Unmarshal(line, &entry); err != nil {
		log.Printf("Ошибка парсинга DBPOSTGRS: %v", err)
		return
	}
	if entry.Name != "DBPOSTGRS" {
		return
	}
	/*
	if entry.Context == "" {
		return
	}
	*/
	p.ch <- entry
}

func (p *DBPostgrsProcessor) Start() {
	go func() {
		p.result = make(map[DBPostgrsGroupKey]*DBPostgrsAggregatedEntry)
		for e := range p.ch {
			window, err := truncateToStep(e.Ts, p.step)
			if err != nil {
				log.Printf("Ошибка: некорректная временная метка %q: %v", e.Ts, err)
				continue
			}
			key := DBPostgrsGroupKey{TsWindow: window, Context: e.Context, ProcessName: e.ProcessName, Usr: e.Usr, SessionID: e.SessionID}
			agg, ok := p.result[key]
			if !ok {
				agg = &DBPostgrsAggregatedEntry{
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

func (p *DBPostgrsProcessor) Stop() {
	close(p.ch)
	<-p.done
}

func (p *DBPostgrsProcessor) WriteOutput(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(p.Entries())
}

func (p *DBPostgrsProcessor) Entries() interface{} {
	entries := make([]*DBPostgrsAggregatedEntry, 0, len(p.result))
	for _, v := range p.result {
		entries = append(entries, v)
	}
	return entries
}

func (p *DBPostgrsProcessor) ResultCount() int {
	return len(p.result)
}

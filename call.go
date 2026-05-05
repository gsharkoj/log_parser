package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

type CallEntry struct {
	Ts          string `json:"ts"`
	Name        string `json:"name"`
	ProcessName string `json:"p:processName"`
	Usr         string `json:"Usr"`
	Func        string `json:"Func"`
	Module      string `json:"Module"`
	Method      string `json:"Method"`
	Context     string `json:"Context"`
	Duration    int64  `json:"duration,string"`
	Memory      int64  `json:"Memory,string"`
	MemoryPeak  int64  `json:"MemoryPeak,string"`
	InBytes     int64  `json:"InBytes,string"`
	OutBytes    int64  `json:"OutBytes,string"`
	CpuTime     int64  `json:"CpuTime,string"`
}

type CallGroupKey struct {
	TsWindow    string
	Context     string
	ProcessName string
	Usr         string
}

type CallAggregatedEntry struct {
	TsWindow    string `json:"ts_window"`
	ProcessName string `json:"p:processName"`
	Usr         string `json:"Usr"`
	Context     string `json:"Context"`
	Duration    int64  `json:"duration"`
	Memory      int64  `json:"Memory"`
	MemoryPeak  int64  `json:"MemoryPeak"`
	InBytes     int64  `json:"InBytes"`
	OutBytes    int64  `json:"OutBytes"`
	CpuTime     int64  `json:"CpuTime"`
	Count       int64  `json:"Count"`
}

type CallProcessor struct {
	ch     chan CallEntry
	result map[CallGroupKey]*CallAggregatedEntry
	step   int
	done   chan struct{}
}

func NewCallProcessor(step int) *CallProcessor {
	return &CallProcessor{
		ch:   make(chan CallEntry, 10000),
		step: step,
		done: make(chan struct{}),
	}
}

func (p *CallProcessor) EventName() string { return "CALL" }

func (p *CallProcessor) Process(line []byte) {
	var entry CallEntry
	if err := json.Unmarshal(line, &entry); err != nil {
		return
	}
	if entry.Name != "CALL" {
		return
	}
	if entry.Func == "Background job" {
		entry.Context = fmt.Sprintf("%s.%s.%s", entry.Func, entry.Module, entry.Method)
	}
	if entry.Context == "" {
		return
	}
	p.ch <- entry
}

func (p *CallProcessor) Start() {
	go func() {
		p.result = make(map[CallGroupKey]*CallAggregatedEntry)
		for e := range p.ch {
			window, err := truncateToStep(e.Ts, p.step)
			if err != nil {
				log.Printf("Ошибка: некорректная временная метка %q: %v", e.Ts, err)
				continue
			}
			key := CallGroupKey{TsWindow: window, Context: e.Context, ProcessName: e.ProcessName, Usr: e.Usr}
			agg, ok := p.result[key]
			if !ok {
				agg = &CallAggregatedEntry{
					TsWindow:    window,
					Context:     e.Context,
					ProcessName: e.ProcessName,
					Usr:         e.Usr,
				}
				p.result[key] = agg
			}
			agg.Duration += e.Duration
			agg.Memory += e.Memory
			agg.MemoryPeak += e.MemoryPeak
			agg.InBytes += e.InBytes
			agg.OutBytes += e.OutBytes
			agg.CpuTime += e.CpuTime
			agg.Count++
		}
		close(p.done)
	}()
}

func (p *CallProcessor) Stop() {
	close(p.ch)
	<-p.done
}

func (p *CallProcessor) WriteOutput(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	entries := make([]*CallAggregatedEntry, 0, len(p.result))
	for _, v := range p.result {
		entries = append(entries, v)
	}
	return enc.Encode(entries)
}

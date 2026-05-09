package main

import (
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"
)

type ClstrPerfEntry struct {
	Ts           string `json:"ts"`
	Name         string `json:"name"`
	Event        string `json:"Event"`
	Data         string `json:"Data"`
}

type clstrPerfData struct {
	Ts                  string
	Process             string
	Pid                 string
	Sql                 int64
	Cpu                 int64
	QueueLength         int64
	QueueLengthCpuNum   int64
	MemoryPerformance   int64
	DiskPerformance     int64
	ResponseTime        int64
	AverageResponseTime int64
}

type ClstrPerfGroupKey struct {
	TsWindow string
	Process  string
	Pid      string
}

type clstrPerfAgg struct {
	TsWindow            string
	Process             string
	Pid                 string
	Sql                 int64
	Cpu                 int64
	QueueLength         int64
	QueueLengthCpuNum   int64
	MemoryPerformance   int64
	DiskPerformance     int64
	ResponseTime        int64
	AverageResponseTime int64
	Count               int64
}

type ClstrPerfAggregateEntry struct {
	TsWindow            string `json:"ts_window"`
	Process             string `json:"process"`
	Pid                 string `json:"pid"`
	Sql                 int64  `json:"sql"`
	Cpu                 int64  `json:"cpu"`
	QueueLength         int64  `json:"queue_length"`
	QueueLengthCpuNum   int64  `json:"queue_length_div_cpu_num"`
	MemoryPerformance   int64  `json:"memory_performance"`
	DiskPerformance     int64  `json:"disk_performance"`
	ResponseTime        int64  `json:"response_time"`
	AverageResponseTime int64  `json:"average_response_time"`
	Count               int64  `json:"Count"`
}

type ClstrPerfProcessor struct {
	ch     chan clstrPerfData
	result map[ClstrPerfGroupKey]*clstrPerfAgg
	step   int
	done   chan struct{}
}

func NewClstrPerfProcessor(step int) *ClstrPerfProcessor {
	// При step=0 создаётся облегчённый объект только для получения EventName()
	if step == 0 {
		return &ClstrPerfProcessor{step: step}
	}
	return &ClstrPerfProcessor{
		ch:   make(chan clstrPerfData, 10000),
		step: step,
		done: make(chan struct{}),
	}
}

func (p *ClstrPerfProcessor) EventName() string { return "CLSTR" }

func parseInt(s string) int64 {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return int64(v + 0.5)
}

func (p *ClstrPerfProcessor) Process(line []byte) {
	var entry ClstrPerfEntry
	if err := json.Unmarshal(line, &entry); err != nil {
		log.Printf("Ошибка парсинга CLSTR: %v", err)
		return
	}
	if entry.Name != "CLSTR" {
		return
	}
	if entry.Event != "Performance update" {
		return
	}

	fields := make(map[string]string)
	for _, pair := range strings.Split(entry.Data, ",") {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) == 2 {
			fields[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
		}
	}

	p.ch <- clstrPerfData{
		Ts:                  entry.Ts,
		Process:             fields["process"],
		Pid:                 fields["pid"],
		Sql:                 parseInt(fields["sql"]),
		Cpu:                 parseInt(fields["cpu"]),
		QueueLength:         parseInt(fields["queue_length"]),
		QueueLengthCpuNum:   parseInt(fields["queue_length/cpu_num"]),
		MemoryPerformance:   parseInt(fields["memory_performance"]),
		DiskPerformance:     parseInt(fields["disk_performance"]),
		ResponseTime:        parseInt(fields["response_time"]),
		AverageResponseTime: parseInt(fields["average_response_time"]),
	}
}

func (p *ClstrPerfProcessor) Start() {
	go func() {
		p.result = make(map[ClstrPerfGroupKey]*clstrPerfAgg)
		for e := range p.ch {
			window, err := truncateToStep(e.Ts, p.step)
			if err != nil {
				log.Printf("Ошибка: некорректная временная метка %q: %v", e.Ts, err)
				continue
			}
			key := ClstrPerfGroupKey{TsWindow: window, Process: e.Process, Pid: e.Pid}
			agg, ok := p.result[key]
			if !ok {
				agg = &clstrPerfAgg{
					TsWindow: window,
					Process:  e.Process,
					Pid:      e.Pid,
				}
				p.result[key] = agg
			}
			agg.Sql += e.Sql
			agg.Cpu += e.Cpu
			agg.QueueLength += e.QueueLength
			agg.QueueLengthCpuNum += e.QueueLengthCpuNum
			agg.MemoryPerformance += e.MemoryPerformance
			agg.DiskPerformance += e.DiskPerformance
			agg.ResponseTime += e.ResponseTime
			agg.AverageResponseTime += e.AverageResponseTime
			agg.Count++
		}
		close(p.done)
	}()
}

func (p *ClstrPerfProcessor) Stop() {
	close(p.ch)
	<-p.done
}

func (p *ClstrPerfProcessor) WriteOutput(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(p.Entries())
}

func (p *ClstrPerfProcessor) Entries() interface{} {
	entries := make([]*ClstrPerfAggregateEntry, 0, len(p.result))
	for _, v := range p.result {
		c := v.Count
		entries = append(entries, &ClstrPerfAggregateEntry{
			TsWindow:            v.TsWindow,
			Process:             v.Process,
			Pid:                 v.Pid,
			Sql:                 v.Sql / c,
			Cpu:                 v.Cpu / c,
			QueueLength:         v.QueueLength / c,
			QueueLengthCpuNum:   v.QueueLengthCpuNum / c,
			MemoryPerformance:   v.MemoryPerformance / c,
			DiskPerformance:     v.DiskPerformance / c,
			ResponseTime:        v.ResponseTime / c,
			AverageResponseTime: v.AverageResponseTime / c,
			Count:               v.Count,
		})
	}
	return entries
}

func (p *ClstrPerfProcessor) ResultCount() int {
	return len(p.result)
}

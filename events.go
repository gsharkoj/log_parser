package main

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

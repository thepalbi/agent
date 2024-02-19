package stages

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-kit/log"
	"github.com/grafana/agent/component/common/loki"
	"github.com/prometheus/client_golang/prometheus"
)

// StageConfig defines a single stage in a processing pipeline.
// We define these as pointers types so we can use reflection to check that
// exactly one is set.
type StageConfig struct {
	//TODO(thampiotr): sync these with new stages
	LabelDropConfig *LabelDropConfig `river:"label_drop,block,optional"`
	MetricsConfig   *MetricsConfig   `river:"metrics,block,optional"`
	SamplingConfig  *SamplingConfig  `river:"sampling,block,optional"`
}

// Pipeline pass down a log entry to each stage for mutation and/or label extraction.
type Pipeline struct {
	logger    log.Logger
	stages    []Stage
	jobName   *string
	dropCount *prometheus.CounterVec
}

// NewPipeline creates a new log entry pipeline from a configuration
func NewPipeline(logger log.Logger, stages []StageConfig, jobName *string, registerer prometheus.Registerer) (*Pipeline, error) {
	st := []Stage{}
	for _, stage := range stages {
		newStage, err := New(logger, jobName, stage, registerer)
		if err != nil {
			return nil, fmt.Errorf("invalid stage config %w", err)
		}
		st = append(st, newStage)
	}
	return &Pipeline{
		logger:  log.With(logger, "component", "pipeline"),
		stages:  st,
		jobName: jobName,
	}, nil
}

// RunWith will reads from the input channel entries, mutate them with the process function and returns them via the output channel.
func RunWith(input chan Entry, process func(e Entry) Entry) chan Entry {
	out := make(chan Entry)
	go func() {
		defer close(out)
		for e := range input {
			out <- process(e)
		}
	}()
	return out
}

// RunWithSkip same as RunWith, except it skip sending it to output channel, if `process` functions returns `skip` true.
func RunWithSkip(input chan Entry, process func(e Entry) (Entry, bool)) chan Entry {
	out := make(chan Entry)
	go func() {
		defer close(out)
		for e := range input {
			ee, skip := process(e)
			if skip {
				continue
			}
			out <- ee
		}
	}()

	return out
}

// RunWithSkiporSendMany same as RunWithSkip, except it can either skip sending it to output channel, if `process` functions returns `skip` true. Or send many entries.
func RunWithSkipOrSendMany(input chan Entry, process func(e Entry) ([]Entry, bool)) chan Entry {
	out := make(chan Entry)
	go func() {
		defer close(out)
		for e := range input {
			results, skip := process(e)
			if skip {
				continue
			}
			for _, result := range results {
				out <- result
			}
		}
	}()

	return out
}

// Run implements Stage
func (p *Pipeline) Run(in chan Entry) chan Entry {
	in = RunWith(in, func(e Entry) Entry {
		// Initialize the extracted map with the initial labels (ie. "filename"),
		// so that stages can operate on initial labels too
		for labelName, labelValue := range e.Labels {
			e.Extracted[string(labelName)] = string(labelValue)
		}
		return e
	})
	// chain all stages together.
	for _, m := range p.stages {
		in = m.Run(in)
	}
	return in
}

// Name implements Stage
func (p *Pipeline) Name() string {
	return StageTypePipeline
}

func (p *Pipeline) Appender(next loki.Appender) loki.Appender {
	interceptor := loki.NewInterceptor(next, loki.WithAppendHook(func(ctx context.Context, e loki.Entry, innerNext loki.Appender) (loki.Entry, error) {
		initialExtracted := map[string]interface{}{}
		for labelName, labelValue := range e.Labels {
			initialExtracted[string(labelName)] = string(labelValue)
		}
		entry := Entry{
			Extracted: initialExtracted,
			Entry:     e,
		}
		for _, m := range p.stages {
			m.Process(e.Labels, entry.Extracted, &e.Timestamp, &e.Line)
		}
		return innerNext.Append(ctx, entry.Entry)
	}))
	return interceptor
}

// Wrap implements EntryMiddleware
func (p *Pipeline) Wrap(next loki.EntryHandler) loki.EntryHandler {
	handlerIn := make(chan loki.Entry)
	nextChan := next.Chan()
	wg, once := sync.WaitGroup{}, sync.Once{}
	pipelineIn := make(chan Entry)
	pipelineOut := p.Run(pipelineIn)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for e := range pipelineOut {
			nextChan <- e.Entry
		}
	}()
	go func() {
		defer wg.Done()
		defer close(pipelineIn)
		for e := range handlerIn {
			pipelineIn <- Entry{
				Extracted: map[string]interface{}{},
				Entry:     e,
			}
		}
	}()
	return loki.NewEntryHandler(handlerIn, func() {
		once.Do(func() { close(handlerIn) })
		wg.Wait()
	})
}

// Size gets the current number of stages in the pipeline
func (p *Pipeline) Size() int {
	return len(p.stages)
}

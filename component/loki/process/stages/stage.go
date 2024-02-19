package stages

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/agent/component/common/loki"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"gopkg.in/yaml.v2"
)

// TODO(@tpaschalis) Let's use this as the list of stages we need to port over.
const (
	StageTypeCRI        = "cri"
	StageTypeDecolorize = "decolorize"
	StageTypeDocker     = "docker"
	StageTypeDrop       = "drop"
	//TODO(thampiotr): Add support for eventlogmessage stage
	StageTypeEventLogMessage    = "eventlogmessage"
	StageTypeGeoIP              = "geoip"
	StageTypeJSON               = "json"
	StageTypeLabel              = "labels"
	StageTypeLabelAllow         = "labelallow"
	StageTypeLabelDrop          = "labeldrop"
	StageTypeLimit              = "limit"
	StageTypeLogfmt             = "logfmt"
	StageTypeMatch              = "match"
	StageTypeMetric             = "metrics"
	StageTypeMultiline          = "multiline"
	StageTypeOutput             = "output"
	StageTypePack               = "pack"
	StageTypePipeline           = "pipeline"
	StageTypeRegex              = "regex"
	StageTypeReplace            = "replace"
	StageTypeSampling           = "sampling"
	StageTypeStaticLabels       = "static_labels"
	StageTypeStructuredMetadata = "structured_metadata"
	StageTypeTemplate           = "template"
	StageTypeTenant             = "tenant"
	StageTypeTimestamp          = "timestamp"
)

// Processor takes an existing set of labels, timestamp and log entry and returns either a possibly mutated
// timestamp and log entry
type Processor interface {
	Process(labels model.LabelSet, extracted map[string]interface{}, time *time.Time, entry *string)
	Name() string
}

type Entry struct {
	Extracted map[string]interface{}
	loki.Entry
}

// Stage can receive entries via an inbound channel and forward mutated entries to an outbound channel.
type Stage interface {
	Name() string
	Run(chan Entry) chan Entry
}

func (entry *Entry) copy() *Entry {
	out, err := yaml.Marshal(entry)
	if err != nil {
		return nil
	}

	var n *Entry
	err = yaml.Unmarshal(out, &n)
	if err != nil {
		return nil
	}

	return n
}

// stageProcessor Allow to transform a Processor (old synchronous pipeline stage) into an async Stage
type stageProcessor struct {
	Processor

	inspector *inspector
}

func (s stageProcessor) Run(in chan Entry) chan Entry {
	return RunWith(in, func(e Entry) Entry {
		var before *Entry

		if Inspect {
			before = e.copy()
		}

		s.Process(e.Labels, e.Extracted, &e.Timestamp, &e.Line)

		if Inspect {
			s.inspector.inspect(s.Processor.Name(), before, e)
		}

		return e
	})
}

func toStage(p Processor) Stage {
	return &stageProcessor{
		Processor: p,
		inspector: newInspector(os.Stderr, runtime.GOOS == "windows"),
	}
}

// New creates a new stage for the given type and configuration.
func New(logger log.Logger, jobName *string, cfg StageConfig, registerer prometheus.Registerer) (Stage, error) {
	var (
		s   Stage
		err error
	)
	switch {
	case cfg.MetricsConfig != nil:
		s, err = newMetricStage(logger, *cfg.MetricsConfig, registerer)
		if err != nil {
			return nil, err
		}
	case cfg.LabelDropConfig != nil:
		s, err = newLabelDropStage(*cfg.LabelDropConfig)
		if err != nil {
			return nil, err
		}
	default:
		panic(fmt.Sprintf("unreachable; should have decoded into one of the StageConfig fields: %+v", cfg))
	}
	return s, nil
}

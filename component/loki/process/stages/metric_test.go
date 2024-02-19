package stages

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/grafana/agent/component/common/loki"
	"github.com/grafana/loki/pkg/logproto"
	util_log "github.com/grafana/loki/pkg/util/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

var testMetricRiver = `
stage.static_labels {
    values = {
      app = "superapp",
	  payload = "10",
    }
}
stage.metrics {
	metric.counter {
			name = "loki_count"
			description = "uhhhhhhh"
			prefix = "my_agent_custom_"
			source = "test"
			value = "app"
			action = "inc"
	}
	metric.counter {
			name = "total_lines_count"
			description = "nothing to see here..."
			match_all = true
			action = "inc"
	}
} `

var testMetricLogLine1 = `
{
	"time":"2012-11-01T22:08:41+00:00",
	"app":"loki",
    "payload": 10,
	"component": ["parser","type"],
	"level" : "WARN"
}
`

var testMetricLogLine2 = `
{
	"time":"2012-11-01T22:08:41+00:00",
	"app":"bloki",
    "payload": 20,
	"component": ["parser","type"],
	"level" : "WARN"
}
`

var testMetricLogLineWithMissingKey = `
{
	"time":"2012-11-01T22:08:41+00:00",
	"payload": 20,
	"component": ["parser","type"],
	"level" : "WARN"
}
`

const expectedMetrics = `# HELP loki_process_custom_total_lines_count nothing to see here...
# TYPE loki_process_custom_total_lines_count counter
loki_process_custom_total_lines_count{app="superapp",payload="10",test="app"} 1
loki_process_custom_total_lines_count{app="superapp",payload="10",test="app2"} 1
# HELP my_agent_custom_loki_count uhhhhhhh
# TYPE my_agent_custom_loki_count counter
my_agent_custom_loki_count{app="superapp",payload="10",test="app"} 1
`

func TestMetricsPipeline(t *testing.T) {
	registry := prometheus.NewRegistry()
	pl, err := NewPipeline(util_log.Logger, loadConfig(testMetricRiver), nil, registry)
	if err != nil {
		t.Fatal(err)
	}

	app := &testAppender{}
	pipelineAppender := pl.Appender(app)

	_, err = pipelineAppender.Append(context.Background(), loki.Entry{
		Labels: model.LabelSet{"test": "app"},
		Entry: logproto.Entry{
			Timestamp: time.Now(),
			Line:      testMetricLogLine1,
		},
	})
	require.NoError(t, err)
	_, err = pipelineAppender.Append(context.Background(), loki.Entry{
		Labels: model.LabelSet{"test": "app2"},
		Entry: logproto.Entry{
			Timestamp: time.Now(),
			Line:      testMetricLogLine2,
		},
	})
	require.NoError(t, err)

	if err := testutil.GatherAndCompare(registry,
		strings.NewReader(expectedMetrics)); err != nil {
		t.Fatalf("mismatch metrics: %v", err)
	}
}

var testMetricWithNonPromLabel = `
stage.static_labels {
		values = { "good_label" = "1" }
}
stage.metrics {
		metric.counter {
				name = "loki_count"
				source = "app"
				description = "should count all entries"
				match_all = true
				action = "inc"
		}
} `

func TestNonPrometheusLabelsShouldBeDropped(t *testing.T) {
	const counterConfig = `
stage.static_labels {
		values = { "good_label" = "1" }
}
stage.metrics {
		metric.counter {
				name = "loki_count"
				source = "app"
				description = "should count all entries"
				match_all = true
				action = "inc"
		}
} `

	const expectedCounterMetrics = `# HELP loki_process_custom_loki_count should count all entries
# TYPE loki_process_custom_loki_count counter
loki_process_custom_loki_count{good_label="1"} 1
`
	for name, tc := range map[string]struct {
		promtailConfig  string
		labels          model.LabelSet
		line            string
		expectedCollect string
	}{
		"counter metric with non-prometheus incoming label": {
			promtailConfig: testMetricWithNonPromLabel,
			labels: model.LabelSet{
				"__bad_label__": "2",
			},
			line:            testMetricLogLine1,
			expectedCollect: expectedCounterMetrics,
		},
		"counter metric with tenant step injected label": {
			promtailConfig:  counterConfig,
			line:            testMetricLogLine1,
			expectedCollect: expectedCounterMetrics,
		},
	} {
		t.Run(name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			pl, err := NewPipeline(util_log.Logger, loadConfig(tc.promtailConfig), nil, registry)
			require.NoError(t, err)
			in := make(chan Entry)
			out := pl.Run(in)

			in <- newEntry(nil, tc.labels, tc.line, time.Now())
			close(in)
			<-out

			err = testutil.GatherAndCompare(registry, strings.NewReader(tc.expectedCollect))
			require.NoError(t, err, "gathered metrics are different than expected")
		})
	}
}

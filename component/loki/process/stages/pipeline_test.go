package stages

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/agent/component/common/loki"
	"github.com/grafana/loki/pkg/logproto"
	util_log "github.com/grafana/loki/pkg/util/log"
	"github.com/grafana/river"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

// Configs defines multiple StageConfigs as consequent blocks.
type Configs struct {
	Stages []StageConfig `river:"stage,enum,optional"`
}

func loadConfig(yml string) []StageConfig {
	var config Configs
	err := river.Unmarshal([]byte(yml), &config)
	if err != nil {
		panic(err)
	}
	return config.Stages
}

type testAppender struct {
	entries []loki.Entry
}

func (t *testAppender) Append(ctx context.Context, e loki.Entry) (loki.Entry, error) {
	t.entries = append(t.entries, e)
	return e, nil
}

func TestPipeline_AppenderAPI(t *testing.T) {
	var config Configs
	err := river.Unmarshal([]byte(`
	stage.static_labels {
		values = {
		  foo = "fooval",
		  bar = "barval",
		}
	}
	`), &config)
	require.NoError(t, err)

	p, err := NewPipeline(util_log.Logger, config.Stages, nil, prometheus.DefaultRegisterer)
	require.NoError(t, err)

	app := &testAppender{}

	out, err := p.Appender(app).Append(context.Background(), loki.Entry{
		Labels: model.LabelSet{
			"tre": "bol",
		},
		Entry: logproto.Entry{
			Line:      "test",
			Timestamp: time.Now(),
		},
	})
	require.NoError(t, err)

	require.Len(t, app.entries, 1)
	require.Equal(t, "test", app.entries[0].Line)
	require.Len(t, out.Labels, 3)
	require.Equal(t, model.LabelSet{
		"foo": "fooval",
		"bar": "barval",
		"tre": "bol",
	}, out.Labels)
}

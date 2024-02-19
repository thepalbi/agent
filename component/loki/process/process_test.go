package process

// NOTE: This code is copied from Promtail (07cbef92268aecc0f20d1791a6df390c2df5c072) with changes kept to the minimum.

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/grafana/agent/component/common/loki"
	"github.com/grafana/agent/component/discovery"
	lsf "github.com/grafana/agent/component/loki/source/file"
	"github.com/grafana/agent/pkg/flow/componenttest"
	"github.com/grafana/agent/pkg/util"
	"github.com/grafana/river"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

type testAppender struct {
	mut     sync.RWMutex
	entries []loki.Entry
	hook    func(entry loki.Entry)
}

func (t *testAppender) Append(ctx context.Context, entry loki.Entry) (loki.Entry, error) {
	t.mut.Lock()
	defer t.mut.Unlock()
	if t.hook != nil {
		t.hook(entry)
	}
	t.entries = append(t.entries, entry)
	return entry, nil
}

func TestEntrySentToTwoProcessComponents(t *testing.T) {
	// Set up two different loki.process components.
	stg1 := `
forward_to = []
stage.static_labels {
    values = { "lbl" = "foo" }
}
`
	stg2 := `
forward_to = []
stage.static_labels {
    values = { "lbl" = "bar" }
}
`

	app1, app2 := &testAppender{}, &testAppender{}
	var args1, args2 Arguments
	require.NoError(t, river.Unmarshal([]byte(stg1), &args1))
	require.NoError(t, river.Unmarshal([]byte(stg2), &args2))
	args1.ForwardTo = []loki.Appender{app1}
	args2.ForwardTo = []loki.Appender{app2}

	// Start the loki.process components.
	tc1, err := componenttest.NewControllerFromID(util.TestLogger(t), "loki.process")
	require.NoError(t, err)
	tc2, err := componenttest.NewControllerFromID(util.TestLogger(t), "loki.process")
	require.NoError(t, err)
	go func() { require.NoError(t, tc1.Run(componenttest.TestContext(t), args1)) }()
	go func() { require.NoError(t, tc2.Run(componenttest.TestContext(t), args2)) }()
	require.NoError(t, tc1.WaitExports(time.Second))
	require.NoError(t, tc2.WaitExports(time.Second))

	// Create a file to log to.
	f, err := os.CreateTemp(t.TempDir(), "example")
	require.NoError(t, err)
	defer f.Close()

	// Create and start a component that will read from that file and fan out to both components.
	ctrl, err := componenttest.NewControllerFromID(util.TestLogger(t), "loki.source.file")
	require.NoError(t, err)

	go func() {
		err := ctrl.Run(context.Background(), lsf.Arguments{
			Targets: []discovery.Target{{"__path__": f.Name(), "somelbl": "somevalue"}},
			ForwardTo: []loki.Appender{
				tc1.Exports().(Exports).Receiver,
				tc2.Exports().(Exports).Receiver,
			},
		})
		require.NoError(t, err)
	}()
	ctrl.WaitRunning(time.Minute)

	// Write a line to the file.
	_, err = f.Write([]byte("writing some text\n"))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		app1.mut.RLock()
		app2.mut.RLock()
		defer app1.mut.RUnlock()
		defer app2.mut.RUnlock()
		return len(app1.entries) == 1 && len(app2.entries) == 1
	}, 5*time.Second, 100*time.Millisecond, "timed out waiting for log lines")

	// assert over entries
	for _, entry := range []loki.Entry{app1.entries[0], app2.entries[0]} {
		require.WithinDuration(t, time.Now(), entry.Timestamp, 1*time.Second)
		require.Equal(t, "writing some text", entry.Line)
	}
	wantLabelSet := model.LabelSet{
		"filename": model.LabelValue(f.Name()),
		"somelbl":  "somevalue",
	}
	require.Equal(t, wantLabelSet.Clone().Merge(model.LabelSet{"lbl": "foo"}), app1.entries[0].Labels)
	require.Equal(t, wantLabelSet.Clone().Merge(model.LabelSet{"lbl": "bar"}), app2.entries[0].Labels)
}

// func TestDeadlockWithFrequentUpdates(t *testing.T) {
// 	stg := `stage.json {
// 			    expressions    = {"output" = "log", stream = "stream", timestamp = "time", "extra" = "" }
// 				drop_malformed = true
// 		    }
// 			stage.json {
// 			    expressions = { "user" = "" }
// 				source      = "extra"
// 			}
// 			stage.labels {
// 			    values = {
// 				  stream = "",
// 				  user   = "",
// 				  ts     = "timestamp",
// 			    }
// 			}`

// 	// Unmarshal the River relabel rules into a custom struct, as we don't have
// 	// an easy way to refer to a loki.LogsReceiver value for the forward_to
// 	// argument.
// 	type cfg struct {
// 		Stages []stages.StageConfig `river:"stage,enum"`
// 	}
// 	var stagesCfg cfg
// 	err := river.Unmarshal([]byte(stg), &stagesCfg)
// 	require.NoError(t, err)

// 	var lastSend atomic.Value
// 	app1, app2 := &testAppender{hook: func(entry loki.Entry) {
// 		lastSend.Store(time.Now())
// 	}}, &testAppender{hook: func(entry loki.Entry) {
// 		lastSend.Store(time.Now())
// 	}}

// 	// Create and run the component, so that it can process and forwards logs.
// 	opts := component.Options{
// 		Logger:        util.TestFlowLogger(t),
// 		Registerer:    prometheus.NewRegistry(),
// 		OnStateChange: func(e component.Exports) {},
// 	}
// 	args := Arguments{
// 		ForwardTo: []loki.Appender{app1, app2},
// 		Stages:    stagesCfg.Stages,
// 	}

// 	c, err := New(opts, args)
// 	require.NoError(t, err)
// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()
// 	go c.Run(ctx)

// 	// Continuously send entries to both channels
// 	// TODO(thepalbi): this should be properly closed
// 	go func() {
// 		for {
// 			ts := time.Now()
// 			logline := `{"log":"log message\n","stream":"stderr","time":"2019-04-30T02:12:41.8443515Z","extra":"{\"user\":\"smith\"}"}`
// 			logEntry := loki.Entry{
// 				Labels: model.LabelSet{"filename": "/var/log/pods/agent/agent/1.log", "foo": "bar"},
// 				Entry: logproto.Entry{
// 					Timestamp: ts,
// 					Line:      logline,
// 				},
// 			}
// 			_, err := c.receiver.Append(context.Background(), logEntry)
// 			require.NoError(t, err)
// 		}
// 	}()

// 	// Call Updates
// 	args1 := Arguments{
// 		ForwardTo: []loki.Appender{app1},
// 		Stages:    stagesCfg.Stages,
// 	}
// 	args2 := Arguments{
// 		ForwardTo: []loki.Appender{app2},
// 		Stages:    stagesCfg.Stages,
// 	}
// 	go func() {
// 		for {
// 			c.Update(args1)
// 			c.Update(args2)
// 		}
// 	}()

// 	// Run everything for a while
// 	time.Sleep(1 * time.Second)
// 	require.WithinDuration(t, time.Now(), lastSend.Load().(time.Time), 300*time.Millisecond)
// }

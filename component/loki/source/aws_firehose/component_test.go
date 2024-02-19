package aws_firehose

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/grafana/regexp"
	"github.com/phayes/freeport"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/grafana/agent/component"
	"github.com/grafana/agent/component/common/loki"
	fnet "github.com/grafana/agent/component/common/net"
	flow_relabel "github.com/grafana/agent/component/common/relabel"
	"github.com/grafana/agent/pkg/util"
)

const singleRecordRequest = `{"requestId":"a1af4300-6c09-4916-ba8f-12f336176246","timestamp":1684422829730,"records":[{"data":"eyJDSEFOR0UiOi0wLjIzLCJQUklDRSI6NC44LCJUSUNLRVJfU1lNQk9MIjoiTkdDIiwiU0VDVE9SIjoiSEVBTFRIQ0FSRSJ9"}]}`

const expectedRecord = "{\"CHANGE\":-0.23,\"PRICE\":4.8,\"TICKER_SYMBOL\":\"NGC\",\"SECTOR\":\"HEALTHCARE\"}"

type testAppender struct {
	mut     sync.RWMutex
	entries []loki.Entry
}

func (t *testAppender) Append(ctx context.Context, entry loki.Entry) (loki.Entry, error) {
	t.mut.Lock()
	defer t.mut.Unlock()
	t.entries = append(t.entries, entry)
	return entry, nil
}

func TestComponent(t *testing.T) {
	opts := component.Options{
		ID:            "loki.source.awsfirehose",
		Logger:        util.TestFlowLogger(t),
		Registerer:    prometheus.NewRegistry(),
		OnStateChange: func(e component.Exports) {},
	}

	app1, app2 := &testAppender{}, &testAppender{}

	args := Arguments{}

	port, err := freeport.GetFreePort()
	require.NoError(t, err)
	args.Server = &fnet.ServerConfig{
		HTTP: &fnet.HTTPConfig{
			ListenAddress: "localhost",
			ListenPort:    port,
		},
		// assign random grpc port
		GRPC: &fnet.GRPCConfig{ListenPort: 0},
	}
	args.ForwardTo = []loki.Appender{app1, app2}

	// Create and run the component.
	c, err := New(opts, args)
	require.NoError(t, err)

	componentCtx, cancelComponent := context.WithCancel(context.Background())
	go c.Run(componentCtx)
	defer cancelComponent()

	// small wait for server start
	time.Sleep(200 * time.Millisecond)

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:%d/awsfirehose/api/v1/push", port), strings.NewReader(singleRecordRequest))
	require.NoError(t, err)

	// create client with timeout
	client := http.Client{
		Timeout: time.Second * 5,
	}

	res, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)

	require.Eventually(t, func() bool {
		app1.mut.RLock()
		app2.mut.RLock()
		defer func() {
			app1.mut.RUnlock()
			app2.mut.RUnlock()
		}()
		return len(app1.entries) == 1 && len(app2.entries) == 1
	}, time.Second*10, time.Second, "timed out waiting for receivers to get all messages")

	// r1 and r2 should have received one entry each
	app1.mut.RLock()
	app2.mut.RLock()
	defer func() {
		app1.mut.RUnlock()
		app2.mut.RUnlock()
	}()
	require.Len(t, app1.entries, 1)
	require.Len(t, app2.entries, 1)
	require.JSONEq(t, expectedRecord, app1.entries[0].Line)
	require.JSONEq(t, expectedRecord, app2.entries[0].Line)
}

func TestComponent_UpdateWithNewArguments(t *testing.T) {
	opts := component.Options{
		ID:            "loki.source.awsfirehose",
		Logger:        util.TestFlowLogger(t),
		Registerer:    prometheus.NewRegistry(),
		OnStateChange: func(e component.Exports) {},
	}

	app1, app2 := &testAppender{}, &testAppender{}

	args := Arguments{}

	port, err := freeport.GetFreePort()
	require.NoError(t, err)

	// port2 will be used to restart server on another port, and test it's relaunched
	port2, err := freeport.GetFreePort()
	require.NoError(t, err)

	args.Server = &fnet.ServerConfig{
		HTTP: &fnet.HTTPConfig{
			ListenAddress: "localhost",
			ListenPort:    port,
		},
		// assign random grpc port
		GRPC: &fnet.GRPCConfig{ListenPort: 0},
	}
	args.ForwardTo = []loki.Appender{app1, app2}
	args.RelabelRules = flow_relabel.Rules{
		{
			SourceLabels: []string{"__aws_firehose_source_arn"},
			Regex:        flow_relabel.Regexp{Regexp: regexp.MustCompile("(.*)")},
			Replacement:  "$1",
			TargetLabel:  "source_arn",
			Action:       flow_relabel.Replace,
		},
	}

	// Create and run the component.
	c, err := New(opts, args)
	require.NoError(t, err)

	componentCtx, cancelComponent := context.WithCancel(context.Background())
	go c.Run(componentCtx)
	defer cancelComponent()

	// small wait for server start
	time.Sleep(200 * time.Millisecond)

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:%d/awsfirehose/api/v1/push", port), strings.NewReader(singleRecordRequest))
	require.NoError(t, err)
	req.Header.Set("X-Amz-Firehose-Source-Arn", "testarn")

	// create client with timeout
	client := http.Client{
		Timeout: time.Second * 5,
	}

	// assert over message received with relabels

	res, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)

	require.Eventually(t, func() bool {
		app1.mut.RLock()
		defer app1.mut.RUnlock()
		return len(app1.entries) == 1
	}, time.Second*10, time.Second, "timed out waiting for receivers to get all messages")

	app1.mut.RLock()
	require.Len(t, app1.entries, 1)
	require.JSONEq(t, expectedRecord, app1.entries[0].Line)
	require.Equal(t, "testarn", string(app1.entries[0].Labels["source_arn"]))
	app1.mut.RUnlock()

	//
	// create new config without relabels, and adding a new forward
	//

	args2 := Arguments{
		ForwardTo: []loki.Appender{app1, app2},
	}
	args2.Server = &fnet.ServerConfig{
		HTTP: &fnet.HTTPConfig{
			ListenAddress: "0.0.0.0",
			ListenPort:    port2,
		},
		GRPC: &fnet.GRPCConfig{ListenPort: 0},
	}
	require.NoError(t, c.Update(args2))
	time.Sleep(200 * time.Millisecond)

	// clear entries entries
	app1.mut.Lock()
	app2.mut.Lock()
	app1.entries = nil
	app2.entries = nil
	app1.mut.Unlock()
	app2.mut.Unlock()

	_, err = client.Do(req)
	require.Error(t, err, "now that the port change, the first request should have errored")

	req2, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:%d/awsfirehose/api/v1/push", port2), strings.NewReader(singleRecordRequest))
	require.NoError(t, err)
	req2.Header.Set("X-Amz-Firehose-Source-Arn", "testarn")

	res, err = client.Do(req2)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)

	require.Eventually(t, func() bool {
		app1.mut.RLock()
		app2.mut.RLock()
		defer func() {
			app1.mut.RUnlock()
			app2.mut.RUnlock()
		}()
		return len(app1.entries) == 1 && len(app2.entries) == 1
	}, time.Second*10, time.Second, "timed out waiting for receivers to get all messages")

	app1.mut.RLock()
	app2.mut.RLock()
	defer func() {
		app1.mut.RUnlock()
		app2.mut.RUnlock()
	}()
	require.Len(t, app1.entries, 1)
	require.Len(t, app2.entries, 1)
	require.JSONEq(t, expectedRecord, app1.entries[0].Line)
	require.NotContains(t, app1.entries[0].Labels, model.LabelName("source_arn"), "expected entries entry to not contain label")
	require.JSONEq(t, expectedRecord, app2.entries[0].Line)
	require.NotContains(t, app2.entries[0].Labels, model.LabelName("source_arn"), "expected entries entry to not contain label")
}

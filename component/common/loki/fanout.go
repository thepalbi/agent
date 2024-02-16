package loki

import (
	"context"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/prometheus/client_golang/prometheus"
)

// compile-time check that Fanout implements loki.Appender
var _ Appender = (*Fanout)(nil)

type Fanout struct {
	mut sync.RWMutex
	// children is where to fan out.
	children []Appender
	// componentID is what component this belongs to.
	componentID    string
	writeLatency   prometheus.Histogram
	samplesCounter prometheus.Counter
}

// NewFanout creates a fanout appendable.
func NewFanout(children []Appender, componentID string, register prometheus.Registerer) *Fanout {
	wl := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "agent_loki_fanout_latency",
		Help: "Write latency for sending to direct and indirect components",
	})
	_ = register.Register(wl)

	s := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agent_loki_forwarded_samples_total",
		Help: "Total number of samples sent to downstream components.",
	})
	_ = register.Register(s)

	return &Fanout{
		children:       children,
		componentID:    componentID,
		writeLatency:   wl,
		samplesCounter: s,
	}
}

// Append implements loki.Appender.
func (f *Fanout) Append(ctx context.Context, entry Entry) (Entry, error) {
	now := time.Now()
	f.mut.RLock()
	defer f.mut.RUnlock()

	var multiErr error
	updated := false
	for _, c := range f.children {
		_, err := c.Append(ctx, entry)
		if err != nil {
			multiErr = multierror.Append(multiErr, err)
		} else {
			updated = true
		}
	}
	if updated {
		f.samplesCounter.Inc()
	}

	f.writeLatency.Observe(time.Since(now).Seconds())
	return entry, multiErr
}

// UpdateChildren allows changing of the children of the fanout.
func (f *Fanout) UpdateChildren(children []Appender) {
	f.mut.Lock()
	defer f.mut.Unlock()
	f.children = children
}

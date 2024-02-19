// The code in this package is adapted/ported over from grafana/loki/clients/pkg/logentry.
//
// The last Loki commit scanned for upstream changes was 7d5475541c66a819f6f456a45f8c143a084e6831.
package process

import (
	"context"
	"reflect"
	"sync"

	"github.com/grafana/agent/component"
	"github.com/grafana/agent/component/common/loki"
	"github.com/grafana/agent/component/loki/process/stages"
)

// TODO(thampiotr): We should reconsider which parts of this component should be exported and which should
//                  be internal before 1.0, specifically the metrics and stages configuration structures.
//					To keep the `stages` package internal, we may need to move the `converter` logic into
//					the `component/loki/process` package.

func init() {
	component.Register(component.Registration{
		Name:    "loki.process",
		Args:    Arguments{},
		Exports: Exports{},
		Build: func(opts component.Options, args component.Arguments) (component.Component, error) {
			return New(opts, args.(Arguments))
		},
	})
}

// Arguments holds values which are used to configure the loki.process
// component.
type Arguments struct {
	ForwardTo []loki.Appender      `river:"forward_to,attr"`
	Stages    []stages.StageConfig `river:"stage,enum,optional"`
}

// Exports exposes the receiver that can be used to send log entries to
// loki.process.
type Exports struct {
	Receiver loki.Appender `river:"receiver,attr"`
}

var (
	_ component.Component = (*Component)(nil)
)

// Component implements the loki.process component.
type Component struct {
	opts component.Options

	mut      sync.RWMutex
	receiver *appenderForwarder
	stages   []stages.StageConfig

	fanoutMut sync.RWMutex
	fanout    *loki.Fanout
}

// New creates a new loki.process component.
func New(o component.Options, args Arguments) (*Component, error) {
	c := &Component{
		opts: o,
	}

	// Create and immediately export the receiver which remains the same for
	// the component's lifetime.
	c.receiver = &appenderForwarder{
		mut: &c.mut,
	}
	c.fanout = loki.NewFanout(args.ForwardTo, o.ID, o.Registerer)
	o.OnStateChange(Exports{Receiver: c.receiver})

	// Call to Update() to start readers and set receivers once at the start.
	if err := c.Update(args); err != nil {
		return nil, err
	}

	return c, nil
}

// maybe instead of using this the component should implement Appender, so it can lock and forward when acquired
// should we be able to drop that lock? at least for the WAL writer?
type appenderForwarder struct {
	mut *sync.RWMutex
	to  loki.Appender
}

func (a *appenderForwarder) Append(ctx context.Context, entry loki.Entry) (loki.Entry, error) {
	a.mut.RLock()
	defer a.mut.RUnlock()
	return a.to.Append(ctx, entry)
}

// Run implements component.Component.
func (c *Component) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

// Update implements component.Component.
func (c *Component) Update(args component.Arguments) error {
	newArgs := args.(Arguments)

	// Update c.fanout first in case anything else fails.
	c.fanoutMut.Lock()
	c.fanout.UpdateChildren(newArgs.ForwardTo)
	c.fanoutMut.Unlock()

	// Then update the pipeline itself.
	c.mut.Lock()
	defer c.mut.Unlock()

	// We want to create a new pipeline if the config changed or if this is the
	// first load. This will allow a component with no stages to function
	// properly.
	if stagesChanged(c.stages, newArgs.Stages) || c.stages == nil {
		pipeline, err := stages.NewPipeline(c.opts.Logger, newArgs.Stages, &c.opts.ID, c.opts.Registerer)
		if err != nil {
			return err
		}
		c.receiver.to = pipeline.Appender(c.fanout)
		c.stages = newArgs.Stages
	}

	return nil
}

func stagesChanged(prev, next []stages.StageConfig) bool {
	if len(prev) != len(next) {
		return true
	}
	for i := range prev {
		if !reflect.DeepEqual(prev[i], next[i]) {
			return true
		}
	}
	return false
}

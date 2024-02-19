package stages

import (
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
)

func Test_StaticLabels(t *testing.T) {
	staticVal := "val"

	tests := []struct {
		name           string
		config         StaticLabelsConfig
		inputLabels    model.LabelSet
		expectedLabels model.LabelSet
	}{
		{
			name: "add static label",
			config: StaticLabelsConfig{Values: map[string]*string{
				"staticLabel": &staticVal,
			}},
			inputLabels: model.LabelSet{
				"testLabel": "testValue",
			},
			expectedLabels: model.LabelSet{
				"testLabel":   "testValue",
				"staticLabel": "val",
			},
		},
		{
			name: "add static label with empty value",
			config: StaticLabelsConfig{Values: map[string]*string{
				"staticLabel": nil,
			}},
			inputLabels: model.LabelSet{
				"testLabel": "testValue",
			},
			expectedLabels: model.LabelSet{
				"testLabel": "testValue",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			st, err := newStaticLabelsStage(nil, test.config)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now()
			labelSet := test.inputLabels.Clone()
			l := ""
			st.Process(labelSet, nil, &now, &l)
			assert.Equal(t, test.expectedLabels, labelSet)
		})
	}
}

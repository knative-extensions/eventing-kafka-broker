package v1alpha1

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKafkaSinkSpecSetDefaults(t *testing.T) {

	defaultMode := ModeStructured

	tests := []struct {
		name string
		spec *KafkaSinkSpec
		want *KafkaSinkSpec
	}{
		{
			name: "structured content mode by default",
			spec: &KafkaSinkSpec{},
			want: &KafkaSinkSpec{
				ContentMode: &defaultMode,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			tt.spec.SetDefaults(context.Background())

			assert.Equal(t, *tt.want, *tt.spec)
		})
	}
}

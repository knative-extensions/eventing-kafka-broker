package config

import (
	"testing"

	eventing "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
)

func TestContentModeFromString(t *testing.T) {
	type args struct {
		mode string
	}
	tests := []struct {
		name string
		args args
		want ContentMode
	}{
		{
			name: eventing.ModeBinary,
			args: args{
				mode: eventing.ModeBinary,
			},
			want: ContentMode_BINARY,
		},
		{
			name: eventing.ModeStructured,
			args: args{
				mode: eventing.ModeStructured,
			},
			want: ContentMode_STRUCTURED,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ContentModeFromString(tt.args.mode); got != tt.want {
				t.Errorf("ContentModeFromString() = %v, want %v", got, tt.want)
			}
		})
	}
}

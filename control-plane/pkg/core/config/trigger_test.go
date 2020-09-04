package config

import (
	"testing"

	"k8s.io/apimachinery/pkg/types"
)

func TestFindTrigger(t *testing.T) {
	type args struct {
		triggers []*Trigger
		trigger  types.UID
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "trigger not found",
			args: args{
				triggers: []*Trigger{
					{
						Attributes: map[string]string{
							"source": "source1",
						},
						Destination: "http://localhost:9090",
						Id:          "2",
					},
				},
				trigger: "1",
			},
			want: NoTrigger,
		},
		{
			name: "trigger found",
			args: args{
				triggers: []*Trigger{
					{
						Attributes: map[string]string{
							"source": "source1",
						},
						Destination: "http://localhost:9090",
						Id:          "2",
					},
					{
						Attributes: map[string]string{
							"source": "source1",
						},
						Destination: "http://localhost:9090",
						Id:          "1",
					},
				},
				trigger: "1",
			},
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FindTrigger(tt.args.triggers, tt.args.trigger); got != tt.want {
				t.Errorf("FindTrigger() = %v, want %v", got, tt.want)
			}
		})
	}
}

package keda

import (
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/autoscaler/keda/mocks"
	"strings"
	"testing"
)

func TestGenerateScaledObjectName(t *testing.T) {
	type args struct {
		obj v1.Object
	}
	tests := []struct {
		name       string
		args       args
		wantPrefix string
		wantSuffix types.UID
	}{
		{
			name: "create scaled object name successfully",
			args: args{
				obj: mocks.ObjectMock{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GenerateScaledObjectName(tt.args.obj)
			soNameLength := len(got)
			if soNameLength != 39 {
				t.Errorf("GenerateScaledObjectName() length = %v, want length %v", len(got), soNameLength)
			}
			if !strings.HasPrefix(got, scaledObjectPrefixName) {
				t.Errorf("GenerateScaledObjectName() prefix = %v, want prefix %v", got, tt.wantPrefix)
			}
			uid := types.UID(strings.TrimLeft(got, scaledObjectPrefixName+"-"))
			uidLength := len(uid)
			if uidLength != 36 {
				t.Errorf("GenerateScaledObjectName() UID length = %v, want length 36", len(string(uid)))
			}
		})
	}
}

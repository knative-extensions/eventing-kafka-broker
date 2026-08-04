/*
 * Copyright 2026 The Knative Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package v1beta1

import (
	"context"
	"fmt"
	"testing"

	authenticationv1 "k8s.io/api/authentication/v1"

	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/system"
	_ "knative.dev/pkg/system/testing"
)

func TestCheckSubscribersChangeAllowed(t *testing.T) {
	subscriberURI := apis.HTTP("example.com")

	original := &KafkaChannel{
		Spec: KafkaChannelSpec{},
	}
	updated := &KafkaChannel{
		Spec: KafkaChannelSpec{
			ChannelableSpec: eventingduck.ChannelableSpec{
				SubscribableSpec: eventingduck.SubscribableSpec{
					Subscribers: []eventingduck.SubscriberSpec{{SubscriberURI: subscriberURI}},
				},
			},
		},
	}

	tests := []struct {
		name     string
		username string
		wantErr  bool
	}{{
		name:     "allowed for the eventing-controller in the configured system namespace",
		username: fmt.Sprintf("system:serviceaccount:%s:eventing-controller", system.Namespace()),
		wantErr:  false,
	}, {
		name:     "denied for the eventing-controller in a different namespace",
		username: "system:serviceaccount:some-other-namespace:eventing-controller",
		wantErr:  true,
	}, {
		name:     "denied for an arbitrary user",
		username: "system:serviceaccount:some-other-namespace:some-other-controller",
		wantErr:  true,
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := apis.WithUserInfo(context.Background(), &authenticationv1.UserInfo{Username: test.username})

			err := updated.CheckSubscribersChangeAllowed(ctx, original)
			if test.wantErr && err == nil {
				t.Errorf("expected an error, got none")
			}
			if !test.wantErr && err != nil {
				t.Errorf("expected no error, got %v", err)
			}
		})
	}
}

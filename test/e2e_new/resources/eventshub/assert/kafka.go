/*
 * Copyright 2021 The Knative Authors
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

package assert

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"time"

	"knative.dev/reconciler-test/pkg/eventshub"
)

// MatchTopic matches the topic of EventInfo
func MatchTopic(want string) eventshub.EventInfoMatcher {
	return func(info eventshub.EventInfo) error {
		actual := ""
		if len(info.AdditionalInfo) != 0 {
			if val, ok := info.AdditionalInfo["topic"].(string); ok {
				actual = val
			}
		}
		if actual != want {
			return fmt.Errorf("event topic don't match. Expected: '%s', Actual: '%s'", want, actual)
		}
		return nil
	}
}

// MatchPartition matches the partition of EventInfo
func MatchPartition(partition int) eventshub.EventInfoMatcher {
	return matchAdditionalInfoInt("partition", partition)
}

// MatchOffset matches the offset of EventInfo
func MatchOffset(offset int) eventshub.EventInfoMatcher {
	return matchAdditionalInfoInt("offset", offset)
}

// MatchKey matches the key of EventInfo
func MatchKey(want []byte) eventshub.EventInfoMatcher {
	return func(info eventshub.EventInfo) error {
		var actual []byte
		if len(info.AdditionalInfo) != 0 {
			if val, ok := info.AdditionalInfo["key"].(string); ok {
				var err error
				actual, err = base64.StdEncoding.DecodeString(val)
				if err != nil {
					return fmt.Errorf("cannot decode the key in EventInfo.AdditionalInfo '%s': %s", val, err)
				}
			}
		}
		if bytes.Compare(actual, want) != 0 {
			return fmt.Errorf("event key don't match. Expected: '%s', Actual: '%s'", want, actual)
		}
		return nil
	}
}

// MatchTimestamp matches the key of EventInfo
func MatchTimestamp(want time.Time) eventshub.EventInfoMatcher {
	return func(info eventshub.EventInfo) error {
		var actual time.Time
		if len(info.AdditionalInfo) != 0 {
			if val, ok := info.AdditionalInfo["timestamp"].(string); ok {
				var err error
				actual, err = time.Parse(time.RFC3339Nano, val)
				if err != nil {
					return fmt.Errorf("cannot parse the key in EventInfo.AdditionalInfo '%s': %s", val, err)
				}
			}
		}
		if !want.Equal(actual) {
			return fmt.Errorf("event timestamp don't match. Expected: '%s', Actual: '%s'", want, actual)
		}
		return nil
	}
}

func matchAdditionalInfoInt(name string, want int) eventshub.EventInfoMatcher {
	return func(info eventshub.EventInfo) error {
		actual := 0
		if len(info.AdditionalInfo) != 0 {
			if val, ok := info.AdditionalInfo[name].(float64); ok {
				actual = int(val)
			}
		}
		if actual != want {
			return fmt.Errorf("event %s don't match. Expected: '%d', Actual: '%d'", name, want, actual)
		}
		return nil
	}
}

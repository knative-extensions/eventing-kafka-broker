/*
Copyright 2020 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ko

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"knative.dev/pkg/logging"
)

// ErrKoPublishFailed is returned when the ko publish command fails.
var ErrKoPublishFailed = errors.New("ko publish failed")

const (
	// publishAttempts is how many times Publish runs ko. ko runs with
	// go run, so a network error while fetching it fails the publish.
	publishAttempts   = 3
	publishRetryDelay = 5 * time.Second
)

// Publish uses ko to publish the image.
func Publish(ctx context.Context, path string) (string, error) {
	version := os.Getenv("GOOGLE_KO_VERSION")
	if version == "" {
		version = "v0.18.0"
	}
	args := []string{
		"go", "run", fmt.Sprintf("github.com/google/ko@%s", version),
		"publish",
	}
	platform := os.Getenv("PLATFORM")
	if len(platform) > 0 {
		args = append(args, "--platform="+platform)
	}
	args = append(args, "-B", path)
	for attempt := 1; ; attempt++ {
		out, err := runCmd(ctx, args)
		if err == nil {
			return out, nil
		}
		if attempt == publishAttempts {
			return "", fmt.Errorf("%w: %v -- command: %q",
				ErrKoPublishFailed, err, args)
		}
		logging.FromContext(ctx).Warnw("ko publish failed, retrying",
			"attempt", attempt, "error", err)
		time.Sleep(publishRetryDelay)
	}
}

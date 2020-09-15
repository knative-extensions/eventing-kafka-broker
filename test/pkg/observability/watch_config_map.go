/*
 * Copyright 2020 The Knative Authors
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

package observability

import (
	"context"
	"google.golang.org/protobuf/encoding/protojson"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"log"
	"sync"

	"github.com/google/go-cmp/cmp"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/injection/sharedmain"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/signals"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
)

const (
	component = "kafka-broker-cm-watcher"
)

// WatchDataPlaneConfigMap watches our data plane config map, and it logs out all differences at every change.
//
// This function is used for troubleshooting failed test runs.
func WatchDataPlaneConfigMap(cm types.NamespacedName, format string) {

	ctx := signals.NewContext()
	cfg := sharedmain.ParseAndGetConfigOrDie()
	ctx = sharedmain.WithHADisabled(ctx)

	sharedmain.MainWithConfig(ctx, component, cfg, func(ctx context.Context, watcher configmap.Watcher) *controller.Impl {

		logger := logging.FromContext(ctx).Desugar()

		diffLogger := diffLogger{
			logger: logger,
			format: format,
		}

		watcher.Watch(cm.Name, diffLogger.logDiff)

		return controller.NewImpl(
			ReconcileFunc(func(ctx context.Context, key string) error {
				return nil
			}),
			logger.Sugar(),
			component,
		)
	})
}

type diffLogger struct {
	m      sync.Mutex
	logger *zap.Logger
	format string
	prev   *contract.Contract
}

func (d *diffLogger) logDiff(cm *corev1.ConfigMap) {

	ct, err := base.GetDataPlaneConfigMapData(d.logger, cm, d.format)
	if err != nil {
		d.logger.Error(
			"failed to get data plane config map data",
			zap.Error(err),
			zap.String("version", cm.ResourceVersion),
		)
		return
	}

	d.m.Lock()
	defer d.m.Unlock()

	sJNow := protojson.Format(ct)
	sJPrev := protojson.Format(d.prev)

	log.Println(cmp.Diff(sJPrev, sJNow))

	d.prev = ct
}

type reconciler struct {
	f func(ctx context.Context, key string) error
}

func (r reconciler) Reconcile(ctx context.Context, key string) error {
	return r.f(ctx, key)
}

func ReconcileFunc(f func(ctx context.Context, key string) error) controller.Reconciler {
	return reconciler{f: f}
}

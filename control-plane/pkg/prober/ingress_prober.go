package prober

import (
	"context"
	"fmt"
	"net/http"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/network"
)

type IngressProbeRequest struct {
	object metav1.Object
	path   string
	pod    corev1.Pod
}

type RequestOption func(r *http.Request) error

type Prober interface {
	Probe(ctx context.Context, pods []*corev1.Pod, object metav1.Object, opts ...RequestOption) error
}

type KeyFunc func(r *http.Request) string

type asyncProber struct {
	onExpire  ExpiredFunc
	keyFunc   KeyFunc
	cache     Cache
	doRequest func(r *http.Request) (*http.Response, error)
}

func (prober *asyncProber) Probe(ctx context.Context, pods []*corev1.Pod, object metav1.Object, opts ...RequestOption) error {

	for _, p := range pods {
		if podIP := p.Status.PodIP; podIP != "" {
			if err := prober.probe(ctx, p, object, opts); err != nil {
				return err // probe already decorates err.
			}
		}
	}

	return nil
}

func (prober *asyncProber) probe(ctx context.Context, pod *corev1.Pod, object metav1.Object, opts []RequestOption) error {
	r, err := http.NewRequest("GET", "", nil)
	if err != nil {
		return fmt.Errorf("failed to create request %w", err)
	}
	for _, op := range opts {
		if err := op(r); err != nil {
			return err
		}
	}

	// We don't want to probe object based on their content, so header values are constant.
	r.Header.Add(network.ProbeHeaderName, "probe")
	r.Header.Add(network.HashHeaderName, "hash")

	go func() {
		response, err := prober.doRequest(r)
		if err != nil {
			prober.cache.UpsertStatus(prober.keyFunc(r), StatusUnknown, object, prober.onExpire)
			logging.FromContext(ctx).Desugar().Error("failed to probe pod",
				zap.String("url", r.URL.String()),
				zap.Error(err),
			)
			return
		}

		if response.StatusCode != http.StatusOK {
			prober.cache.UpsertStatus(prober.keyFunc(r), StatusNotReady, object, prober.onExpire)
			logging.FromContext(ctx).Desugar().Info("probe failed unexpected response status code",
				zap.Int("statusCode", response.StatusCode),
				zap.String("url", r.URL.String()),
				zap.Error(err),
			)
			return
		}
		prober.cache.UpsertStatus(prober.keyFunc(r), StatusReady, object, prober.onExpire)
	}()

	return nil
}

package receiver

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PathFromObject returns an HTTP request path given a generic object.
func PathFromObject(obj metav1.Object) string {
	return Path(obj.GetNamespace(), obj.GetName())
}

// Path returns an HTTP request path given namespace and name of an object.
func Path(namespace, name string) string {
	return fmt.Sprintf("/%s/%s", namespace, name)
}

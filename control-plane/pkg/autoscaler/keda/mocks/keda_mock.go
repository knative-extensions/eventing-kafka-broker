package mocks

import (
	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type ObjectMock struct{}

func (ObjectMock) GetNamespace() string {
	//TODO implement me
	panic("implement me")
}

func (ObjectMock) SetNamespace(namespace string) {
	//TODO implement me
	panic("implement me")
}

func (ObjectMock) GetName() string {
	//TODO implement me
	panic("implement me")
}

func (ObjectMock) SetName(name string) {
	//TODO implement me
	panic("implement me")
}

func (ObjectMock) GetGenerateName() string {
	//TODO implement me
	panic("implement me")
}

func (ObjectMock) SetGenerateName(name string) {
	//TODO implement me
	panic("implement me")
}

func (ObjectMock) GetUID() types.UID {
	return types.UID(uuid.New().String())
}

func (ObjectMock) SetUID(uid types.UID) {
	//TODO implement me
	panic("implement me")
}

func (ObjectMock) GetResourceVersion() string {
	//TODO implement me
	panic("implement me")
}

func (ObjectMock) SetResourceVersion(version string) {
	//TODO implement me
	panic("implement me")
}

func (ObjectMock) GetGeneration() int64 {
	//TODO implement me
	panic("implement me")
}

func (ObjectMock) SetGeneration(generation int64) {
	//TODO implement me
	panic("implement me")
}

func (ObjectMock) GetSelfLink() string {
	//TODO implement me
	panic("implement me")
}

func (ObjectMock) SetSelfLink(selfLink string) {
	//TODO implement me
	panic("implement me")
}

func (ObjectMock) GetCreationTimestamp() v1.Time {
	//TODO implement me
	panic("implement me")
}

func (ObjectMock) SetCreationTimestamp(timestamp v1.Time) {
	//TODO implement me
	panic("implement me")
}

func (ObjectMock) GetDeletionTimestamp() *v1.Time {
	//TODO implement me
	panic("implement me")
}

func (ObjectMock) SetDeletionTimestamp(timestamp *v1.Time) {
	//TODO implement me
	panic("implement me")
}

func (ObjectMock) GetDeletionGracePeriodSeconds() *int64 {
	//TODO implement me
	panic("implement me")
}

func (ObjectMock) SetDeletionGracePeriodSeconds(i *int64) {
	//TODO implement me
	panic("implement me")
}

func (ObjectMock) GetLabels() map[string]string {
	//TODO implement me
	panic("implement me")
}

func (ObjectMock) SetLabels(labels map[string]string) {
	//TODO implement me
	panic("implement me")
}

func (ObjectMock) GetAnnotations() map[string]string {
	//TODO implement me
	panic("implement me")
}

func (ObjectMock) SetAnnotations(annotations map[string]string) {
	//TODO implement me
	panic("implement me")
}

func (ObjectMock) GetFinalizers() []string {
	//TODO implement me
	panic("implement me")
}

func (ObjectMock) SetFinalizers(finalizers []string) {
	//TODO implement me
	panic("implement me")
}

func (ObjectMock) GetOwnerReferences() []v1.OwnerReference {
	//TODO implement me
	panic("implement me")
}

func (ObjectMock) SetOwnerReferences(references []v1.OwnerReference) {
	//TODO implement me
	panic("implement me")
}

func (ObjectMock) GetManagedFields() []v1.ManagedFieldsEntry {
	//TODO implement me
	panic("implement me")
}

func (ObjectMock) SetManagedFields(managedFields []v1.ManagedFieldsEntry) {
	//TODO implement me
	panic("implement me")
}

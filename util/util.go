/*
Copyright 2017 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package util

import (
	"github.com/david-kiko/datax-on-k8s-operator/api/v1beta1"
	"github.com/david-kiko/datax-on-k8s-operator/config"
	"hash"
	"hash/fnv"
	"reflect"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NewHash32 returns a 32-bit hash computed from the given byte slice.
func NewHash32() hash.Hash32 {
	return fnv.New32()
}

// GetOwnerReference returns an OwnerReference pointing to the given app.
func GetOwnerReference(app *v1beta1.DataxApplication) metav1.OwnerReference {
	controller := true
	return metav1.OwnerReference{
		APIVersion: v1beta1.GroupVersion.String(),
		Kind:       reflect.TypeOf(v1beta1.DataxApplication{}).Name(),
		Name:       app.Name,
		UID:        app.UID,
		Controller: &controller,
	}
}

// IsLaunchedBySparkOperator returns whether the given pod is launched by the Datax Operator.
func IsLaunchedBySparkOperator(pod *apiv1.Pod) bool {
	return pod.Labels[config.LaunchedByDataxOperatorLabel] == "true"
}

// IsDriverPod returns whether the given pod is a Datax driver Pod.
func IsDriverPod(pod *apiv1.Pod) bool {
	return pod.Labels[config.DataxRoleLabel] == config.DataxDriverRole
}

// IsExecutorPod returns whether the given pod is a Datax executor Pod.
func IsExecutorPod(pod *apiv1.Pod) bool {
	return pod.Labels[config.DataxRoleLabel] == config.DataxExecutorRole
}

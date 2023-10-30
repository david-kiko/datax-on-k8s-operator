/*
Copyright 2018 Google LLC

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

package controller

import (
	"encoding/json"
	"fmt"
	"github.com/david-kiko/datax-on-k8s-operator/api/v1beta1"
	"github.com/david-kiko/datax-on-k8s-operator/config"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	apiv1 "k8s.io/api/core/v1"
)

// Helper method to create a key with namespace and appName
func createMetaNamespaceKey(namespace, name string) string {
	return fmt.Sprintf("%s/%s", namespace, name)
}

func createRequestKey(namespace, name string) reconcile.Request {
	return reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: name},
	}
}

func getAppName(pod *apiv1.Pod) (string, bool) {
	appName, ok := pod.Labels[config.DataxAppNameLabel]
	return appName, ok
}

func getDataxApplicationID(pod *apiv1.Pod) string {
	return pod.Labels[config.DataxApplicationSelectorLabel]
}

func getDriverPodName(app *v1beta1.DataxApplication) string {
	name := app.Spec.Driver.PodName
	if name != nil && len(*name) > 0 {
		return *name
	}

	return fmt.Sprintf("%s-driver", app.Name)
}

func getResourceLabels(app *v1beta1.DataxApplication) map[string]string {
	labels := map[string]string{config.DataxAppNameLabel: app.Name}
	if app.Status.SubmissionID != "" {
		labels[config.SubmissionIDLabel] = app.Status.SubmissionID
	}
	return labels
}

func podPhaseToExecutorState(podPhase apiv1.PodPhase) v1beta1.ExecutorState {
	switch podPhase {
	case apiv1.PodPending:
		return v1beta1.ExecutorPendingState
	case apiv1.PodRunning:
		return v1beta1.ExecutorRunningState
	case apiv1.PodSucceeded:
		return v1beta1.ExecutorCompletedState
	case apiv1.PodFailed:
		return v1beta1.ExecutorFailedState
	default:
		return v1beta1.ExecutorUnknownState
	}
}

func isExecutorTerminated(executorState v1beta1.ExecutorState) bool {
	return executorState == v1beta1.ExecutorCompletedState || executorState == v1beta1.ExecutorFailedState
}

func isDriverRunning(app *v1beta1.DataxApplication) bool {
	return app.Status.AppState.State == v1beta1.RunningState
}

func getDriverContainerTerminatedState(podStatus apiv1.PodStatus) *apiv1.ContainerStateTerminated {
	return getContainerTerminatedState(config.DataxDriverContainerName, podStatus)
}

func getExecutorContainerTerminatedState(podStatus apiv1.PodStatus) *apiv1.ContainerStateTerminated {
	return getContainerTerminatedState(config.DataxExecutorContainerName, podStatus)
}

func getContainerTerminatedState(name string, podStatus apiv1.PodStatus) *apiv1.ContainerStateTerminated {
	for _, c := range podStatus.ContainerStatuses {
		if c.Name == name {
			if c.State.Terminated != nil {
				return c.State.Terminated
			}
			return nil
		}
	}
	return nil
}

func podStatusToDriverState(podStatus apiv1.PodStatus) v1beta1.DriverState {
	switch podStatus.Phase {
	case apiv1.PodPending:
		return v1beta1.DriverPendingState
	case apiv1.PodRunning:
		state := getDriverContainerTerminatedState(podStatus)
		if state != nil {
			if state.ExitCode == 0 {
				return v1beta1.DriverCompletedState
			}
			return v1beta1.DriverFailedState
		}
		return v1beta1.DriverRunningState
	case apiv1.PodSucceeded:
		return v1beta1.DriverCompletedState
	case apiv1.PodFailed:
		state := getDriverContainerTerminatedState(podStatus)
		if state != nil && state.ExitCode == 0 {
			return v1beta1.DriverCompletedState
		}
		return v1beta1.DriverFailedState
	default:
		return v1beta1.DriverUnknownState
	}
}

func hasDriverTerminated(driverState v1beta1.DriverState) bool {
	return driverState == v1beta1.DriverCompletedState || driverState == v1beta1.DriverFailedState
}

func driverStateToApplicationState(driverState v1beta1.DriverState) v1beta1.ApplicationStateType {
	switch driverState {
	case v1beta1.DriverPendingState:
		return v1beta1.SubmittedState
	case v1beta1.DriverCompletedState:
		return v1beta1.SucceedingState
	case v1beta1.DriverFailedState:
		return v1beta1.FailingState
	case v1beta1.DriverRunningState:
		return v1beta1.RunningState
	default:
		return v1beta1.UnknownState
	}
}

func printStatus(status *v1beta1.DataxApplicationStatus) (string, error) {
	marshalled, err := json.MarshalIndent(status, "", "  ")
	if err != nil {
		return "", err
	}
	return string(marshalled), nil
}

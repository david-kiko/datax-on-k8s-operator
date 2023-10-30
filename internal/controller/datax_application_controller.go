/*
Copyright 2023 ldw54@126.com.

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

package controller

import (
	"context"
	"fmt"
	"github.com/david-kiko/datax-on-k8s-operator/batchscheduler"
	schedulerinterface "github.com/david-kiko/datax-on-k8s-operator/batchscheduler/interface"
	"github.com/david-kiko/datax-on-k8s-operator/config"
	"github.com/david-kiko/datax-on-k8s-operator/util"
	"github.com/golang/glog"
	"github.com/google/uuid"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"time"

	"github.com/david-kiko/datax-on-k8s-operator/api/v1beta1"
)

// DataxApplicationReconciler reconciles a DataxApplication object
type DataxApplicationReconciler struct {
	client.Client
	Scheme            *runtime.Scheme
	Recorder          record.EventRecorder
	BatchSchedulerMgr *batchscheduler.SchedulerManager
}

//+kubebuilder:rbac:groups=dataxoperator.tech.io,resources=dataxapplications,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=dataxoperator.tech.io,resources=dataxapplications/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=dataxoperator.tech.io,resources=dataxapplications/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the DataxApplication object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.15.0/pkg/reconcile
func (d *DataxApplicationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = log.FromContext(ctx)
	ctx = context.Background()

	err := d.syncDataxApplication(ctx, req)

	if err != nil {

	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (d *DataxApplicationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	podEventHandler := dataxPodEventHandler{}
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1beta1.DataxApplication{}).
		Watches(
			&apiv1.Pod{},
			handler.Funcs{
				CreateFunc: podEventHandler.onPodAdd,
				UpdateFunc: podEventHandler.onPodUpdate,
				DeleteFunc: podEventHandler.onDelete,
			},
			builder.WithPredicates(predicate.ResourceVersionChangedPredicate{}),
		).
		Complete(d)
}

func (d *DataxApplicationReconciler) syncDataxApplication(ctx context.Context, req ctrl.Request) error {
	app, err := d.getDataxApplication(ctx, req)

	if err != nil {
		return err
	}
	if app == nil {
		// SparkApplication not found.
		return nil
	}

	if !app.DeletionTimestamp.IsZero() {
		d.handleDataxApplicationDeletion(ctx, req, app)
		return nil
	}

	appCopy := app.DeepCopy()
	// Apply the default values to the copy. Note that the default values applied
	// won't be sent to the API server as we only update the /status subresource.
	v1beta1.SetDataxApplicationDefaults(appCopy)

	// Take action based on application state.
	switch appCopy.Status.AppState.State {
	case v1beta1.NewState:
		d.recordDataxApplicationEvent(appCopy)
		if err := d.validateDataxApplication(appCopy); err != nil {
			appCopy.Status.AppState.State = v1beta1.FailedState
			appCopy.Status.AppState.ErrorMessage = err.Error()
		} else {
			appCopy = d.submitDataxApplication(ctx, appCopy)
		}
	case v1beta1.SucceedingState:
		if !shouldRetry(appCopy) {
			appCopy.Status.AppState.State = v1beta1.CompletedState
			d.recordDataxApplicationEvent(appCopy)
		} else {
			if err := d.deleteDataxResources(ctx, req, appCopy); err != nil {
				glog.Errorf("failed to delete resources associated with DataxApplication %s/%s: %v",
					appCopy.Namespace, appCopy.Name, err)
				return err
			}
			appCopy.Status.AppState.State = v1beta1.PendingRerunState
		}
	case v1beta1.FailingState:
		if !shouldRetry(appCopy) {
			appCopy.Status.AppState.State = v1beta1.FailedState
			d.recordDataxApplicationEvent(appCopy)
		} else if isNextRetryDue(appCopy.Spec.RestartPolicy.OnFailureRetryInterval, appCopy.Status.ExecutionAttempts, appCopy.Status.TerminationTime) {
			if err := d.deleteDataxResources(ctx, req, appCopy); err != nil {
				glog.Errorf("failed to delete resources associated with SparkApplication %s/%s: %v",
					appCopy.Namespace, appCopy.Name, err)
				return err
			}
			appCopy.Status.AppState.State = v1beta1.PendingRerunState
		}
	case v1beta1.FailedSubmissionState:
		if !shouldRetry(appCopy) {
			// App will never be retried. Move to terminal FailedState.
			appCopy.Status.AppState.State = v1beta1.FailedState
			d.recordDataxApplicationEvent(appCopy)
		} else if isNextRetryDue(appCopy.Spec.RestartPolicy.OnSubmissionFailureRetryInterval, appCopy.Status.SubmissionAttempts, appCopy.Status.LastSubmissionAttemptTime) {
			if d.validateDataxResourceDeletion(ctx, req, appCopy) {
				d.submitDataxApplication(ctx, appCopy)
			} else {
				if err := d.deleteDataxResources(ctx, req, appCopy); err != nil {
					glog.Errorf("failed to delete resources associated with SparkApplication %s/%s: %v",
						appCopy.Namespace, appCopy.Name, err)
					return err
				}
			}
		}
	case v1beta1.InvalidatingState:
		// Invalidate the current run and enqueue the SparkApplication for re-execution.
		if err := d.deleteDataxResources(ctx, req, appCopy); err != nil {
			glog.Errorf("failed to delete resources associated with SparkApplication %s/%s: %v",
				appCopy.Namespace, appCopy.Name, err)
			return err
		}
		d.clearStatus(&appCopy.Status)
		appCopy.Status.AppState.State = v1beta1.PendingRerunState
	case v1beta1.PendingRerunState:
		glog.V(2).Infof("SparkApplication %s/%s is pending rerun", appCopy.Namespace, appCopy.Name)
		if d.validateDataxResourceDeletion(ctx, req, appCopy) {
			glog.V(2).Infof("Resources for SparkApplication %s/%s successfully deleted", appCopy.Namespace, appCopy.Name)
			d.recordDataxApplicationEvent(appCopy)
			d.clearStatus(&appCopy.Status)
			appCopy = d.submitDataxApplication(ctx, appCopy)
		}
	case v1beta1.SubmittedState, v1beta1.RunningState, v1beta1.UnknownState:
		if err := d.getAndUpdateAppState(appCopy); err != nil {
			return err
		}
	case v1beta1.CompletedState, v1beta1.FailedState:
		if d.hasApplicationExpired(app) {
			glog.Infof("Garbage collecting expired DataxApplication %s/%s", app.Namespace, app.Name)
			err := d.Client.Delete(ctx, app, &client.DeleteOptions{GracePeriodSeconds: int64ptr(0)})
			if err != nil && !errors.IsNotFound(err) {
				return err
			}
			return nil
		}
		if err := d.getAndUpdateExecutorState(appCopy); err != nil {
			return err
		}
	}

	if appCopy != nil {
		err = d.updateStatus(app, appCopy)
		if err != nil {
			glog.Errorf("failed to update SparkApplication %s/%s: %v", app.Namespace, app.Name, err)
			return err
		}

		if state := appCopy.Status.AppState.State; state == v1beta1.CompletedState ||
			state == v1beta1.FailedState {
			if err := d.cleanUpOnTermination(app, appCopy); err != nil {
				glog.Errorf("failed to clean up resources for SparkApplication %s/%s: %v", app.Namespace, app.Name, err)
				return err
			}
		}
	}

	return nil
}

// Helper func to determine if the next retry the DataxApplication is due now.
func isNextRetryDue(retryInterval *int64, attemptsDone int32, lastEventTime metav1.Time) bool {
	if retryInterval == nil || lastEventTime.IsZero() || attemptsDone <= 0 {
		return false
	}

	// Retry if we have waited at-least equal to attempts*RetryInterval since we do a linear back-off.
	interval := time.Duration(*retryInterval) * time.Second * time.Duration(attemptsDone)
	currentTime := time.Now()
	glog.V(3).Infof("currentTime is %v, interval is %v", currentTime, interval)
	if currentTime.After(lastEventTime.Add(interval)) {
		return true
	}
	return false
}

// ShouldRetry determines if DataxApplication in a given state should be retried.
func shouldRetry(app *v1beta1.DataxApplication) bool {
	switch app.Status.AppState.State {
	case v1beta1.SucceedingState:
		return app.Spec.RestartPolicy.Type == v1beta1.Always
	case v1beta1.FailingState:
		if app.Spec.RestartPolicy.Type == v1beta1.Always {
			return true
		} else if app.Spec.RestartPolicy.Type == v1beta1.OnFailure {
			// We retry if we haven't hit the retry limit.
			if app.Spec.RestartPolicy.OnFailureRetries != nil && app.Status.ExecutionAttempts <= *app.Spec.RestartPolicy.OnFailureRetries {
				return true
			}
		}
	case v1beta1.FailedSubmissionState:
		if app.Spec.RestartPolicy.Type == v1beta1.Always {
			return true
		} else if app.Spec.RestartPolicy.Type == v1beta1.OnFailure {
			// We retry if we haven't hit the retry limit.
			if app.Spec.RestartPolicy.OnSubmissionFailureRetries != nil && app.Status.SubmissionAttempts <= *app.Spec.RestartPolicy.OnSubmissionFailureRetries {
				return true
			}
		}
	}
	return false
}

func (d *DataxApplicationReconciler) getDataxApplication(ctx context.Context, req ctrl.Request) (*v1beta1.DataxApplication, error) {
	obj := &v1beta1.DataxApplication{}
	err := d.Client.Get(ctx, req.NamespacedName, obj, &client.GetOptions{})

	if err != nil {
		if errors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return obj, nil
}

func (d *DataxApplicationReconciler) handleDataxApplicationDeletion(ctx context.Context, req ctrl.Request, app *v1beta1.DataxApplication) {
	if err := d.deleteDataxResources(ctx, req, app); err != nil {
		glog.Errorf("failed to delete resources associated with deleted DataxApplication %s/%s: %v", app.Namespace, app.Name, err)
	}
}

func (d *DataxApplicationReconciler) deleteDataxResources(ctx context.Context, req ctrl.Request, app *v1beta1.DataxApplication) error {
	driverPodName := app.Status.DriverInfo.PodName
	// Derive the driver pod name in case the driver pod name was not recorded in the status,
	// which could happen if the status update right after submission failed.
	if driverPodName == "" {
		driverPodName = getDriverPodName(app)
	}

	driverPod := &apiv1.Pod{}

	err := d.Client.Get(ctx, req.NamespacedName, driverPod, &client.GetOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	glog.V(2).Infof("Deleting pod %s in namespace %s", driverPodName, app.Namespace)

	err = d.Client.Delete(ctx, driverPod, &client.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	return nil
}

func (d *DataxApplicationReconciler) recordDataxApplicationEvent(app *v1beta1.DataxApplication) {
	switch app.Status.AppState.State {
	case v1beta1.NewState:
		d.Recorder.Eventf(
			app,
			apiv1.EventTypeNormal,
			"DataxApplicationAdded",
			"DataxApplication %s was added, enqueuing it for submission",
			app.Name)
	case v1beta1.SubmittedState:
		d.Recorder.Eventf(
			app,
			apiv1.EventTypeNormal,
			"DataxApplicationSubmitted",
			"DataxApplication %s was submitted successfully",
			app.Name)
	case v1beta1.FailedSubmissionState:
		d.Recorder.Eventf(
			app,
			apiv1.EventTypeWarning,
			"DataxApplicationSubmissionFailed",
			"failed to submit DataxApplication %s: %s",
			app.Name,
			app.Status.AppState.ErrorMessage)
	case v1beta1.CompletedState:
		d.Recorder.Eventf(
			app,
			apiv1.EventTypeNormal,
			"DataxApplicationCompleted",
			"DataxApplication %s completed",
			app.Name)
	case v1beta1.FailedState:
		d.Recorder.Eventf(
			app,
			apiv1.EventTypeWarning,
			"DataxApplicationFailed",
			"DataxApplication %s failed: %s",
			app.Name,
			app.Status.AppState.ErrorMessage)
	case v1beta1.PendingRerunState:
		d.Recorder.Eventf(
			app,
			apiv1.EventTypeWarning,
			"DataxApplicationPendingRerun",
			"DataxApplication %s is pending rerun",
			app.Name)
	}
}

func (d *DataxApplicationReconciler) validateDataxApplication(app *v1beta1.DataxApplication) error {
	appSpec := app.Spec
	driverSpec := appSpec.Driver
	executorSpec := appSpec.Executor
	if appSpec.NodeSelector != nil && (driverSpec.NodeSelector != nil || executorSpec.NodeSelector != nil) {
		return fmt.Errorf("NodeSelector property can be defined at DataxApplication or at any of Driver,Executor")
	}

	return nil
}

func (d *DataxApplicationReconciler) submitDataxApplication(ctx context.Context, app *v1beta1.DataxApplication) *v1beta1.DataxApplication {
	// Use batch scheduler to perform scheduling task before submitting (before build command arguments).
	annotations := make(map[string]string)
	if needScheduling, scheduler := d.shouldDoBatchScheduling(app); needScheduling {
		err := scheduler.DoBatchSchedulingOnSubmission(app)
		if err != nil {
			glog.Errorf("failed to process batch scheduler BeforeSubmitSparkApplication with error %v", err)
			return app
		}
		annotations = scheduler.GetAnnotations(app)
	}

	driverInfo := v1beta1.DriverInfo{}
	driverPodName := getDriverPodName(app)
	driverInfo.PodName = driverPodName
	submissionID := uuid.New().String()

	driverPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        driverPodName,
			Namespace:   app.Namespace,
			Annotations: annotations,
			Labels: map[string]string{
				config.SubmissionIDLabel: submissionID,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(app, v1beta1.GroupVersionKind),
			},
		},
		Spec: apiv1.PodSpec{
			Volumes:        app.Spec.Volumes,
			InitContainers: app.Spec.Driver.InitContainers,
			Containers: []apiv1.Container{
				{
					Name:  config.DataxDriverContainerName,
					Image: app.Spec.Driver.Image,
					Ports: []apiv1.ContainerPort{
						{
							Name:          "tcp",
							Protocol:      apiv1.ProtocolTCP,
							ContainerPort: app.Spec.Driver.DriverPort,
						},
					},
					ImagePullPolicy: app.Spec.ImagePullPolicy,
					Env: []apiv1.EnvVar{
						{
							Name:  config.DataxExecutorContainerImageKeyEnv,
							Value: app.Spec.Executor.Image,
						},
						{
							Name:  config.DataxExecutorCoreRequestKeyEnv,
							Value: app.Spec.Executor.CoreRequest,
						},
						{
							Name:  config.DataxExecutorCoreLimitKeyEnv,
							Value: app.Spec.Executor.CoreLimit,
						},
						{
							Name:  config.DataxExecutorMemoryRequestKeyEnv,
							Value: app.Spec.Executor.MemoryRequest,
						},
						{
							Name:  config.DataxExecutorMemoryLimitKeyEnv,
							Value: app.Spec.Executor.MemoryLimit,
						},
					},
				},
			},
			NodeSelector: app.Spec.NodeSelector,
		},
	}
	if err := d.Client.Create(ctx, driverPod); err != nil {
		app.Status = v1beta1.DataxApplicationStatus{
			AppState: v1beta1.ApplicationState{
				State:        v1beta1.FailedSubmissionState,
				ErrorMessage: err.Error(),
			},
			SubmissionAttempts:        app.Status.SubmissionAttempts + 1,
			LastSubmissionAttemptTime: metav1.Now(),
		}
		d.recordDataxApplicationEvent(app)
		glog.Errorf("failed to run driver pod for DataxApplication %s/%s: %v", app.Namespace, app.Name, err)
		return app
	}

	glog.Infof("DataxApplication %s/%s has been submitted", app.Namespace, app.Name)
	app.Status = v1beta1.DataxApplicationStatus{
		SubmissionID: submissionID,
		AppState: v1beta1.ApplicationState{
			State: v1beta1.SubmittedState,
		},
		DriverInfo:                driverInfo,
		SubmissionAttempts:        app.Status.SubmissionAttempts + 1,
		ExecutionAttempts:         app.Status.ExecutionAttempts + 1,
		LastSubmissionAttemptTime: metav1.Now(),
	}
	d.recordDataxApplicationEvent(app)

	return app
}

func (d *DataxApplicationReconciler) shouldDoBatchScheduling(app *v1beta1.DataxApplication) (bool, schedulerinterface.BatchScheduler) {
	if d.BatchSchedulerMgr == nil || app.Spec.BatchScheduler == nil || *app.Spec.BatchScheduler == "" {
		return false, nil
	}

	scheduler, err := d.BatchSchedulerMgr.GetScheduler(*app.Spec.BatchScheduler)
	if err != nil {
		glog.Errorf("failed to get batch scheduler for name %s, %v", *app.Spec.BatchScheduler, err)
		return false, nil
	}
	return scheduler.ShouldSchedule(app), scheduler
}

func (d *DataxApplicationReconciler) validateDataxResourceDeletion(ctx context.Context, req ctrl.Request, app *v1beta1.DataxApplication) bool {
	driverPodName := app.Status.DriverInfo.PodName
	// Derive the driver pod name in case the driver pod name was not recorded in the status,
	// which could happen if the status update right after submission failed.
	if driverPodName == "" {
		driverPodName = getDriverPodName(app)
	}
	err := d.Client.Get(ctx, req.NamespacedName, &apiv1.Pod{}, &client.GetOptions{})
	if err == nil || !errors.IsNotFound(err) {
		return false
	}

	return true
}

func (d *DataxApplicationReconciler) clearStatus(status *v1beta1.DataxApplicationStatus) {
	if status.AppState.State == v1beta1.InvalidatingState {
		status.DataxApplicationID = ""
		status.SubmissionAttempts = 0
		status.ExecutionAttempts = 0
		status.LastSubmissionAttemptTime = metav1.Time{}
		status.TerminationTime = metav1.Time{}
		status.AppState.ErrorMessage = ""
		status.ExecutorState = nil
	} else if status.AppState.State == v1beta1.PendingRerunState {
		status.DataxApplicationID = ""
		status.SubmissionAttempts = 0
		status.LastSubmissionAttemptTime = metav1.Time{}
		status.DriverInfo = v1beta1.DriverInfo{}
		status.AppState.ErrorMessage = ""
		status.ExecutorState = nil
	}
}

func (d *DataxApplicationReconciler) hasApplicationExpired(app *v1beta1.DataxApplication) bool {
	// The application has no TTL defined and will never expire.
	if app.Spec.TimeToLiveSeconds == nil {
		return false
	}

	ttl := time.Duration(*app.Spec.TimeToLiveSeconds) * time.Second
	now := time.Now()
	if !app.Status.TerminationTime.IsZero() && now.Sub(app.Status.TerminationTime.Time) > ttl {
		return true
	}

	return false
}

func (d *DataxApplicationReconciler) getAndUpdateAppState(app *v1beta1.DataxApplication) error {
	if err := d.getAndUpdateDriverState(app); err != nil {
		return err
	}
	if err := d.getAndUpdateExecutorState(app); err != nil {
		return err
	}
	return nil
}

func (d *DataxApplicationReconciler) getAndUpdateDriverState(app *v1beta1.DataxApplication) error {
	// Either the driver pod doesn't exist yet or its name has not been updated.
	if app.Status.DriverInfo.PodName == "" {
		return fmt.Errorf("empty driver pod name with application state %s", app.Status.AppState.State)
	}

	driverPod, err := d.getDriverPod(app)
	if err != nil {
		return err
	}

	if driverPod == nil {
		app.Status.AppState.ErrorMessage = "driver pod not found"
		app.Status.AppState.State = v1beta1.FailingState
		app.Status.TerminationTime = metav1.Now()
		return nil
	}

	app.Status.DataxApplicationID = getDataxApplicationID(driverPod)
	driverState := podStatusToDriverState(driverPod.Status)

	if hasDriverTerminated(driverState) {
		if app.Status.TerminationTime.IsZero() {
			app.Status.TerminationTime = metav1.Now()
		}
		if driverState == v1beta1.DriverFailedState {
			state := getDriverContainerTerminatedState(driverPod.Status)
			if state != nil {
				if state.ExitCode != 0 {
					app.Status.AppState.ErrorMessage = fmt.Sprintf("driver container failed with ExitCode: %d, Reason: %s", state.ExitCode, state.Reason)
				}
			} else {
				app.Status.AppState.ErrorMessage = "driver container status missing"
			}
		}
	}

	newState := driverStateToApplicationState(driverState)
	// Only record a driver event if the application state (derived from the driver pod phase) has changed.
	if newState != app.Status.AppState.State {
		d.recordDriverEvent(app, driverState, driverPod.Name)
		app.Status.AppState.State = newState
	}

	return nil
}

func (d *DataxApplicationReconciler) getDriverPod(app *v1beta1.DataxApplication) (*apiv1.Pod, error) {
	podNamespaced := getPodNamespaced(app)
	pod := &apiv1.Pod{}

	// The driver pod was not found in the informer cache, try getting it directly from the API server.
	err := d.Client.Get(context.Background(), podNamespaced, pod, &client.GetOptions{})
	if err == nil {
		return pod, nil
	}
	if !errors.IsNotFound(err) {
		return nil, fmt.Errorf("failed to get driver pod %s: %v", app.Status.DriverInfo.PodName, err)
	}
	// Driver pod was not found on the API server either.
	return nil, nil
}

func (d *DataxApplicationReconciler) recordDriverEvent(app *v1beta1.DataxApplication, phase v1beta1.DriverState, name string) {
	switch phase {
	case v1beta1.DriverCompletedState:
		d.Recorder.Eventf(app, apiv1.EventTypeNormal, "DataxDriverCompleted", "Driver %s completed", name)
	case v1beta1.DriverPendingState:
		d.Recorder.Eventf(app, apiv1.EventTypeNormal, "DataxDriverPending", "Driver %s is pending", name)
	case v1beta1.DriverRunningState:
		d.Recorder.Eventf(app, apiv1.EventTypeNormal, "DataxDriverRunning", "Driver %s is running", name)
	case v1beta1.DriverFailedState:
		d.Recorder.Eventf(app, apiv1.EventTypeWarning, "DataxDriverFailed", "Driver %s failed", name)
	case v1beta1.DriverUnknownState:
		d.Recorder.Eventf(app, apiv1.EventTypeWarning, "DataxDriverUnknownState", "Driver %s in unknown state", name)
	}
}

// getAndUpdateExecutorState lists the executor pods of the application
// and updates the executor state based on the current phase of the pods.
func (d *DataxApplicationReconciler) getAndUpdateExecutorState(app *v1beta1.DataxApplication) error {
	pods, err := d.getExecutorPods(app)
	if err != nil {
		return err
	}

	executorStateMap := make(map[string]v1beta1.ExecutorState)
	var executorApplicationID string
	for _, pod := range pods.Items {
		if util.IsExecutorPod(&pod) {
			newState := podPhaseToExecutorState(pod.Status.Phase)
			oldState, exists := app.Status.ExecutorState[pod.Name]
			// Only record an executor event if the executor state is new or it has changed.
			if !exists || newState != oldState {
				if newState == v1beta1.ExecutorFailedState {
					execContainerState := getExecutorContainerTerminatedState(pod.Status)
					if execContainerState != nil {
						d.recordExecutorEvent(app, newState, pod.Name, execContainerState.ExitCode, execContainerState.Reason)
					} else {
						// If we can't find the container state,
						// we need to set the exitCode and the Reason to unambiguous values.
						d.recordExecutorEvent(app, newState, pod.Name, -1, "Unknown (Container not Found)")
					}
				} else {
					d.recordExecutorEvent(app, newState, pod.Name)
				}
			}
			executorStateMap[pod.Name] = newState

			if executorApplicationID == "" {
				executorApplicationID = getDataxApplicationID(&pod)
			}
		}
	}

	// ApplicationID label can be different on driver/executors. Prefer executor ApplicationID if set.
	// Refer https://issues.apache.org/jira/projects/SPARK/issues/SPARK-25922 for details.
	if executorApplicationID != "" {
		app.Status.DataxApplicationID = executorApplicationID
	}

	if app.Status.ExecutorState == nil {
		app.Status.ExecutorState = make(map[string]v1beta1.ExecutorState)
	}
	for name, execStatus := range executorStateMap {
		app.Status.ExecutorState[name] = execStatus
	}

	// Handle missing/deleted executors.
	for name, oldStatus := range app.Status.ExecutorState {
		_, exists := executorStateMap[name]
		if !isExecutorTerminated(oldStatus) && !exists {
			if !isDriverRunning(app) {
				// If ApplicationState is COMPLETED, in other words, the driver pod has been completed
				// successfully. The executor pods terminate and are cleaned up, so we could not found
				// the executor pod, under this circumstances, we assume the executor pod are completed.
				if app.Status.AppState.State == v1beta1.CompletedState {
					app.Status.ExecutorState[name] = v1beta1.ExecutorCompletedState
				} else {
					glog.Infof("Executor pod %s not found, assuming it was deleted.", name)
					app.Status.ExecutorState[name] = v1beta1.ExecutorFailedState
				}
			} else {
				app.Status.ExecutorState[name] = v1beta1.ExecutorUnknownState
			}
		}
	}

	return nil
}

func (d *DataxApplicationReconciler) getExecutorPods(app *v1beta1.DataxApplication) (*apiv1.PodList, error) {
	matchLabels := getResourceLabels(app)
	matchLabels[config.DataxRoleLabel] = config.DataxExecutorRole
	// Fetch all the executor pods for the current run of the application.
	selector := labels.SelectorFromSet(labels.Set(matchLabels))

	pods := &apiv1.PodList{}
	err := d.Client.List(context.Background(), pods, &client.ListOptions{Namespace: app.Namespace, LabelSelector: selector})
	if err != nil {
		return nil, fmt.Errorf("failed to get pods for SparkApplication %s/%s: %v", app.Namespace, app.Name, err)
	}
	return pods, nil
}

func (d *DataxApplicationReconciler) updateStatus(oldApp, newApp *v1beta1.DataxApplication) error {
	// Skip update if nothing changed.
	if equality.Semantic.DeepEqual(oldApp.Status, newApp.Status) {
		return nil
	}

	oldStatusJSON, err := printStatus(&oldApp.Status)
	if err != nil {
		return err
	}
	newStatusJSON, err := printStatus(&newApp.Status)
	if err != nil {
		return err
	}

	glog.V(2).Infof("Update the status of DataxApplication %s/%s from:\n%s\nto:\n%s", newApp.Namespace, newApp.Name, oldStatusJSON, newStatusJSON)
	_, err = d.updateApplication(oldApp, func(status *v1beta1.DataxApplicationStatus) {
		*status = newApp.Status
	})
	if err != nil {
		return err
	}
	return nil
}

func (d *DataxApplicationReconciler) updateApplication(
	original *v1beta1.DataxApplication,
	updateFunc func(status *v1beta1.DataxApplicationStatus)) (*v1beta1.DataxApplication, error) {
	toUpdate := original.DeepCopy()
	updateErr := wait.ExponentialBackoff(retry.DefaultBackoff, func() (ok bool, err error) {
		updateFunc(&toUpdate.Status)
		if equality.Semantic.DeepEqual(original.Status, toUpdate.Status) {
			return true, nil
		}

		err = d.Client.Status().Update(context.Background(), toUpdate, &client.SubResourceUpdateOptions{})

		if err == nil {
			return true, nil
		}
		if !errors.IsConflict(err) {
			return false, err
		}

		// There was a conflict updating the DataxApplication, fetch the latest version from the API server.
		namespaced := getNamespaced(toUpdate)
		err = d.Client.Get(context.TODO(), namespaced, toUpdate, &client.GetOptions{})
		if err != nil {
			glog.Errorf("failed to get DataxApplication %s/%s: %v", original.Namespace, original.Name, err)
			return false, err
		}

		// Retry with the latest version.
		return false, nil
	})

	if updateErr != nil {
		glog.Errorf("failed to update SparkApplication %s/%s: %v", original.Namespace, original.Name, updateErr)
		return nil, updateErr
	}

	return toUpdate, nil
}

func (d *DataxApplicationReconciler) cleanUpOnTermination(oldApp, newApp *v1beta1.DataxApplication) error {
	if needScheduling, scheduler := d.shouldDoBatchScheduling(newApp); needScheduling {
		if err := scheduler.CleanupOnCompletion(newApp); err != nil {
			return err
		}
	}
	return nil
}

func (d *DataxApplicationReconciler) recordExecutorEvent(app *v1beta1.DataxApplication, state v1beta1.ExecutorState, args ...interface{}) {
	switch state {
	case v1beta1.ExecutorCompletedState:
		d.Recorder.Eventf(app, apiv1.EventTypeNormal, "DataxExecutorCompleted", "Executor %s completed", args)
	case v1beta1.ExecutorPendingState:
		d.Recorder.Eventf(app, apiv1.EventTypeNormal, "DataxExecutorPending", "Executor %s is pending", args)
	case v1beta1.ExecutorRunningState:
		d.Recorder.Eventf(app, apiv1.EventTypeNormal, "DataxExecutorRunning", "Executor %s is running", args)
	case v1beta1.ExecutorFailedState:
		d.Recorder.Eventf(app, apiv1.EventTypeWarning, "DataxExecutorFailed", "Executor %s failed with ExitCode: %d, Reason: %s", args)
	case v1beta1.ExecutorUnknownState:
		d.Recorder.Eventf(app, apiv1.EventTypeWarning, "DataxExecutorUnknownState", "Executor %s in unknown state", args)
	}
}

func getPodNamespaced(app *v1beta1.DataxApplication) types.NamespacedName {
	return types.NamespacedName{
		Namespace: app.Namespace,
		Name:      app.Status.DriverInfo.PodName,
	}
}

func getNamespaced(app *v1beta1.DataxApplication) types.NamespacedName {
	return types.NamespacedName{
		Namespace: app.Namespace,
		Name:      app.Name,
	}
}

func int64ptr(n int64) *int64 {
	return &n
}

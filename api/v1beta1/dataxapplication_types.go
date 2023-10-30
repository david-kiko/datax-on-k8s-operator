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

package v1beta1

import (
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// DeployMode describes the type of deployment of a Datax application.
type DeployMode string

// Different types of deployments.
const (
	ClusterMode         DeployMode = "cluster"
	ClientMode          DeployMode = "client"
	InClusterClientMode DeployMode = "in-cluster-client"
)

type RestartPolicyType string

const (
	Never     RestartPolicyType = "Never"
	OnFailure RestartPolicyType = "OnFailure"
	Always    RestartPolicyType = "Always"
)

// RestartPolicy is the policy of if and in which conditions the controller should restart a terminated application.
// This completely defines actions to be taken on any kind of Failures during an application run.
type RestartPolicy struct {
	// Type specifies the RestartPolicyType.
	// +kubebuilder:validation:Enum={Never,Always,OnFailure}
	Type RestartPolicyType `json:"type,omitempty"`

	// OnSubmissionFailureRetries is the number of times to retry submitting an application before giving up.
	// This is best effort and actual retry attempts can be >= the value specified due to caching.
	// These are required if RestartPolicy is OnFailure.
	// +kubebuilder:validation:Minimum=0
	// +optional
	OnSubmissionFailureRetries *int32 `json:"onSubmissionFailureRetries,omitempty"`

	// OnFailureRetries the number of times to retry running an application before giving up.
	// +kubebuilder:validation:Minimum=0
	// +optional
	OnFailureRetries *int32 `json:"onFailureRetries,omitempty"`

	// OnSubmissionFailureRetryInterval is the interval in seconds between retries on failed submissions.
	// +kubebuilder:validation:Minimum=1
	// +optional
	OnSubmissionFailureRetryInterval *int64 `json:"onSubmissionFailureRetryInterval,omitempty"`

	// OnFailureRetryInterval is the interval in seconds between retries on failed runs.
	// +kubebuilder:validation:Minimum=1
	// +optional
	OnFailureRetryInterval *int64 `json:"onFailureRetryInterval,omitempty"`
}

type DataxPodSpec struct {
	// CoreRequest is the physical CPU core request for the driver.
	// Maps to `spark.kubernetes.driver.request.cores` that is available since Spark 3.0.
	// +optional
	CoreRequest string `json:"coreRequest,omitempty"`
	// CoreLimit specifies a hard limit on CPU cores for the pod.
	// Optional
	CoreLimit string `json:"coreLimit,omitempty"`
	// MemoryRequest is the physical Memory core request for the driver.
	// +optional
	MemoryRequest string `json:"memoryRequest,omitempty"`
	// MemoryLimit specifies a hard limit on Memory for the pod.
	// +optional
	MemoryLimit string `json:"memoryLimit,omitempty"`
	// Image is the container image to use. Overrides Spec.Image if set.
	// +optional
	Image string `json:"image,omitempty"`
	// ConfigMaps carries information of other ConfigMaps to add to the pod.
	// +optional
	Env []apiv1.EnvVar `json:"env,omitempty"`
	// EnvVars carries the environment variables to add to the pod.
	// Deprecated. Consider using `env` instead.
	// +optional
	EnvVars map[string]string `json:"envVars,omitempty"`
	// EnvFrom is a list of sources to populate environment variables in the container.
	// +optional
	EnvFrom []apiv1.EnvFromSource `json:"envFrom,omitempty"`
	// Labels are the Kubernetes labels to be added to the pod.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
	// Annotations are the Kubernetes annotations to be added to the pod.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
	// VolumeMounts specifies the volumes listed in ".spec.volumes" to mount into the main container's filesystem.
	// +optional
	VolumeMounts []apiv1.VolumeMount `json:"volumeMounts,omitempty"`
	// Affinity specifies the affinity/anti-affinity settings for the pod.
	// +optional
	Affinity *apiv1.Affinity `json:"affinity,omitempty"`
	// Tolerations specifies the tolerations listed in ".spec.tolerations" to be applied to the pod.
	// +optional
	Tolerations []apiv1.Toleration `json:"tolerations,omitempty"`
	// PodSecurityContext specifies the PodSecurityContext to apply.
	// +optional
	PodSecurityContext *apiv1.PodSecurityContext `json:"podSecurityContext,omitempty"`
	// SecurityContext specifies the container's SecurityContext to apply.
	// +optional
	SecurityContext *apiv1.SecurityContext `json:"securityContext,omitempty"`
	// SchedulerName specifies the scheduler that will be used for scheduling
	// +optional
	SchedulerName *string `json:"schedulerName,omitempty"`
	// Sidecars is a list of sidecar containers that run along side the main Spark container.
	// +optional
	Sidecars []apiv1.Container `json:"sidecars,omitempty"`
	// InitContainers is a list of init-containers that run to completion before the main Spark container.
	// +optional
	InitContainers []apiv1.Container `json:"initContainers,omitempty"`
	// HostNetwork indicates whether to request host networking for the pod or not.
	// +optional
	HostNetwork *bool `json:"hostNetwork,omitempty"`
	// NodeSelector is the Kubernetes node selector to be added to the driver and executor pods.
	// This field is mutually exclusive with nodeSelector at SparkApplication level (which will be deprecated).
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// DnsConfig dns settings for the pod, following the Kubernetes specifications.
	// +optional
	DNSConfig *apiv1.PodDNSConfig `json:"dnsConfig,omitempty"`
	// Termination grace period seconds for the pod
	// +optional
	TerminationGracePeriodSeconds *int64 `json:"terminationGracePeriodSeconds,omitempty"`
	// ServiceAccount is the name of the custom Kubernetes service account used by the pod.
	// +optional
	ServiceAccount *string `json:"serviceAccount,omitempty"`
	// HostAliases settings for the pod, following the Kubernetes specifications.
	// +optional
	HostAliases []apiv1.HostAlias `json:"hostAliases,omitempty"`
	// ShareProcessNamespace settings for the pod, following the Kubernetes specifications.
	// +optional
	ShareProcessNamespace *bool `json:"shareProcessNamespace,omitempty"`
}

type Port struct {
	Name          string `json:"name"`
	Protocol      string `json:"protocol"`
	ContainerPort int32  `json:"containerPort"`
}

const (
	DefaultDriverPort = 10000

	DefaultDriverCoreRequest   = "512m"
	DefaultDriverCoreLimit     = "512m"
	DefaultDriverMemoryRequest = "1024m"
	DefaultDriverMemoryLimit   = "1024m"

	DefaultExecutorCoreRequest   = "512m"
	DefaultExecutorCoreLimit     = "512m"
	DefaultExecutorMemoryRequest = "1024m"
	DefaultExecutorMemoryLimit   = "1024m"
)

type DriverSpec struct {
	DataxPodSpec `json:",inline"`
	// PodName is the name of the driver pod that the user creates. This is used for the
	// in-cluster client mode in which the user creates a client pod where the driver of
	// the user application runs. It's an error to set this field if Mode is not
	// in-cluster-client.
	// +optional
	// +kubebuilder:validation:Pattern=[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*
	PodName *string `json:"podName,omitempty"`
	// JavaOptions is a string of extra JVM options to pass to the driver. For instance,
	// GC settings or other logging.
	// +optional
	JavaOptions *string `json:"javaOptions,omitempty"`
	// Lifecycle for running preStop or postStart commands
	// +optional
	Lifecycle *apiv1.Lifecycle `json:"lifecycle,omitempty"`
	// KubernetesMaster is the URL of the Kubernetes master used by the driver to manage executor pods and
	// other Kubernetes resources. Default to https://kubernetes.default.svc.
	// +optional
	KubernetesMaster *string `json:"kubernetesMaster,omitempty"`
	// ServiceAnnotations defines the annotations to be added to the Kubernetes headless service used by
	// executors to connect to the driver.
	// +optional
	ServiceAnnotations map[string]string `json:"serviceAnnotations,omitempty"`
	// Ports settings for the pods, following the Kubernetes specifications.
	// +optional
	// Defaults to 10000
	DriverPort int32 `json:"driverPort,omitempty"`
}

// BatchSchedulerConfiguration used to configure how to batch scheduling Spark Application
type BatchSchedulerConfiguration struct {
	// Queue stands for the resource queue which the application belongs to, it's being used in Volcano batch scheduler.
	// +optional
	Queue *string `json:"queue,omitempty"`
	// PriorityClassName stands for the name of k8s PriorityClass resource, it's being used in Volcano batch scheduler.
	// +optional
	PriorityClassName *string `json:"priorityClassName,omitempty"`
	// Resources stands for the resource list custom request for. Usually it is used to define the lower-bound limit.
	// If specified, volcano scheduler will consider it as the resources requested.
	// +optional
	Resources apiv1.ResourceList `json:"resources,omitempty"`
}

type ExecutorSpec struct {
	DataxPodSpec `json:",inline"`
	// JavaOptions is a string of extra JVM options to pass to the executors. For instance,
	// GC settings or other logging.
	// +optional
	JavaOptions *string `json:"javaOptions,omitempty"`
	// Lifecycle for running preStop or postStart commands
	// +optional
	Lifecycle *apiv1.Lifecycle `json:"lifecycle,omitempty"`
	// DeleteOnTermination specify whether executor pods should be deleted in case of failure or normal termination.
	// +optional
	DeleteOnTermination *bool `json:"deleteOnTermination,omitempty"`
	// Ports settings for the pods, following the Kubernetes specifications.
	// +optional
	Ports []Port `json:"ports,omitempty"`
}

// DataxApplicationSpec defines the desired state of DataxApplication
type DataxApplicationSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// Mode is the deployment mode of the Datax application.
	// +kubebuilder:validation:Enum={cluster,client}
	Mode DeployMode `json:"mode,omitempty"`
	// Image is the container image for the driver, executor, and init-container. Any custom container images for the
	// driver, executor, or init-container takes precedence over this.
	// +optional
	Image string `json:"image,omitempty"`
	// ImagePullPolicy is the image pull policy for the driver, executor, and init-container.
	// +optional
	ImagePullPolicy apiv1.PullPolicy `json:"imagePullPolicy,omitempty"`
	// ImagePullSecrets is the list of image-pull secrets.
	// +optional
	ImagePullSecrets []string `json:"imagePullSecrets,omitempty"`
	// SparkConf carries user-specified Spark configuration properties as they would use the  "--conf" option in
	// spark-submit.
	// +optional
	DataxConf map[string]string `json:"sparkConf,omitempty"`
	// Volumes is the list of Kubernetes volumes that can be mounted by the driver and/or executors.
	// +optional
	Volumes []apiv1.Volume `json:"volumes,omitempty"`
	// Driver is the driver specification.
	Driver DriverSpec `json:"driver"`
	// Executor is the executor specification.
	Executor ExecutorSpec `json:"executor"`
	// Channel is the number of executor instances.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Channel *int32 `json:"channel,omitempty"`
	// RestartPolicy defines the policy on if and in which conditions the controller should restart an application.
	RestartPolicy RestartPolicy `json:"restartPolicy,omitempty"`
	// NodeSelector is the Kubernetes node selector to be added to the driver and executor pods.
	// This field is mutually exclusive with nodeSelector at podSpec level (driver or executor).
	// This field will be deprecated in future versions (at SparkApplicationSpec level).
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// BatchScheduler configures which batch scheduler will be used for scheduling
	// +optional
	BatchScheduler *string `json:"batchScheduler,omitempty"`
	// TimeToLiveSeconds defines the Time-To-Live (TTL) duration in seconds for this SparkApplication
	// after its termination.
	// The SparkApplication object will be garbage collected if the current time is more than the
	// TimeToLiveSeconds since its termination.
	// +optional
	TimeToLiveSeconds *int64 `json:"timeToLiveSeconds,omitempty"`
	// BatchSchedulerOptions provides fine-grained control on how to batch scheduling.
	// +optional
	BatchSchedulerOptions *BatchSchedulerConfiguration `json:"batchSchedulerOptions,omitempty"`
}

// DriverInfo captures information about the driver.
type DriverInfo struct {
	PodName string `json:"podName,omitempty"`
}

// ApplicationStateType represents the type of the current state of an application.
type ApplicationStateType string

// Different states an application may have.
const (
	NewState              ApplicationStateType = ""
	SubmittedState        ApplicationStateType = "SUBMITTED"
	RunningState          ApplicationStateType = "RUNNING"
	CompletedState        ApplicationStateType = "COMPLETED"
	FailedState           ApplicationStateType = "FAILED"
	FailedSubmissionState ApplicationStateType = "SUBMISSION_FAILED"
	PendingRerunState     ApplicationStateType = "PENDING_RERUN"
	InvalidatingState     ApplicationStateType = "INVALIDATING"
	SucceedingState       ApplicationStateType = "SUCCEEDING"
	FailingState          ApplicationStateType = "FAILING"
	UnknownState          ApplicationStateType = "UNKNOWN"
)

// ApplicationState tells the current state of the application and an error message in case of failures.
type ApplicationState struct {
	State        ApplicationStateType `json:"state"`
	ErrorMessage string               `json:"errorMessage,omitempty"`
}

// DriverState tells the current state of a spark driver.
type DriverState string

// Different states a spark driver may have.
const (
	DriverPendingState   DriverState = "PENDING"
	DriverRunningState   DriverState = "RUNNING"
	DriverCompletedState DriverState = "COMPLETED"
	DriverFailedState    DriverState = "FAILED"
	DriverUnknownState   DriverState = "UNKNOWN"
)

// ExecutorState tells the current state of an executor.
type ExecutorState string

// Different states an executor may have.
const (
	ExecutorPendingState   ExecutorState = "PENDING"
	ExecutorRunningState   ExecutorState = "RUNNING"
	ExecutorCompletedState ExecutorState = "COMPLETED"
	ExecutorFailedState    ExecutorState = "FAILED"
	ExecutorUnknownState   ExecutorState = "UNKNOWN"
)

// DataxApplicationStatus defines the observed state of DataxApplication
type DataxApplicationStatus struct {
	// DataxApplicationID is set by the on the driver and executor pods
	DataxApplicationID string `json:"dataxApplicationId,omitempty"`
	// SubmissionID is a unique ID of the current submission of the application.
	SubmissionID string `json:"submissionID,omitempty"`
	// LastSubmissionAttemptTime is the time for the last application submission attempt.
	// +nullable
	LastSubmissionAttemptTime metav1.Time `json:"lastSubmissionAttemptTime,omitempty"`
	// CompletionTime is the time when the application runs to completion if it does.
	// +nullable
	TerminationTime metav1.Time `json:"terminationTime,omitempty"`
	// DriverInfo has information about the driver.
	DriverInfo DriverInfo `json:"driverInfo"`
	// AppState tells the overall application state.
	AppState ApplicationState `json:"applicationState,omitempty"`
	// ExecutorState records the state of executors by executor Pod names.
	ExecutorState map[string]ExecutorState `json:"executorState,omitempty"`
	// ExecutionAttempts is the total number of attempts to run a submitted application to completion.
	// Incremented upon each attempted run of the application and reset upon invalidation.
	ExecutionAttempts int32 `json:"executionAttempts,omitempty"`
	// SubmissionAttempts is the total number of attempts to submit an application to run.
	// Incremented upon each attempted submission of the application and reset upon invalidation and rerun.
	SubmissionAttempts int32 `json:"submissionAttempts,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// DataxApplication is the Schema for the dataxapplications API
type DataxApplication struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DataxApplicationSpec   `json:"spec,omitempty"`
	Status DataxApplicationStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// DataxApplicationList contains a list of DataxApplication
type DataxApplicationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DataxApplication `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DataxApplication{}, &DataxApplicationList{})
}

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

package config

const (
	// DefaultDataxConfDir is the default directory for Datax configuration files if not specified.
	// This directory is where the Datax ConfigMap is mounted in the driver and executor containers.
	DefaultDataxConfDir = "/etc/datax/conf"
	// DataxConfigMapVolumeName is the name of the ConfigMap volume of Datax configuration files.
	DataxConfigMapVolumeName = "datax-configmap-volume"
	// DefaultHadoopConfDir is the default directory for Datax configuration files if not specified.
	// This directory is where the Hadoop ConfigMap is mounted in the driver and executor containers.
	DefaultHadoopConfDir = "/etc/hadoop/conf"
	// HadoopConfigMapVolumeName is the name of the ConfigMap volume of Hadoop configuration files.
	HadoopConfigMapVolumeName = "hadoop-configmap-volume"
	// DataxConfDirEnvVar is the environment variable to add to the driver and executor Pods that point
	// to the directory where the Datax ConfigMap is mounted.
	DataxConfDirEnvVar = "SPARK_CONF_DIR"
	// HadoopConfDirEnvVar is the environment variable to add to the driver and executor Pods that point
	// to the directory where the Hadoop ConfigMap is mounted.
	HadoopConfDirEnvVar = "HADOOP_CONF_DIR"
)

const (
	// LabelAnnotationPrefix is the prefix of every labels and annotations added by the controller.
	LabelAnnotationPrefix = "dataxoperator.k8s.io/"
	// DataxAppNameLabel is the name of the label for the DataxApplication object name.
	DataxAppNameLabel = LabelAnnotationPrefix + "app-name"
	// ScheduledDataxAppNameLabel is the name of the label for the ScheduledDataxApplication object name.
	ScheduledDataxAppNameLabel = LabelAnnotationPrefix + "scheduled-app-name"
	// LaunchedByDataxOperatorLabel is a label on Datax pods launched through the Datax Operator.
	LaunchedByDataxOperatorLabel = LabelAnnotationPrefix + "launched-by-datax-operator"
	// DataxApplicationSelectorLabel is the AppID set by the datax-distribution on the driver/executors Pods.
	DataxApplicationSelectorLabel = "datax-app-selector"
	// DataxRoleLabel is the driver/executor label set by the operator/datax-distribution on the driver/executors Pods.
	DataxRoleLabel = "datax-role"
	// DataxDriverRole is the value of the datax-role label for the driver.
	DataxDriverRole = "driver"
	// DataxExecutorRole is the value of the datax-role label for the executors.
	DataxExecutorRole = "executor"
	// SubmissionIDLabel is the label that records the submission ID of the current run of an application.
	SubmissionIDLabel = LabelAnnotationPrefix + "submission-id"
)

const (
	DataxExecutorContainerImageKeyEnv = "DATAX_KUBERNETES_EXECUTOR_CONTAINER_IMAGE"
	DataxExecutorCoreRequestKeyEnv    = "DATAX_KUBERNETES_EXECUTOR_REQUEST_CORES"
	DataxExecutorMemoryRequestKeyEnv  = "DATAX_KUBERNETES_EXECUTOR_REQUEST_MEMORY"
	DataxExecutorCoreLimitKeyEnv      = "DATAX_KUBERNETES_EXECUTOR_LIMIT_CORES"
	DataxExecutorMemoryLimitKeyEnv    = "DATAX_KUBERNETES_EXECUTOR_LIMIT_MEMORY"
)

const (
	GroupName = "dataxoperator.tech.io"
)

const (
	// DataxAppNameKey is the configuration property for application name.
	DataxAppNameKey = "datax.app.name"
	// DataxAppNamespaceKey is the configuration property for application namespace.
	DataxAppNamespaceKey = "datax.kubernetes.namespace"
	// DataxContainerImageKey is the configuration property for specifying the unified container image.
	DataxContainerImageKey = "datax.kubernetes.container.image"
	// DataxImagePullSecretKey is the configuration property for specifying the comma-separated list of image-pull
	// secrets.
	DataxImagePullSecretKey = "datax.kubernetes.container.image.pullSecrets"
	// DataxContainerImagePullPolicyKey is the configuration property for specifying the container image pull policy.
	DataxContainerImagePullPolicyKey = "datax.kubernetes.container.image.pullPolicy"
	// DataxNodeSelectorKeyPrefix is the configuration property prefix for specifying node selector for the pods.
	DataxNodeSelectorKeyPrefix = "datax.kubernetes.node.selector."
	// DataxDriverContainerImageKey is the configuration property for specifying a custom driver container image.
	DataxDriverContainerImageKey = "datax.kubernetes.driver.container.image"
	// DataxExecutorContainerImageKey is the configuration property for specifying a custom executor container image.
	DataxExecutorContainerImageKey = "datax.kubernetes.executor.container.image"
	// DataxDriverCoreRequestKey is the configuration property for specifying the physical CPU request for the driver.
	DataxDriverCoreRequestKey = "datax.kubernetes.driver.request.cores"
	// DataxExecutorCoreRequestKey is the configuration property for specifying the physical CPU request for executors.
	DataxExecutorCoreRequestKey = "datax.kubernetes.executor.request.cores"
	// DataxDriverCoreLimitKey is the configuration property for specifying the hard CPU limit for the driver pod.
	DataxDriverCoreLimitKey = "datax.kubernetes.driver.limit.cores"
	// DataxExecutorCoreLimitKey is the configuration property for specifying the hard CPU limit for the executor pods.
	DataxExecutorCoreLimitKey = "datax.kubernetes.executor.limit.cores"
	// DataxDriverSecretKeyPrefix is the configuration property prefix for specifying secrets to be mounted into the
	// driver.
	DataxDriverSecretKeyPrefix = "datax.kubernetes.driver.secrets."
	// DataxExecutorSecretKeyPrefix is the configuration property prefix for specifying secrets to be mounted into the
	// executors.
	DataxExecutorSecretKeyPrefix = "datax.kubernetes.executor.secrets."
	// DataxDriverSecretKeyRefKeyPrefix is the configuration property prefix for specifying environment variables
	// from SecretKeyRefs for the driver.
	DataxDriverSecretKeyRefKeyPrefix = "datax.kubernetes.driver.secretKeyRef."
	// DataxExecutorSecretKeyRefKeyPrefix is the configuration property prefix for specifying environment variables
	// from SecretKeyRefs for the executors.
	DataxExecutorSecretKeyRefKeyPrefix = "datax.kubernetes.executor.secretKeyRef."
	// DataxDriverEnvVarConfigKeyPrefix is the Datax configuration prefix for setting environment variables
	// into the driver.
	DataxDriverEnvVarConfigKeyPrefix = "datax.kubernetes.driverEnv."
	// DataxExecutorEnvVarConfigKeyPrefix is the Datax configuration prefix for setting environment variables
	// into the executor.
	DataxExecutorEnvVarConfigKeyPrefix = "datax.executorEnv."
	// DataxDriverAnnotationKeyPrefix is the Datax configuration key prefix for annotations on the driver Pod.
	DataxDriverAnnotationKeyPrefix = "datax.kubernetes.driver.annotation."
	// DataxExecutorAnnotationKeyPrefix is the Datax configuration key prefix for annotations on the executor Pods.
	DataxExecutorAnnotationKeyPrefix = "datax.kubernetes.executor.annotation."
	// DataxDriverLabelKeyPrefix is the Datax configuration key prefix for labels on the driver Pod.
	DataxDriverLabelKeyPrefix = "datax.kubernetes.driver.label."
	// DataxExecutorLabelKeyPrefix is the Datax configuration key prefix for labels on the executor Pods.
	DataxExecutorLabelKeyPrefix = "datax.kubernetes.executor.label."
	// DataxDriverVolumesPrefix is the Datax volumes configuration for mounting a volume into the driver pod.
	DataxDriverVolumesPrefix = "datax.kubernetes.driver.volumes."
	// DataxExecutorVolumesPrefix is the Datax volumes configuration for mounting a volume into the driver pod.
	DataxExecutorVolumesPrefix = "datax.kubernetes.executor.volumes."
	// DataxDriverPodNameKey is the Datax configuration key for driver pod name.
	DataxDriverPodNameKey = "datax.kubernetes.driver.pod.name"
	// DataxDriverServiceAccountName is the Datax configuration key for specifying name of the Kubernetes service
	// account used by the driver pod.
	DataxDriverServiceAccountName = "datax.kubernetes.authenticate.driver.serviceAccountName"
	// account used by the executor pod.
	DataxExecutorAccountName = "datax.kubernetes.authenticate.executor.serviceAccountName"
	// DataxInitContainerImage is the Datax configuration key for specifying a custom init-container image.
	DataxInitContainerImage = "datax.kubernetes.initContainer.image"
	// DataxJarsDownloadDir is the Datax configuration key for specifying the download path in the driver and
	// executors for remote jars.
	DataxJarsDownloadDir = "datax.kubernetes.mountDependencies.jarsDownloadDir"
	// DataxFilesDownloadDir is the Datax configuration key for specifying the download path in the driver and
	// executors for remote files.
	DataxFilesDownloadDir = "datax.kubernetes.mountDependencies.filesDownloadDir"
	// DataxDownloadTimeout is the Datax configuration key for specifying the timeout in seconds of downloading
	// remote dependencies.
	DataxDownloadTimeout = "datax.kubernetes.mountDependencies.timeout"
	// DataxMaxSimultaneousDownloads is the Datax configuration key for specifying the maximum number of remote
	// dependencies to download.
	DataxMaxSimultaneousDownloads = "datax.kubernetes.mountDependencies.maxSimultaneousDownloads"
	// DataxWaitAppCompletion is the Datax configuration key for specifying whether to wait for application to complete.
	DataxWaitAppCompletion = "datax.kubernetes.submission.waitAppCompletion"
	// DataxPythonVersion is the Datax configuration key for specifying python version used.
	DataxPythonVersion = "datax.kubernetes.pydatax.pythonVersion"
	// DataxMemoryOverheadFactor is the Datax configuration key for specifying memory overhead factor used for Non-JVM memory.
	DataxMemoryOverheadFactor = "datax.kubernetes.memoryOverheadFactor"
	// DataxDriverJavaOptions is the Datax configuration key for a string of extra JVM options to pass to driver.
	DataxDriverJavaOptions = "datax.driver.extraJavaOptions"
	// DataxExecutorJavaOptions is the Datax configuration key for a string of extra JVM options to pass to executors.
	DataxExecutorJavaOptions = "datax.executor.extraJavaOptions"
	// DataxExecutorDeleteOnTermination is the Datax configuration for specifying whether executor pods should be deleted in case of failure or normal termination
	DataxExecutorDeleteOnTermination = "datax.kubernetes.executor.deleteOnTermination"
	// DataxDriverKubernetesMaster is the Datax configuration key for specifying the Kubernetes master the driver use
	// to manage executor pods and other Kubernetes resources.
	DataxDriverKubernetesMaster = "datax.kubernetes.driver.master"
	// DataxDriverServiceAnnotationKeyPrefix is the key prefix of annotations to be added to the driver service.
	DataxDriverServiceAnnotationKeyPrefix = "datax.kubernetes.driver.service.annotation."
	// DataxDynamicAllocationEnabled is the Datax configuration key for specifying if dynamic
	// allocation is enabled or not.
	DataxDynamicAllocationEnabled = "datax.dynamicAllocation.enabled"
	// DataxDynamicAllocationShuffleTrackingEnabled is the Datax configuration key for
	// specifying if shuffle data tracking is enabled.
	DataxDynamicAllocationShuffleTrackingEnabled = "datax.dynamicAllocation.shuffleTracking.enabled"
	// DataxDynamicAllocationShuffleTrackingTimeout is the Datax configuration key for specifying
	// the shuffle tracking timeout in milliseconds if shuffle tracking is enabled.
	DataxDynamicAllocationShuffleTrackingTimeout = "datax.dynamicAllocation.shuffleTracking.timeout"
	// DataxDynamicAllocationInitialExecutors is the Datax configuration key for specifying
	// the initial number of executors to request if dynamic allocation is enabled.
	DataxDynamicAllocationInitialExecutors = "datax.dynamicAllocation.initialExecutors"
	// DataxDynamicAllocationMinExecutors is the Datax configuration key for specifying the
	// lower bound of the number of executors to request if dynamic allocation is enabled.
	DataxDynamicAllocationMinExecutors = "datax.dynamicAllocation.minExecutors"
	// DataxDynamicAllocationMaxExecutors is the Datax configuration key for specifying the
	// upper bound of the number of executors to request if dynamic allocation is enabled.
	DataxDynamicAllocationMaxExecutors = "datax.dynamicAllocation.maxExecutors"
)

const (
	// GoogleApplicationCredentialsEnvVar is the environment variable used by the
	// Application Default Credentials mechanism. More details can be found at
	// https://developers.google.com/identity/protocols/application-default-credentials.
	GoogleApplicationCredentialsEnvVar = "GOOGLE_APPLICATION_CREDENTIALS"
	// ServiceAccountJSONKeyFileName is the assumed name of the service account
	// Json key file. This name is added to the service account secret mount path to
	// form the path to the Json key file referred to by GOOGLE_APPLICATION_CREDENTIALS.
	ServiceAccountJSONKeyFileName = "key.json"
	// HadoopTokenFileLocationEnvVar is the environment variable for specifying the location
	// where the file storing the Hadoop delegation token is located.
	HadoopTokenFileLocationEnvVar = "HADOOP_TOKEN_FILE_LOCATION"
	// HadoopDelegationTokenFileName is the assumed name of the file storing the Hadoop
	// delegation token. This name is added to the delegation token secret mount path to
	// form the path to the file referred to by HADOOP_TOKEN_FILE_LOCATION.
	HadoopDelegationTokenFileName = "hadoop.token"
)

const (
	// DataxDriverContainerName is name of driver container in datax driver pod
	DataxDriverContainerName = "datax-kubernetes-driver"
	// DataxExecutorContainerName is name of executor container in datax executor pod
	DataxExecutorContainerName = "datax-kubernetes-executor"
	// DataxLocalDirVolumePrefix is the volume name prefix for "scratch" space directory
	DataxLocalDirVolumePrefix = "datax-local-dir-"
)

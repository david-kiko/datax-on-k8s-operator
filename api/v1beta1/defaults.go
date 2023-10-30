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

package v1beta1

// SetDataxApplicationDefaults sets default values for certain fields of a DataxApplication.
func SetDataxApplicationDefaults(app *DataxApplication) {
	if app == nil {
		return
	}

	if app.Spec.Mode == "" {
		app.Spec.Mode = ClusterMode
	}

	if app.Spec.RestartPolicy.Type == "" {
		app.Spec.RestartPolicy.Type = Never
	}

	if app.Spec.Driver.DriverPort == 0 {
		app.Spec.Driver.DriverPort = DefaultDriverPort
	}

	if app.Spec.RestartPolicy.Type != Never {
		// Default to 5 sec if the RestartPolicy is OnFailure or Always and these values aren't specified.
		if app.Spec.RestartPolicy.OnFailureRetryInterval == nil {
			app.Spec.RestartPolicy.OnFailureRetryInterval = new(int64)
			*app.Spec.RestartPolicy.OnFailureRetryInterval = 5
		}

		if app.Spec.RestartPolicy.OnSubmissionFailureRetryInterval == nil {
			app.Spec.RestartPolicy.OnSubmissionFailureRetryInterval = new(int64)
			*app.Spec.RestartPolicy.OnSubmissionFailureRetryInterval = 5
		}
	}

	if app.Spec.Driver.Image == "" {
		app.Spec.Driver.Image = app.Spec.Image
	}

	if app.Spec.Executor.Image == "" {
		app.Spec.Executor.Image = app.Spec.Image
	}

	if app.Spec.Driver.CoreRequest == "" {
		app.Spec.Driver.CoreRequest = DefaultDriverCoreRequest
	}

	if app.Spec.Driver.CoreLimit == "" {
		app.Spec.Driver.CoreLimit = DefaultDriverCoreLimit
	}

	if app.Spec.Driver.MemoryRequest == "" {
		app.Spec.Driver.MemoryRequest = DefaultDriverMemoryRequest
	}

	if app.Spec.Driver.MemoryLimit == "" {
		app.Spec.Driver.MemoryLimit = DefaultDriverMemoryLimit
	}

	if app.Spec.Executor.CoreRequest == "" {
		app.Spec.Executor.CoreRequest = DefaultExecutorCoreRequest
	}

	if app.Spec.Executor.CoreLimit == "" {
		app.Spec.Executor.CoreLimit = DefaultExecutorCoreLimit
	}

	if app.Spec.Executor.MemoryRequest == "" {
		app.Spec.Executor.MemoryRequest = DefaultExecutorMemoryRequest
	}

	if app.Spec.Executor.MemoryLimit == "" {
		app.Spec.Executor.MemoryLimit = DefaultExecutorMemoryLimit
	}

	//
	//setDriverSpecDefaults(&app.Spec.Driver, app.Spec.DataxConf)
	//setExecutorSpecDefaults(&app.Spec.Executor, app.Spec.DataxConf)
}

//func setDriverSpecDefaults(spec *DriverSpec, dataxConf map[string]string) {
//
//	if _, exists := dataxConf["datax.driver.cores"]; !exists && spec.Cores == nil {
//		spec.Cores = new(int32)
//		*spec.Cores = 1
//	}
//	if _, exists := dataxConf["datax.driver.memory"]; !exists && spec.Memory == nil {
//		spec.Memory = new(string)
//		*spec.Memory = "1g"
//	}
//}
//
//func setExecutorSpecDefaults(spec *ExecutorSpec, dataxConf map[string]string) {
//	if _, exists := dataxConf["datax.executor.cores"]; !exists && spec.Cores == nil {
//		spec.Cores = new(int32)
//		*spec.Cores = 1
//	}
//	if _, exists := dataxConf["datax.executor.memory"]; !exists && spec.Memory == nil {
//		spec.Memory = new(string)
//		*spec.Memory = "1g"
//	}
//	if _, exists := dataxConf["datax.executor.instances"]; !exists && spec.Instances == nil {
//		spec.Instances = new(int32)
//		*spec.Instances = 1
//	}
//}

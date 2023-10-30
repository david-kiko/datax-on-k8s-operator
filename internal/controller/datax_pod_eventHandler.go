package controller

import (
	"context"
	"github.com/david-kiko/datax-on-k8s-operator/config"
	"github.com/golang/glog"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

type dataxPodEventHandler struct {
}

func (d *dataxPodEventHandler) onPodAdd(ctx context.Context, evt event.CreateEvent, q workqueue.RateLimitingInterface) {
	pod := evt.Object.(*apiv1.Pod)
	glog.V(2).Infof("Pod %s added in namespace %s.", pod.GetName(), pod.GetNamespace())
	d.enqueueSparkAppForUpdate(pod, q)
}

func (d *dataxPodEventHandler) onPodUpdate(ctx context.Context, evt event.UpdateEvent, q workqueue.RateLimitingInterface) {
	oldPod := evt.ObjectOld.(*apiv1.Pod)
	newPod := evt.ObjectNew.(*apiv1.Pod)

	if newPod.ResourceVersion == oldPod.ResourceVersion {
		return
	}
	glog.V(2).Infof("Pod %s updated in namespace %s.", newPod.GetName(), newPod.GetNamespace())
	d.enqueueSparkAppForUpdate(newPod, q)
}

func (d *dataxPodEventHandler) onDelete(ctx context.Context, evt event.DeleteEvent, q workqueue.RateLimitingInterface) {
	deletedPod := evt.Object.(*apiv1.Pod)

	if deletedPod == nil {
		return
	}
	glog.V(2).Infof("Pod %s deleted in namespace %s.", deletedPod.GetName(), deletedPod.GetNamespace())
	d.enqueueSparkAppForUpdate(deletedPod, q)
}

func (d *dataxPodEventHandler) enqueueSparkAppForUpdate(pod *apiv1.Pod, q workqueue.RateLimitingInterface) {
	appName, exists := getAppName(pod)
	if !exists {
		return
	}

	if _, exists := pod.Labels[config.SubmissionIDLabel]; exists {
		//app, err := d.applicationLister.SparkApplications(pod.GetNamespace()).Get(appName)
		//if err != nil || app.Status.SubmissionID != submissionID {
		//	return
		//}
		appKey := createRequestKey(pod.GetNamespace(), appName)
		glog.V(2).Infof("Enqueuing SparkApplication %s for app update processing.", appKey)
		q.AddRateLimited(appKey)
	}
}

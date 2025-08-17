// Copyright (c) KAITO authors.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controllers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kaitov1beta1 "github.com/kaito-project/kaito/api/v1beta1"
	"github.com/kaito-project/kaito/pkg/utils/consts"
	"github.com/kaito-project/kaito/pkg/utils/plugin"
	"github.com/kaito-project/kaito/pkg/utils/resources"
	ctrlutils "github.com/kaito-project/kaito/pkg/workspace/controllers/utils"
	"github.com/kaito-project/kaito/pkg/workspace/inference"
	"github.com/kaito-project/kaito/pkg/workspace/manifests"
	"github.com/kaito-project/kaito/pkg/workspace/tuning"
)

const (
	WorkspaceHashAnnotation = "workspace.kaito.io/hash"
	WorkspaceNameLabel      = "workspace.kaito.io/name"
	revisionHashSuffix      = 5
)

type WorkspaceReconciler struct {
	client.Client
	Log      logr.Logger
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

func NewWorkspaceReconciler(client client.Client, scheme *runtime.Scheme, log logr.Logger, Recorder record.EventRecorder) *WorkspaceReconciler {
	return &WorkspaceReconciler{
		Client:   client,
		Scheme:   scheme,
		Log:      log,
		Recorder: Recorder,
	}
}

func (c *WorkspaceReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	workspaceObj := &kaitov1beta1.Workspace{}
	if err := c.Client.Get(ctx, req.NamespacedName, workspaceObj); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		klog.ErrorS(err, "failed to get workspace", "workspace", req.Name)
		return reconcile.Result{}, err
	}

	// if the condition ConditionTypeResourceStatus is not true, we should return directly.
	resourceStatusCondition := meta.FindStatusCondition(workspaceObj.Status.Conditions, string(kaitov1beta1.ConditionTypeResourceStatus))
	if resourceStatusCondition == nil || resourceStatusCondition.Status != metav1.ConditionTrue {
		klog.InfoS("Resource of Workspace is not ready", "workspace", req.NamespacedName)
		return reconcile.Result{}, nil
	}

	klog.InfoS("Reconciling", "workspace", req.NamespacedName)

	if err := c.syncControllerRevision(ctx, workspaceObj); err != nil {
		return reconcile.Result{}, err
	}

	result, err := c.addOrUpdateWorkspace(ctx, workspaceObj)
	if err != nil {
		return result, err
	}

	return result, nil
}

func (c *WorkspaceReconciler) addOrUpdateWorkspace(ctx context.Context, wObj *kaitov1beta1.Workspace) (reconcile.Result, error) {
	var err error
	if wObj.Tuning != nil {
		if err = c.applyTuning(ctx, wObj); err != nil {
			if updateErr := c.updateStatusConditionIfNotMatch(ctx, wObj, kaitov1beta1.WorkspaceConditionTypeSucceeded, metav1.ConditionFalse,
				"workspaceFailed", err.Error()); updateErr != nil {
				klog.ErrorS(updateErr, "failed to update workspace status", "workspace", klog.KObj(wObj))
				return reconcile.Result{}, updateErr
			}
			return reconcile.Result{}, err
		}
		// Only mark workspace succeeded when job completes.
		job := &batchv1.Job{}
		if err = resources.GetResource(ctx, wObj.Name, wObj.Namespace, c.Client, job); err == nil {
			if job.Status.Succeeded > 0 {
				if updateErr := c.updateStatusConditionIfNotMatch(ctx, wObj, kaitov1beta1.WorkspaceConditionTypeSucceeded, metav1.ConditionTrue,
					"workspaceSucceeded", "workspace succeeds"); updateErr != nil {
					klog.ErrorS(updateErr, "failed to update workspace status", "workspace", klog.KObj(wObj))
					return reconcile.Result{}, updateErr
				}
			} else { // The job is still running
				var readyPod int32
				if job.Status.Ready != nil {
					readyPod = *job.Status.Ready
				}
				if updateErr := c.updateStatusConditionIfNotMatch(ctx, wObj, kaitov1beta1.WorkspaceConditionTypeSucceeded, metav1.ConditionFalse,
					"workspacePending", fmt.Sprintf("workspace has not completed, tuning job has %d active pod, %d ready pod", job.Status.Active, readyPod)); updateErr != nil {
					klog.ErrorS(updateErr, "failed to update workspace status", "workspace", klog.KObj(wObj))
					return reconcile.Result{}, updateErr
				}
			}
		} else {
			klog.ErrorS(err, "failed to get job resource", "workspace", klog.KObj(wObj))
			return reconcile.Result{}, err
		}
	} else if wObj.Inference != nil {
		if err := c.ensureService(ctx, wObj); err != nil {
			if updateErr := c.updateStatusConditionIfNotMatch(ctx, wObj, kaitov1beta1.WorkspaceConditionTypeSucceeded, metav1.ConditionFalse,
				"workspaceFailed", err.Error()); updateErr != nil {
				klog.ErrorS(updateErr, "failed to update workspace status", "workspace", klog.KObj(wObj))
				return reconcile.Result{}, updateErr
			}
			return reconcile.Result{}, err
		}
		if err = c.applyInference(ctx, wObj); err != nil {
			if updateErr := c.updateStatusConditionIfNotMatch(ctx, wObj, kaitov1beta1.WorkspaceConditionTypeSucceeded, metav1.ConditionFalse,
				"workspaceFailed", err.Error()); updateErr != nil {
				klog.ErrorS(updateErr, "failed to update workspace status", "workspace", klog.KObj(wObj))
				return reconcile.Result{}, updateErr
			}
			return reconcile.Result{}, err
		}

		if err = c.updateStatusConditionIfNotMatch(ctx, wObj, kaitov1beta1.WorkspaceConditionTypeSucceeded, metav1.ConditionTrue,
			"workspaceSucceeded", "workspace succeeds"); err != nil {
			klog.ErrorS(err, "failed to update workspace status", "workspace", klog.KObj(wObj))
			return reconcile.Result{}, err
		}
	}

	return reconcile.Result{}, nil
}

func (c *WorkspaceReconciler) syncControllerRevision(ctx context.Context, wObj *kaitov1beta1.Workspace) error {
	currentHash := computeHash(wObj)
	annotations := wObj.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	} // nil checking.

	revisionNum := int64(1)

	revisions := &appsv1.ControllerRevisionList{}
	if err := c.List(ctx, revisions, client.InNamespace(wObj.Namespace), client.MatchingLabels{WorkspaceNameLabel: wObj.Name}); err != nil {
		return fmt.Errorf("failed to list revisions: %w", err)
	}
	sort.Slice(revisions.Items, func(i, j int) bool {
		return revisions.Items[i].Revision < revisions.Items[j].Revision
	})

	var latestRevision *appsv1.ControllerRevision

	jsonData, err := marshalSelectedFields(wObj)
	if err != nil {
		return fmt.Errorf("failed to marshal revision data: %w", err)
	}

	if len(revisions.Items) > 0 {
		latestRevision = &revisions.Items[len(revisions.Items)-1]

		revisionNum = latestRevision.Revision + 1
	}
	newRevision := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s", wObj.Name, currentHash[:revisionHashSuffix]),
			Namespace: wObj.Namespace,
			Annotations: map[string]string{
				WorkspaceHashAnnotation: currentHash,
			},
			Labels: map[string]string{
				WorkspaceNameLabel: wObj.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(wObj, kaitov1beta1.GroupVersion.WithKind("Workspace")),
			},
		},
		Revision: revisionNum,
		Data:     runtime.RawExtension{Raw: jsonData},
	}

	annotations[WorkspaceHashAnnotation] = currentHash
	wObj.SetAnnotations(annotations)
	controllerRevision := &appsv1.ControllerRevision{}
	if err := c.Get(ctx, types.NamespacedName{
		Name:      newRevision.Name,
		Namespace: newRevision.Namespace,
	}, controllerRevision); err != nil {
		if apierrors.IsNotFound(err) {

			if err := c.Create(ctx, newRevision); err != nil {
				return fmt.Errorf("failed to create new ControllerRevision: %w", err)
			} else {
				annotations[kaitov1beta1.WorkspaceRevisionAnnotation] = strconv.FormatInt(revisionNum, 10)
			}

			if len(revisions.Items) > consts.MaxRevisionHistoryLimit {
				if err := c.Delete(ctx, &revisions.Items[0]); err != nil {
					return fmt.Errorf("failed to delete old revision: %w", err)
				}
			}
		} else {
			return fmt.Errorf("failed to get controller revision: %w", err)
		}
	} else {
		if controllerRevision.Annotations[WorkspaceHashAnnotation] != newRevision.Annotations[WorkspaceHashAnnotation] {
			return fmt.Errorf("revision name conflicts, the hash values are different")
		}
		annotations[kaitov1beta1.WorkspaceRevisionAnnotation] = strconv.FormatInt(controllerRevision.Revision, 10)
	}
	annotations[WorkspaceHashAnnotation] = currentHash

	err = updateWorkspaceWithRetry(ctx, c.Client, wObj, func(ws *kaitov1beta1.Workspace) error {
		ws.SetAnnotations(annotations)
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to update Workspace annotations: %w", err)
	}
	return nil
}

func marshalSelectedFields(wObj *kaitov1beta1.Workspace) ([]byte, error) {
	partialMap := map[string]interface{}{
		"resource":  wObj.Resource,
		"inference": wObj.Inference,
		"tuning":    wObj.Tuning,
	}

	jsonData, err := json.Marshal(partialMap)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal selected fields: %w", err)
	}

	return jsonData, nil
}

func computeHash(w *kaitov1beta1.Workspace) string {
	hasher := sha256.New()
	encoder := json.NewEncoder(hasher)
	encoder.Encode(w.Resource)
	encoder.Encode(w.Inference)
	encoder.Encode(w.Tuning)
	return hex.EncodeToString(hasher.Sum(nil))
}

// getPresetName returns the preset name from wObj if available
func getPresetName(wObj *kaitov1beta1.Workspace) string {
	if wObj.Inference != nil && wObj.Inference.Preset != nil {
		return string(wObj.Inference.Preset.Name)
	}
	if wObj.Tuning != nil && wObj.Tuning.Preset != nil {
		return string(wObj.Tuning.Preset.Name)
	}
	return ""
}

func (c *WorkspaceReconciler) ensureService(ctx context.Context, wObj *kaitov1beta1.Workspace) error {
	serviceType := corev1.ServiceTypeClusterIP
	wAnnotation := wObj.GetAnnotations()

	if len(wAnnotation) != 0 {
		val, found := wAnnotation[kaitov1beta1.AnnotationEnableLB]
		if found && val == "True" {
			serviceType = corev1.ServiceTypeLoadBalancer
		}
	}

	existingSVC := &corev1.Service{}
	err := resources.GetResource(ctx, wObj.Name, wObj.Namespace, c.Client, existingSVC)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
	} else {
		return nil
	}

	isStatefulSet := false
	if presetName := getPresetName(wObj); presetName != "" {
		model := plugin.KaitoModelRegister.MustGet(presetName)
		// Dry-run the inference workload generation to determine if it will be a StatefulSet or not.
		workloadObj, _ := inference.GeneratePresetInference(ctx, wObj, "", model, c.Client)
		_, isStatefulSet = workloadObj.(*appsv1.StatefulSet)
	}

	serviceObj := manifests.GenerateServiceManifest(wObj, serviceType, isStatefulSet)
	if err := resources.CreateResource(ctx, serviceObj, c.Client); err != nil {
		return err
	}

	if isStatefulSet {
		headlessService := manifests.GenerateHeadlessServiceManifest(wObj)
		if err := resources.CreateResource(ctx, headlessService, c.Client); err != nil {
			return err
		}
	}

	return nil
}

func (c *WorkspaceReconciler) applyTuning(ctx context.Context, wObj *kaitov1beta1.Workspace) error {
	var err error
	func() {
		if wObj.Tuning.Preset != nil {
			presetName := string(wObj.Tuning.Preset.Name)
			model := plugin.KaitoModelRegister.MustGet(presetName)

			tuningParam := model.GetTuningParameters()
			existingObj := &batchv1.Job{}
			revisionNum := wObj.Annotations[kaitov1beta1.WorkspaceRevisionAnnotation]
			if err = resources.GetResource(ctx, wObj.Name, wObj.Namespace, c.Client, existingObj); err == nil {
				klog.InfoS("A tuning workload already exists for workspace", "workspace", klog.KObj(wObj))

				if existingObj.Annotations[kaitov1beta1.WorkspaceRevisionAnnotation] != revisionNum {
					deletePolicy := metav1.DeletePropagationForeground
					if err := c.Delete(ctx, existingObj, &client.DeleteOptions{
						PropagationPolicy: &deletePolicy,
					}); err != nil {
						return
					}

					var workloadObj client.Object
					workloadObj, err = tuning.CreatePresetTuning(ctx, wObj, revisionNum, model, c.Client)
					if err != nil {
						return
					}
					existingObj = workloadObj.(*batchv1.Job)
				}

				if err = resources.CheckResourceStatus(existingObj, c.Client, tuningParam.ReadinessTimeout); err != nil {
					return
				}
			} else if apierrors.IsNotFound(err) {
				var workloadObj client.Object
				// Need to create a new workload
				workloadObj, err = tuning.CreatePresetTuning(ctx, wObj, revisionNum, model, c.Client)
				if err != nil {
					return
				}
				if err = resources.CheckResourceStatus(workloadObj, c.Client, tuningParam.ReadinessTimeout); err != nil {
					return
				}
			}
		}
	}()

	if err != nil {
		if updateErr := c.updateStatusConditionIfNotMatch(ctx, wObj, kaitov1beta1.WorkspaceConditionTypeTuningJobStatus, metav1.ConditionFalse,
			"WorkspaceTuningJobStatusFailed", err.Error()); updateErr != nil {
			klog.ErrorS(updateErr, "failed to update workspace status", "workspace", klog.KObj(wObj))
			return updateErr
		}
		return err
	}

	if err := c.updateStatusConditionIfNotMatch(ctx, wObj, kaitov1beta1.WorkspaceConditionTypeTuningJobStatus, metav1.ConditionTrue,
		"WorkspaceTuningJobStatusStarted", "Tuning job has started"); err != nil {
		klog.ErrorS(err, "failed to update workspace status", "workspace", klog.KObj(wObj))
		return err
	}

	return nil
}

// applyInference applies inference spec.
func (c *WorkspaceReconciler) applyInference(ctx context.Context, wObj *kaitov1beta1.Workspace) error {
	var err error
	func() {
		if wObj.Inference.Template != nil {
			var workloadObj client.Object
			// TODO: handle update
			workloadObj, err = inference.CreateTemplateInference(ctx, wObj, c.Client)
			if err != nil {
				return
			}

			if err = resources.CheckResourceStatus(workloadObj, c.Client, time.Duration(10)*time.Minute); err != nil {
				return
			}
		} else if wObj.Inference != nil && wObj.Inference.Preset != nil {
			presetName := string(wObj.Inference.Preset.Name)
			model := plugin.KaitoModelRegister.MustGet(presetName)
			inferenceParam := model.GetInferenceParameters()
			revisionStr := wObj.Annotations[kaitov1beta1.WorkspaceRevisionAnnotation]

			// Generate the inference workload (including adapters and their associated
			// volumes) ahead of time. This is important to ensure we are modifying the
			// correct type of workload (Deployment or StatefulSet) based on the model's
			// inference parameters.
			var workloadObj client.Object
			workloadObj, err = inference.GeneratePresetInference(ctx, wObj, revisionStr, model, c.Client)
			if err != nil {
				return
			}

			// Assign the correct type to existingObj based on the type of workloadObj.
			var existingObj client.Object
			var desiredPodSpec *corev1.PodSpec
			switch workloadObj := workloadObj.(type) {
			case *appsv1.StatefulSet:
				existingObj = &appsv1.StatefulSet{}
				desiredPodSpec = &workloadObj.Spec.Template.Spec
			case *appsv1.Deployment:
				existingObj = &appsv1.Deployment{}
				desiredPodSpec = &workloadObj.Spec.Template.Spec
			}

			if err = resources.GetResource(ctx, wObj.Name, wObj.Namespace, c.Client, existingObj); err == nil {
				klog.InfoS("An inference workload already exists for workspace", "workspace", klog.KObj(wObj))
				if _, ok := workloadObj.(*appsv1.Deployment); ok {
					isScaled := ctrlutils.ScaleDeploymentIfNeeded(ctx, c.Client, client.ObjectKeyFromObject(existingObj), wObj)
					if isScaled {
						return
					}
				}

				annotations := existingObj.GetAnnotations()
				if annotations == nil {
					annotations = make(map[string]string)
				}

				currentRevisionStr, ok := annotations[kaitov1beta1.WorkspaceRevisionAnnotation]
				// If the current workload revision matches the one in Workspace, we do not need to update it.
				if ok && currentRevisionStr == revisionStr {
					return
				}

				var spec *corev1.PodSpec
				switch existingObj := existingObj.(type) {
				case *appsv1.StatefulSet:
					spec = &existingObj.Spec.Template.Spec
				case *appsv1.Deployment:
					spec = &existingObj.Spec.Template.Spec
				}

				// Selectively update the pod spec fields that are relevant to inference,
				// and leave the rest unchanged in case user has customized them.
				spec.Containers[0].Env = desiredPodSpec.Containers[0].Env
				spec.Containers[0].VolumeMounts = desiredPodSpec.Containers[0].VolumeMounts
				spec.InitContainers = desiredPodSpec.InitContainers
				spec.Volumes = desiredPodSpec.Volumes

				annotations[kaitov1beta1.WorkspaceRevisionAnnotation] = revisionStr
				existingObj.SetAnnotations(annotations)

				// Update it with the latest one generated above.
				err = c.Update(ctx, existingObj)
				return
			} else if !apierrors.IsNotFound(err) {
				return
			}

			err = resources.CreateResource(ctx, workloadObj, c.Client)
			if client.IgnoreAlreadyExists(err) != nil {
				return
			}
			if err = resources.CheckResourceStatus(workloadObj, c.Client, inferenceParam.ReadinessTimeout); err != nil {
				return
			}
		}
	}()

	if err != nil {
		if updateErr := c.updateStatusConditionIfNotMatch(ctx, wObj, kaitov1beta1.WorkspaceConditionTypeInferenceStatus, metav1.ConditionFalse,
			"WorkspaceInferenceStatusFailed", err.Error()); updateErr != nil {
			klog.ErrorS(updateErr, "failed to update workspace status", "workspace", klog.KObj(wObj))
			return updateErr
		} else {
			return err
		}
	}

	if err := c.updateStatusConditionIfNotMatch(ctx, wObj, kaitov1beta1.WorkspaceConditionTypeInferenceStatus, metav1.ConditionTrue,
		"WorkspaceInferenceStatusSuccess", "Inference has been deployed successfully"); err != nil {
		klog.ErrorS(err, "failed to update workspace status", "workspace", klog.KObj(wObj))
		return err
	}
	return nil
}

// updateWorkspaceWithRetry gets the latest workspace object, applies the modify function, and retries on conflict
func updateWorkspaceWithRetry(ctx context.Context, c client.Client, wObj *kaitov1beta1.Workspace, modifyFn func(*kaitov1beta1.Workspace) error) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latestWorkspace := &kaitov1beta1.Workspace{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(wObj), latestWorkspace); err != nil {
			return err
		}
		if err := modifyFn(latestWorkspace); err != nil {
			return err
		}
		return c.Update(ctx, latestWorkspace)
	})
}

// SetupWithManager sets up the controller with the Manager.
func (c *WorkspaceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	c.Recorder = mgr.GetEventRecorderFor("Workspace")

	builder := ctrl.NewControllerManagedBy(mgr).
		For(&kaitov1beta1.Workspace{}).
		Owns(&corev1.Service{}).
		Owns(&appsv1.ControllerRevision{}).
		Owns(&appsv1.Deployment{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&batchv1.Job{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: 5})

	go monitorWorkspaces(context.Background(), c.Client)

	return builder.Complete(c)
}

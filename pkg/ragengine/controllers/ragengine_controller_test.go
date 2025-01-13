// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	azurev1alpha2 "github.com/Azure/karpenter-provider-azure/pkg/apis/v1alpha2"
	awsv1beta1 "github.com/aws/karpenter-provider-aws/pkg/apis/v1beta1"
	"github.com/awslabs/operatorpkg/status"
	"github.com/stretchr/testify/mock"
	"gotest.tools/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"
	karpenterv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	"github.com/kaito-project/kaito/api/v1alpha1"
	"github.com/kaito-project/kaito/pkg/utils/consts"
	"github.com/kaito-project/kaito/pkg/utils/test"
)

func TestApplyRAGEngineResource(t *testing.T) {
	test.RegisterTestModel()
	testcases := map[string]struct {
		callMocks     func(c *test.MockClient)
		expectedError error
		ragengine     v1alpha1.RAGEngine
	}{
		"Fail to apply ragengine because associated nodeClaim cannot be retrieved": {
			callMocks: func(c *test.MockClient) {
				c.On("List", mock.IsType(context.Background()), mock.IsType(&karpenterv1.NodeClaimList{}), mock.Anything).Return(errors.New("failed to retrieve nodeClaims"))

			},
			ragengine:     *test.MockRAGEngineDistributedModel,
			expectedError: errors.New("failed to retrieve nodeClaims"),
		},
		"Fail to apply ragengine with nodeClaims because can't get qualified nodes": {
			callMocks: func(c *test.MockClient) {
				nodeClaimList := test.MockNodeClaimList
				relevantMap := c.CreateMapWithType(nodeClaimList)
				c.CreateOrUpdateObjectInMap(&test.MockNodeClaim)

				//insert nodeClaim objects into the map
				for _, obj := range nodeClaimList.Items {
					m := obj
					objKey := client.ObjectKeyFromObject(&m)

					relevantMap[objKey] = &m
				}
				c.On("List", mock.IsType(context.Background()), mock.IsType(&karpenterv1.NodeClaimList{}), mock.Anything).Return(nil)
				c.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&karpenterv1.NodeClaim{}), mock.Anything).Return(nil)

				c.On("List", mock.IsType(context.Background()), mock.IsType(&corev1.NodeList{}), mock.Anything).Return(errors.New("failed to list nodes"))
			},
			ragengine:     *test.MockRAGEngineDistributedModel,
			expectedError: errors.New("failed to list nodes"),
		},
		"Successfully apply ragengine resource with nodeClaim": {
			callMocks: func(c *test.MockClient) {
				nodeList := test.MockNodeList
				relevantMap := c.CreateMapWithType(nodeList)
				//insert node objects into the map
				for _, obj := range nodeList.Items {
					n := obj
					objKey := client.ObjectKeyFromObject(&n)

					relevantMap[objKey] = &n
				}

				c.On("List", mock.IsType(context.Background()), mock.IsType(&karpenterv1.NodeClaimList{}), mock.Anything).Return(nil)
				c.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&karpenterv1.NodeClaim{}), mock.Anything).Return(nil)

				c.On("List", mock.IsType(context.Background()), mock.IsType(&corev1.NodeList{}), mock.Anything).Return(nil)

				c.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&v1alpha1.RAGEngine{}), mock.Anything).Return(nil)
				c.StatusMock.On("Update", mock.IsType(context.Background()), mock.IsType(&v1alpha1.RAGEngine{}), mock.Anything).Return(nil)

			},
			ragengine:     *test.MockRAGEngineDistributedModel,
			expectedError: nil,
		},
	}

	for k, tc := range testcases {
		t.Run(k, func(t *testing.T) {
			mockClient := test.NewClient()
			tc.callMocks(mockClient)

			mockNodeClaim := &karpenterv1.NodeClaim{}

			mockClient.UpdateCb = func(key types.NamespacedName) {
				mockClient.GetObjectFromMap(mockNodeClaim, key)
				mockNodeClaim.Status.Conditions = []status.Condition{
					{
						Type:   string(apis.ConditionReady),
						Status: v1.ConditionTrue,
					},
				}
				mockClient.CreateOrUpdateObjectInMap(mockNodeClaim)
			}

			reconciler := &RAGEngineReconciler{
				Client: mockClient,
				Scheme: test.NewTestScheme(),
			}
			ctx := context.Background()

			err := reconciler.applyRAGEngineResource(ctx, &tc.ragengine)
			if tc.expectedError == nil {
				assert.Check(t, err == nil, "Not expected to return error")
			} else {
				assert.Equal(t, tc.expectedError.Error(), err.Error())
			}
		})
	}
}

func TestGetAllQualifiedNodesforRAGEngine(t *testing.T) {
	testcases := map[string]struct {
		callMocks     func(c *test.MockClient)
		expectedError error
	}{
		"Fails to get qualified nodes because can't list nodes": {
			callMocks: func(c *test.MockClient) {
				c.On("List", mock.IsType(context.Background()), mock.IsType(&corev1.NodeList{}), mock.Anything).Return(errors.New("Failed to list nodes"))
			},
			expectedError: errors.New("Failed to list nodes"),
		},
		"Gets all qualified nodes": {
			callMocks: func(c *test.MockClient) {
				nodeList := test.MockNodeList
				deletedNode := corev1.Node{
					ObjectMeta: v1.ObjectMeta{
						Name: "node4",
						Labels: map[string]string{
							corev1.LabelInstanceTypeStable: "Standard_NC12s_v3",
						},
						DeletionTimestamp: &v1.Time{Time: time.Now()},
					},
				}
				nodeList.Items = append(nodeList.Items, deletedNode)

				relevantMap := c.CreateMapWithType(nodeList)
				//insert node objects into the map
				for _, obj := range test.MockNodeList.Items {
					n := obj
					objKey := client.ObjectKeyFromObject(&n)

					relevantMap[objKey] = &n
				}

				c.On("List", mock.IsType(context.Background()), mock.IsType(&corev1.NodeList{}), mock.Anything).Return(nil)
			},
			expectedError: nil,
		},
	}

	for k, tc := range testcases {
		t.Run(k, func(t *testing.T) {
			mockClient := test.NewClient()
			mockRAGEngine := test.MockRAGEngineDistributedModel
			reconciler := &RAGEngineReconciler{
				Client: mockClient,
				Scheme: test.NewTestScheme(),
			}
			ctx := context.Background()

			tc.callMocks(mockClient)

			nodes, err := reconciler.getAllQualifiedNodes(ctx, mockRAGEngine)
			if tc.expectedError == nil {
				assert.Check(t, err == nil, "Not expected to return error")
				assert.Check(t, nodes != nil, "Response node array should not be nil")
				assert.Check(t, len(nodes) == 1, "One out of three nodes should be qualified")
			} else {
				assert.Equal(t, tc.expectedError.Error(), err.Error())
				assert.Check(t, nodes == nil, "Response node array should be nil")
			}
		})
	}
}

func TestCreateAndValidateMachineNodeforRAGEngine(t *testing.T) {
	test.RegisterTestModel()
	testcases := map[string]struct {
		callMocks        func(c *test.MockClient)
		cloudProvider    string
		objectConditions []status.Condition
		ragengine        v1alpha1.RAGEngine
		expectedError    error
	}{
		"An Azure nodeClaim is successfully created": {
			callMocks: func(c *test.MockClient) {
				c.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&azurev1alpha2.AKSNodeClass{}), mock.Anything).Return(nil)
				c.On("Create", mock.IsType(context.Background()), mock.IsType(&azurev1alpha2.AKSNodeClass{}), mock.Anything).Return(nil)
				c.On("Create", mock.IsType(context.Background()), mock.IsType(&karpenterv1.NodeClaim{}), mock.Anything).Return(nil)
				c.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&karpenterv1.NodeClaim{}), mock.Anything).Return(nil)
				c.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&corev1.Node{}), mock.Anything).Return(nil)
			},
			cloudProvider: consts.AzureCloudName,
			objectConditions: []status.Condition{
				{
					Type:   string(apis.ConditionReady),
					Status: v1.ConditionTrue,
				},
			},
			ragengine:     *test.MockRAGEngineDistributedModel,
			expectedError: nil,
		},
		"An AWS nodeClaim is successfully created": {
			callMocks: func(c *test.MockClient) {
				c.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&awsv1beta1.EC2NodeClass{}), mock.Anything).Return(nil)
				c.On("Create", mock.IsType(context.Background()), mock.IsType(&awsv1beta1.EC2NodeClass{}), mock.Anything).Return(nil)
				c.On("Create", mock.IsType(context.Background()), mock.IsType(&karpenterv1.NodeClaim{}), mock.Anything).Return(nil)
				c.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&karpenterv1.NodeClaim{}), mock.Anything).Return(nil)
				c.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&corev1.Node{}), mock.Anything).Return(nil)
			},
			cloudProvider: consts.AWSCloudName,
			objectConditions: []status.Condition{
				{
					Type:   string(apis.ConditionReady),
					Status: v1.ConditionTrue,
				},
			},
			ragengine:     *test.MockRAGEngineDistributedModel,
			expectedError: nil,
		},
	}

	for k, tc := range testcases {
		t.Run(k, func(t *testing.T) {
			mockClient := test.NewClient()
			mockNodeClaim := &karpenterv1.NodeClaim{}

			mockClient.UpdateCb = func(key types.NamespacedName) {
				mockClient.GetObjectFromMap(mockNodeClaim, key)
				mockNodeClaim.Status.Conditions = tc.objectConditions
				mockClient.CreateOrUpdateObjectInMap(mockNodeClaim)
			}

			if tc.cloudProvider != "" {
				t.Setenv("CLOUD_PROVIDER", tc.cloudProvider)

			}

			tc.callMocks(mockClient)

			reconciler := &RAGEngineReconciler{
				Client: mockClient,
				Scheme: test.NewTestScheme(),
			}
			ctx := context.Background()

			node, err := reconciler.createAndValidateNode(ctx, &tc.ragengine)
			if tc.expectedError == nil {
				assert.Check(t, err == nil, "Not expected to return error")
				assert.Check(t, node != nil, "Response node should not be nil")
			} else {
				assert.Equal(t, tc.expectedError.Error(), err.Error())
			}
		})
	}
}

func TestUpdateControllerRevision1(t *testing.T) {
	testcases := map[string]struct {
		callMocks     func(c *test.MockClient)
		ragengine     v1alpha1.RAGEngine
		expectedError error
		verifyCalls   func(c *test.MockClient)
	}{

		"No new revision needed": {
			callMocks: func(c *test.MockClient) {
				c.On("List", mock.IsType(context.Background()), mock.IsType(&appsv1.ControllerRevisionList{}), mock.Anything, mock.Anything).Return(nil)
				c.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&appsv1.ControllerRevision{}), mock.Anything).
					Run(func(args mock.Arguments) {
						dep := args.Get(2).(*appsv1.ControllerRevision)
						*dep = appsv1.ControllerRevision{
							ObjectMeta: v1.ObjectMeta{
								Annotations: map[string]string{
									RAGEngineHashAnnotation: "7985249e078eb041e38c10c3637032b2d352616c609be8542a779460d3ff1d67",
								},
							},
						}
					}).
					Return(nil)
				c.On("Update", mock.IsType(context.Background()), mock.IsType(&v1alpha1.RAGEngine{}), mock.Anything).
					Return(nil)
			},
			ragengine:     test.MockRAGEngineWithComputeHash,
			expectedError: nil,
			verifyCalls: func(c *test.MockClient) {
				c.AssertNumberOfCalls(t, "List", 1)
				c.AssertNumberOfCalls(t, "Create", 0)
				c.AssertNumberOfCalls(t, "Get", 1)
				c.AssertNumberOfCalls(t, "Delete", 0)
				c.AssertNumberOfCalls(t, "Update", 1)
			},
		},

		"Fail to create ControllerRevision": {
			callMocks: func(c *test.MockClient) {
				c.On("List", mock.IsType(context.Background()), mock.IsType(&appsv1.ControllerRevisionList{}), mock.Anything, mock.Anything).Return(nil)
				c.On("Create", mock.IsType(context.Background()), mock.IsType(&appsv1.ControllerRevision{}), mock.Anything).Return(errors.New("failed to create ControllerRevision"))
				c.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&appsv1.ControllerRevision{}), mock.Anything).
					Return(apierrors.NewNotFound(appsv1.Resource("ControllerRevision"), test.MockRAGEngineFailToCreateCR.Name))
				c.On("Update", mock.IsType(context.Background()), mock.IsType(&v1alpha1.RAGEngine{}), mock.Anything).
					Return(nil)
			},
			ragengine:     test.MockRAGEngineFailToCreateCR,
			expectedError: errors.New("failed to create new ControllerRevision: failed to create ControllerRevision"),
			verifyCalls: func(c *test.MockClient) {
				c.AssertNumberOfCalls(t, "List", 1)
				c.AssertNumberOfCalls(t, "Create", 1)
				c.AssertNumberOfCalls(t, "Get", 1)
				c.AssertNumberOfCalls(t, "Delete", 0)
				c.AssertNumberOfCalls(t, "Update", 0)
			},
		},
		"Successfully create new ControllerRevision": {
			callMocks: func(c *test.MockClient) {
				c.On("List", mock.IsType(context.Background()), mock.IsType(&appsv1.ControllerRevisionList{}), mock.Anything, mock.Anything).Return(nil)
				c.On("Create", mock.IsType(context.Background()), mock.IsType(&appsv1.ControllerRevision{}), mock.Anything).Return(nil)
				c.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&appsv1.ControllerRevision{}), mock.Anything).
					Return(apierrors.NewNotFound(appsv1.Resource("ControllerRevision"), test.MockRAGEngineFailToCreateCR.Name))
				c.On("Update", mock.IsType(context.Background()), mock.IsType(&v1alpha1.RAGEngine{}), mock.Anything).
					Return(nil)
			},
			ragengine:     test.MockRAGEngineSuccessful,
			expectedError: nil,
			verifyCalls: func(c *test.MockClient) {
				c.AssertNumberOfCalls(t, "List", 1)
				c.AssertNumberOfCalls(t, "Create", 1)
				c.AssertNumberOfCalls(t, "Get", 1)
				c.AssertNumberOfCalls(t, "Delete", 0)
				c.AssertNumberOfCalls(t, "Update", 1)
			},
		},

		"Successfully delete old ControllerRevision": {
			callMocks: func(c *test.MockClient) {
				revisions := &appsv1.ControllerRevisionList{}
				jsonData, _ := json.Marshal(test.MockRAGEngineWithUpdatedDeployment)

				for i := 0; i <= consts.MaxRevisionHistoryLimit; i++ {
					revision := &appsv1.ControllerRevision{
						ObjectMeta: v1.ObjectMeta{
							Name: fmt.Sprintf("revision-%d", i),
						},
						Revision: int64(i),
						Data:     runtime.RawExtension{Raw: jsonData},
					}
					revisions.Items = append(revisions.Items, *revision)
				}
				relevantMap := c.CreateMapWithType(revisions)

				for _, obj := range revisions.Items {
					m := obj
					objKey := client.ObjectKeyFromObject(&m)
					relevantMap[objKey] = &m
				}
				c.On("List", mock.IsType(context.Background()), mock.IsType(&appsv1.ControllerRevisionList{}), mock.Anything, mock.Anything).Return(nil)
				c.On("Create", mock.IsType(context.Background()), mock.IsType(&appsv1.ControllerRevision{}), mock.Anything).Return(nil)
				c.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&appsv1.ControllerRevision{}), mock.Anything).
					Return(apierrors.NewNotFound(appsv1.Resource("ControllerRevision"), test.MockRAGEngineFailToCreateCR.Name))
				c.On("Delete", mock.IsType(context.Background()), mock.IsType(&appsv1.ControllerRevision{}), mock.Anything).Return(nil)
				c.On("Update", mock.IsType(context.Background()), mock.IsType(&v1alpha1.RAGEngine{}), mock.Anything).
					Return(nil)
			},
			ragengine:     test.MockRAGEngineWithDeleteOldCR,
			expectedError: nil,
			verifyCalls: func(c *test.MockClient) {
				c.AssertNumberOfCalls(t, "List", 1)
				c.AssertNumberOfCalls(t, "Create", 1)
				c.AssertNumberOfCalls(t, "Get", 1)
				c.AssertNumberOfCalls(t, "Delete", 1)
				c.AssertNumberOfCalls(t, "Update", 1)
			},
		},
	}
	for k, tc := range testcases {
		t.Run(k, func(t *testing.T) {
			mockClient := test.NewClient()
			tc.callMocks(mockClient)

			reconciler := &RAGEngineReconciler{
				Client: mockClient,
				Scheme: test.NewTestScheme(),
			}
			ctx := context.Background()

			err := reconciler.syncControllerRevision(ctx, &tc.ragengine)
			if tc.expectedError == nil {
				assert.Check(t, err == nil, "Not expected to return error")
			} else {
				assert.Equal(t, tc.expectedError.Error(), err.Error())
			}
			if tc.verifyCalls != nil {
				tc.verifyCalls(mockClient)
			}
		})
	}
}

func TestApplyRAG(t *testing.T) {
	test.RegisterTestModel()
	testcases := map[string]struct {
		callMocks     func(c *test.MockClient)
		ragengine     v1alpha1.RAGEngine
		expectedError error
		verifyCalls   func(c *test.MockClient)
	}{

		"Fail because associated workload with ragengine cannot be retrieved": {
			callMocks: func(c *test.MockClient) {
				c.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&appsv1.Deployment{}), mock.Anything).Return(errors.New("Failed to get resource"))
				c.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&v1alpha1.RAGEngine{}), mock.Anything).Return(nil)
				c.StatusMock.On("Update", mock.IsType(context.Background()), mock.IsType(&v1alpha1.RAGEngine{}), mock.Anything).Return(nil)
			},
			ragengine:     *test.MockRAGEngineWithRevision1,
			expectedError: errors.New("Failed to get resource"),
			verifyCalls: func(c *test.MockClient) {
				c.AssertNumberOfCalls(t, "List", 0)
				c.AssertNumberOfCalls(t, "Create", 0)
				c.AssertNumberOfCalls(t, "Get", 5)
				c.AssertNumberOfCalls(t, "Delete", 0)
			},
		},

		"Create preset inference because inference workload did not exist": {
			callMocks: func(c *test.MockClient) {
				c.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&appsv1.Deployment{}), mock.Anything).Return(test.NotFoundError()).Times(4)
				c.On("Get", mock.Anything, mock.Anything, mock.IsType(&appsv1.Deployment{}), mock.Anything).Return(nil).Run(func(args mock.Arguments) {
					depObj := &appsv1.Deployment{}
					key := client.ObjectKey{Namespace: "kaito", Name: "testRAGEngine"}
					c.GetObjectFromMap(depObj, key)
					depObj.Status.ReadyReplicas = 1
					c.CreateOrUpdateObjectInMap(depObj)
				})
				c.On("Create", mock.IsType(context.Background()), mock.IsType(&appsv1.Deployment{}), mock.Anything).Return(nil)

				c.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&corev1.Service{}), mock.Anything).Return(nil)

				c.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&v1alpha1.RAGEngine{}), mock.Anything).Return(nil)
				c.StatusMock.On("Update", mock.IsType(context.Background()), mock.IsType(&v1alpha1.RAGEngine{}), mock.Anything).Return(nil)
			},
			ragengine: *test.MockRAGEngineWithRevision1,
			verifyCalls: func(c *test.MockClient) {
				c.AssertNumberOfCalls(t, "List", 0)
				c.AssertNumberOfCalls(t, "Create", 1)
				c.AssertNumberOfCalls(t, "Get", 7)
				c.AssertNumberOfCalls(t, "Delete", 0)
				c.AssertNumberOfCalls(t, "Update", 0)
			},
			expectedError: nil,
		},

		"Apply inference from existing workload": {
			callMocks: func(c *test.MockClient) {
				c.On("Get", mock.Anything, mock.Anything, mock.IsType(&appsv1.Deployment{}), mock.Anything).
					Run(func(args mock.Arguments) {
						dep := args.Get(2).(*appsv1.Deployment)
						*dep = *test.MockRAGDeploymentUpdated.DeepCopy()
					}).
					Return(nil)

				c.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&v1alpha1.RAGEngine{}), mock.Anything).Return(nil)
				c.StatusMock.On("Update", mock.IsType(context.Background()), mock.IsType(&v1alpha1.RAGEngine{}), mock.Anything).Return(nil)
			},
			ragengine:     *test.MockRAGEngineWithRevision1,
			expectedError: nil,
			verifyCalls: func(c *test.MockClient) {
				c.AssertNumberOfCalls(t, "List", 0)
				c.AssertNumberOfCalls(t, "Create", 0)
				c.AssertNumberOfCalls(t, "Get", 3)
				c.AssertNumberOfCalls(t, "Delete", 0)
				c.AssertNumberOfCalls(t, "Update", 0)
			},
		},

		"Update deployment with new configuration": {
			callMocks: func(c *test.MockClient) {
				// Mocking existing Deployment object
				c.On("Get", mock.Anything, mock.Anything, mock.IsType(&appsv1.Deployment{}), mock.Anything).
					Run(func(args mock.Arguments) {
						dep := args.Get(2).(*appsv1.Deployment)
						*dep = *test.MockRAGDeploymentUpdated.DeepCopy()
					}).
					Return(nil)

				c.On("Update", mock.IsType(context.Background()), mock.IsType(&appsv1.Deployment{}), mock.Anything).Return(nil)

				c.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&v1alpha1.RAGEngine{}), mock.Anything).Return(nil)
				c.StatusMock.On("Update", mock.IsType(context.Background()), mock.IsType(&v1alpha1.RAGEngine{}), mock.Anything).Return(nil)
			},
			ragengine:     *test.MockRAGEngineWithPreset,
			expectedError: nil,
			verifyCalls: func(c *test.MockClient) {
				c.AssertNumberOfCalls(t, "List", 0)
				c.AssertNumberOfCalls(t, "Create", 0)
				c.AssertNumberOfCalls(t, "Get", 3)
				c.AssertNumberOfCalls(t, "Delete", 0)
				c.AssertNumberOfCalls(t, "Update", 1)
			},
		},
	}

	for k, tc := range testcases {
		t.Run(k, func(t *testing.T) {
			t.Setenv("CLOUD_PROVIDER", consts.AzureCloudName)

			mockClient := test.NewClient()
			tc.callMocks(mockClient)

			reconciler := &RAGEngineReconciler{
				Client: mockClient,
				Scheme: test.NewTestScheme(),
			}
			ctx := context.Background()

			err := reconciler.applyRAG(ctx, &tc.ragengine)
			if tc.expectedError == nil {
				assert.Check(t, err == nil, "Not expected to return error")
			} else {
				assert.Equal(t, tc.expectedError.Error(), err.Error())
			}
			if tc.verifyCalls != nil {
				tc.verifyCalls(mockClient)
			}
		})
	}
}

func TestEnsureService(t *testing.T) {
	test.RegisterTestModel()
	testcases := map[string]struct {
		callMocks     func(c *test.MockClient)
		expectedError error
		ragengine     v1alpha1.RAGEngine
		verifyCalls   func(c *test.MockClient)
	}{

		"Existing service is found for RAGEngine": {
			callMocks: func(c *test.MockClient) {
				c.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&corev1.Service{}), mock.Anything).Return(nil)
			},
			expectedError: nil,
			ragengine:     *test.MockRAGEngineWithPreset,
			verifyCalls: func(c *test.MockClient) {
				c.AssertNumberOfCalls(t, "List", 0)
				c.AssertNumberOfCalls(t, "Create", 0)
				c.AssertNumberOfCalls(t, "Get", 1)
				c.AssertNumberOfCalls(t, "Delete", 0)
				c.AssertNumberOfCalls(t, "Update", 0)
			},
		},

		"Service creation fails": {
			callMocks: func(c *test.MockClient) {
				c.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&corev1.Service{}), mock.Anything).Return(test.NotFoundError())
				c.On("Create", mock.IsType(context.Background()), mock.IsType(&corev1.Service{}), mock.Anything).Return(errors.New("cannot create service"))
			},
			expectedError: errors.New("cannot create service"),
			ragengine:     *test.MockRAGEngineWithPreset,
			verifyCalls: func(c *test.MockClient) {
				c.AssertNumberOfCalls(t, "List", 0)
				c.AssertNumberOfCalls(t, "Create", 4)
				c.AssertNumberOfCalls(t, "Get", 4)
				c.AssertNumberOfCalls(t, "Delete", 0)
				c.AssertNumberOfCalls(t, "Update", 0)
			},
		},

		"Successfully creates a new service": {
			callMocks: func(c *test.MockClient) {
				c.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&corev1.Service{}), mock.Anything).Return(test.NotFoundError())
				c.On("Create", mock.IsType(context.Background()), mock.IsType(&corev1.Service{}), mock.Anything).Return(nil)
			},
			expectedError: nil,
			ragengine:     *test.MockRAGEngineWithPreset,
			verifyCalls: func(c *test.MockClient) {
				c.AssertNumberOfCalls(t, "List", 0)
				c.AssertNumberOfCalls(t, "Create", 1)
				c.AssertNumberOfCalls(t, "Get", 4)
				c.AssertNumberOfCalls(t, "Delete", 0)
				c.AssertNumberOfCalls(t, "Update", 0)
			},
		},
	}

	for k, tc := range testcases {
		t.Run(k, func(t *testing.T) {
			mockClient := test.NewClient()
			tc.callMocks(mockClient)

			reconciler := &RAGEngineReconciler{
				Client: mockClient,
				Scheme: test.NewTestScheme(),
			}
			ctx := context.Background()

			err := reconciler.ensureService(ctx, &tc.ragengine)
			if tc.expectedError == nil {
				assert.Check(t, err == nil, "Not expected to return error")
			} else {
				assert.Equal(t, tc.expectedError.Error(), err.Error())
			}
			if tc.verifyCalls != nil {
				tc.verifyCalls(mockClient)
			}
		})
	}
}

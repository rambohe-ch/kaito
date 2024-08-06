// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package utils

import (
	"context"
	"fmt"
	"github.com/azure/kaito/pkg/sku"
	"github.com/azure/kaito/pkg/utils/consts"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"knative.dev/pkg/apis"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func Contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

// SearchMap performs a search for a key in a map[string]interface{}.
func SearchMap(m map[string]interface{}, key string) (value interface{}, exists bool) {
	if val, ok := m[key]; ok {
		return val, true
	}
	return nil, false
}

// SearchRawExtension performs a search for a key within a runtime.RawExtension.
func SearchRawExtension(raw runtime.RawExtension, key string) (interface{}, bool, error) {
	var data map[string]interface{}
	if err := yaml.Unmarshal(raw.Raw, &data); err != nil {
		return nil, false, fmt.Errorf("failed to unmarshal runtime.RawExtension: %w", err)
	}

	result, found := data[key]
	if !found {
		return nil, false, nil
	}

	return result, true, nil
}

func MergeConfigMaps(baseMap, overrideMap map[string]string) map[string]string {
	merged := make(map[string]string)
	for k, v := range baseMap {
		merged[k] = v
	}

	// Override with values from overrideMap
	for k, v := range overrideMap {
		merged[k] = v
	}

	return merged
}

func BuildCmdStr(baseCommand string, runParams map[string]string) string {
	updatedBaseCommand := baseCommand
	for key, value := range runParams {
		if value == "" {
			updatedBaseCommand = fmt.Sprintf("%s --%s", updatedBaseCommand, key)
		} else {
			updatedBaseCommand = fmt.Sprintf("%s --%s=%s", updatedBaseCommand, key, value)
		}
	}

	return updatedBaseCommand
}

func ShellCmd(command string) []string {
	return []string{
		"/bin/sh",
		"-c",
		command,
	}
}

func GetReleaseNamespace() (string, error) {
	// Path to the namespace file inside a Kubernetes pod
	namespaceFilePath := "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

	// Attempt to read the namespace from the file
	if content, err := ioutil.ReadFile(namespaceFilePath); err == nil {
		return string(content), nil
	}

	// Fallback: Read the namespace from an environment variable
	if namespace, exists := os.LookupEnv(consts.DefaultReleaseNamespaceEnvVar); exists {
		return namespace, nil
	}
	return "", fmt.Errorf("failed to determine release namespace from file %s and env var %s", namespaceFilePath, consts.DefaultReleaseNamespaceEnvVar)
}

func GetSKUHandler() (sku.CloudSKUHandler, error) {
	// Get the cloud provider from the environment
	provider := os.Getenv("CLOUD_PROVIDER")

	if provider == "" {
		return nil, apis.ErrMissingField("CLOUD_PROVIDER environment variable must be set")
	}
	// Select the correct SKU handler based on the cloud provider
	skuHandler := sku.GetCloudSKUHandler(provider)
	if skuHandler == nil {
		return nil, apis.ErrInvalidValue(fmt.Sprintf("Unsupported cloud provider %s", provider), "CLOUD_PROVIDER")
	}

	return skuHandler, nil
}

func GetSKUNumGPUs(ctx context.Context, kubeClient client.Client, workerNodes []string, instanceType, defaultGPUCount string) (string, error) {
	skuHandler, err := GetSKUHandler()
	if err != nil {
		return "", apis.ErrInvalidValue(fmt.Sprintf("Failed to get SKU handler: %v", err), "sku")
	}

	skuNumGPUs := defaultGPUCount // Default to using the provided default GPU count

	skuConfig, skuExists := skuHandler.GetGPUConfigs()[instanceType]
	if skuExists {
		skuNumGPUs = fmt.Sprintf("%d", skuConfig.GPUCount)
	} else {
		skuGPUCount, err := FetchGPUCountFromNodes(ctx, kubeClient, workerNodes)
		if err != nil {
			fmt.Printf("Failed to fetch GPU count from nodes: %v", err)
		} else if skuGPUCount != "" {
			skuNumGPUs = skuGPUCount
		}
	}

	return skuNumGPUs, nil
}

// FetchGPUCountFromNodes retrieves the GPU count from the given node names.
func FetchGPUCountFromNodes(ctx context.Context, kubeClient client.Client, nodeNames []string) (string, error) {
	if len(nodeNames) == 0 {
		return "", fmt.Errorf("no worker nodes found in the workspace")
	}

	var allNodes v1.NodeList
	for _, nodeName := range nodeNames {
		nodeList := &v1.NodeList{}
		fieldSelector := fields.OneTermEqualSelector("metadata.name", nodeName)
		err := kubeClient.List(ctx, nodeList, &client.ListOptions{
			FieldSelector: fieldSelector,
		})
		if err != nil {
			fmt.Printf("Failed to list Node object %s: %v\n", nodeName, err)
			continue
		}
		allNodes.Items = append(allNodes.Items, nodeList.Items...)
	}

	return GetPerNodeGPUCountFromNodes(&allNodes), nil
}

func GetPerNodeGPUCountFromNodes(nodeList *v1.NodeList) string {
	for _, node := range nodeList.Items {
		gpuCount, exists := node.Status.Capacity[consts.NvidiaGPU]
		if exists && gpuCount.String() != "" {
			return gpuCount.String()
		}
	}
	return ""
}

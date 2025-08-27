/*
Copyright 2018 The Kubernetes Authors.

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

// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package conf_util

import (
	"fmt"
	"io/ioutil"
	"strings"

	"gopkg.in/yaml.v2"

	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/conf"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/framework"
)

func ResolveConfigurationFromFile(confPath string) (*conf.SchedulerConfiguration, error) {
	defaultConfig := conf.GetDefaultSchedulerConfiguration()

	schedulerConfStr, err := readSchedulerConf(confPath)
	if err != nil {
		return nil, err
	}

	if len(schedulerConfStr) == 0 {
		return defaultConfig, nil
	}

	schedulerConfigFromCM, err := loadSchedulerConf(schedulerConfStr)
	if err != nil {
		return nil, err
	}

	if len(schedulerConfigFromCM.Actions) == 0 {
		schedulerConfigFromCM.Actions = defaultConfig.Actions
	}
	if len(schedulerConfigFromCM.Tiers) == 0 {
		schedulerConfigFromCM.Tiers = defaultConfig.Tiers
	}
	if len(schedulerConfigFromCM.QueueDepthPerAction) == 0 {
		schedulerConfigFromCM.QueueDepthPerAction = defaultConfig.QueueDepthPerAction
	}

	return schedulerConfigFromCM, nil
}

func GetActionsFromConfig(conf *conf.SchedulerConfiguration) ([]framework.Action, error) {
	var actions []framework.Action
	actionNames := strings.Split(conf.Actions, ",")
	for _, actionName := range actionNames {
		if action, found := framework.GetAction(strings.TrimSpace(actionName)); found {
			actions = append(actions, action)
		} else {
			return nil, fmt.Errorf("failed to find Action %s as given in the config, ignore it", actionName)
		}
	}
	return actions, nil
}

func loadSchedulerConf(confStr string) (*conf.SchedulerConfiguration, error) {
	schedulerConf := &conf.SchedulerConfiguration{}

	buf := make([]byte, len(confStr))
	copy(buf, confStr)

	if err := yaml.Unmarshal(buf, schedulerConf); err != nil {
		return nil, err
	}
	// Validate that the actions config section is valid
	if _, err := GetActionsFromConfig(schedulerConf); err != nil {
		return nil, err
	}

	return schedulerConf, nil
}

func readSchedulerConf(confPath string) (string, error) {
	if len(confPath) == 0 {
		return "", nil
	}

	dat, err := ioutil.ReadFile(confPath)
	if err != nil {
		return "", err
	}
	return string(dat), nil
}

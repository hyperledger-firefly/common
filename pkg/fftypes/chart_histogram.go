// Copyright © 2025 Kaleido, Inc.
//
// SPDX-License-Identifier: Apache-2.0
//
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

package fftypes

const (
	ChartHistogramMaxBuckets = 100
	ChartHistogramMinBuckets = 1
)

// ChartHistogram: list of buckets with types
type ChartHistogram struct {
	Count     string                `ffstruct:"ChartHistogram" json:"count"`
	Timestamp *FFTime               `ffstruct:"ChartHistogram" json:"timestamp"`
	Types     []*ChartHistogramType `ffstruct:"ChartHistogram" json:"types"`
	IsCapped  bool                  `ffstruct:"ChartHistogram" json:"isCapped"`
}

type ChartHistogramType struct {
	Count string `ffstruct:"ChartHistogramType" json:"count"`
	Type  string `ffstruct:"ChartHistogramType" json:"type"`
}

type ChartHistogramInterval struct {
	StartTime *FFTime `json:"startTime"`
	EndTime   *FFTime `json:"endTime"`
}

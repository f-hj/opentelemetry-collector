// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package redisexporter

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configmodels"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const (
	// The value of "type" key in configuration.
	typeStr = "redis"
)

// NewFactory creates a factory for OTLP exporter.
func NewFactory() component.ExporterFactory {
	return exporterhelper.NewFactory(
		typeStr,
		createDefaultConfig,
		exporterhelper.WithTraces(createTraceExporter),
		exporterhelper.WithMetrics(createMetricsExporter),
		exporterhelper.WithLogs(createLogsExporter),
	)
}

func createDefaultConfig() configmodels.Exporter {
	return &Config{
		ExporterSettings: configmodels.ExporterSettings{
			TypeVal: typeStr,
			NameVal: typeStr,
		},
		Endpoint: "127.0.0.1:6789",
		Topic:    "otel-datas",
	}
}

func createTraceExporter(ctx context.Context, params component.ExporterCreateParams, config configmodels.Exporter) (component.TraceExporter, error) {
	oCfg := config.(*Config)
	return newTraceExporter(ctx, oCfg, params.Logger)
}

func createMetricsExporter(ctx context.Context, params component.ExporterCreateParams, config configmodels.Exporter) (component.MetricsExporter, error) {
	oCfg := config.(*Config)
	return newMetricsExporter(ctx, oCfg, params.Logger)
}

func createLogsExporter(ctx context.Context, params component.ExporterCreateParams, config configmodels.Exporter) (component.LogsExporter, error) {
	oCfg := config.(*Config)
	return newLogsExporter(ctx, oCfg, params.Logger)
}

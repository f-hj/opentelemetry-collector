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
	"errors"
	"fmt"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/internal"
)

type redisExporter struct {
	cfg *Config
	rdb *redis.Client
}

func newRedisExporter(ctx context.Context, cfg *Config) (*redisExporter, error) {
	if cfg.Endpoint == "" {
		return nil, errors.New("redis exporter cfg requires an endpoint")
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Endpoint,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	oce := &redisExporter{
		cfg: cfg,
		rdb: rdb,
	}
	return oce, nil
}

func (oce *redisExporter) shutdown(context.Context) error {
	return oce.rdb.Close()
}

func newTraceExporter(ctx context.Context, cfg *Config, logger *zap.Logger) (component.TraceExporter, error) {
	oce, err := newRedisExporter(ctx, cfg)
	if err != nil {
		return nil, err
	}

	return exporterhelper.NewTraceExporter(
		cfg,
		oce.pushTraceData,
		exporterhelper.WithShutdown(oce.shutdown),
	)
}

func newMetricsExporter(ctx context.Context, cfg *Config, logger *zap.Logger) (component.MetricsExporter, error) {
	oce, err := newRedisExporter(ctx, cfg)
	if err != nil {
		return nil, err
	}

	return exporterhelper.NewMetricsExporter(
		cfg,
		oce.pushMetricsData,
		exporterhelper.WithShutdown(oce.shutdown),
	)
}

func newLogsExporter(ctx context.Context, cfg *Config, logger *zap.Logger) (component.LogsExporter, error) {
	oce, err := newRedisExporter(ctx, cfg)
	if err != nil {
		return nil, err
	}

	return exporterhelper.NewLogsExporter(
		cfg,
		oce.pushLogsData,
		exporterhelper.WithShutdown(oce.shutdown),
	)
}

func (oce *redisExporter) pushTraceData(ctx context.Context, td pdata.Traces) (int, error) {
	spans := pdata.TracesToOtlp(td)
	for _, span := range spans {
		data, err := span.Marshal()
		if err != nil {
			return 0, err
		}
		if err := oce.rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: oce.cfg.Topic,
			Values: map[string]interface{}{"0": data},
		}).Err(); err != nil {
			fmt.Printf("redis stream xadd error:%v\n", err)
			return 0, err
		}
	}
	return 0, nil
}

func (oce *redisExporter) pushMetricsData(ctx context.Context, md pdata.Metrics) (int, error) {
	metrics := pdata.MetricsToOtlp(md)
	for _, metric := range metrics {
		data, err := metric.Marshal()
		if err != nil {
			return 0, err
		}
		if err := oce.rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: oce.cfg.Topic,
			Values: map[string]interface{}{"0": data},
		}).Err(); err != nil {
			fmt.Printf("redis stream xadd error:%v\n", err)
			return 0, err
		}
	}
	return 0, nil
}

func (oce *redisExporter) pushLogsData(ctx context.Context, ld pdata.Logs) (int, error) {
	logs := internal.LogsToOtlp(ld.InternalRep())
	for _, log := range logs {
		data, err := log.Marshal()
		if err != nil {
			return 0, err
		}

		if err := oce.rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: oce.cfg.Topic,
			Values: map[string]interface{}{"0": data},
		}).Err(); err != nil {
			fmt.Printf("redis stream xadd error:%v\n", err)
			return 0, err
		}
	}
	return 0, nil
}

package ocsql

import (
	"context"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

// The following tags are applied to stats recorded by this package.
var (
	// GoSqlMethod is the SQL method called.
	GoSqlMethod, _ = tag.NewKey("go_sql_method")
	// GoSqlError is the error received while calling a SQL method.
	GoSqlError, _ = tag.NewKey("go_sql_error")
	// GoSqlStatus identifies success vs. error from the SQL method response.
	GoSqlStatus, _ = tag.NewKey("go_sql_status")

	valueOK  = tag.Insert(GoSqlStatus, "OK")
	valueErr = tag.Insert(GoSqlStatus, "ERROR")
)

// The following measures are supported for use in custom views.
var (
	MeasureLatencyMs         = stats.Float64("go.sql/latency", "The latency of calls in milliseconds", stats.UnitMilliseconds)
	MeasureOpenConnections   = stats.Int64("go.sql/connections/open", "Count of open connections in the pool", stats.UnitDimensionless)
	MeasureIdleConnections   = stats.Int64("go.sql/connections/idle", "Count of idle connections in the pool", stats.UnitDimensionless)
	MeasureActiveConnections = stats.Int64("go.sql/connections/active", "Count of active connections in the pool", stats.UnitDimensionless)
	MeasureWaitCount         = stats.Int64("go.sql/connections/wait_count", "The total number of connections waited for", stats.UnitDimensionless)
	MeasureWaitDuration      = stats.Float64("go.sql/connections/wait_duration", "The total time blocked waiting for a new connection", stats.UnitMilliseconds)
	MeasureIdleClosed        = stats.Int64("go.sql/connections/idle_closed", "The total number of connections closed due to SetMaxIdleConns", stats.UnitDimensionless)
	MeasureLifetimeClosed    = stats.Int64("go.sql/connections/lifetime_closed", "The total number of connections closed due to SetConnMaxLifetime", stats.UnitDimensionless)
)

// Default distributions used by views in this package
var (
	DefaultMillisecondsDistribution = view.Distribution(
		0.0,
		0.001,
		0.005,
		0.01,
		0.05,
		0.1,
		0.5,
		1.0,
		1.5,
		2.0,
		2.5,
		5.0,
		10.0,
		25.0,
		50.0,
		100.0,
		200.0,
		400.0,
		600.0,
		800.0,
		1000.0,
		1500.0,
		2000.0,
		2500.0,
		5000.0,
		10000.0,
		20000.0,
		40000.0,
		100000.0,
		200000.0,
		500000.0)
)

// Package ocsql provides some convenience views.
// You still need to register these views for data to actually be collected.
// You can use the RegisterAllViews function for this.
var (
	SqlClientLatencyView = &view.View{
		Name:        "go.sql/client/latency",
		Description: "The distribution of latencies of various calls in milliseconds",
		Measure:     MeasureLatencyMs,
		Aggregation: DefaultMillisecondsDistribution,
		TagKeys:     []tag.Key{GoSqlMethod, GoSqlError, GoSqlStatus},
	}

	SqlClientCallsView = &view.View{
		Name:        "go.sql/client/calls",
		Description: "The number of various calls of methods",
		Measure:     MeasureLatencyMs,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{GoSqlMethod, GoSqlError, GoSqlStatus},
	}

	SqlClientOpenConnectionsView = &view.View{
		Name:        "go.sql/db/connections/open",
		Description: "The number of open connections",
		Measure:     MeasureOpenConnections,
		Aggregation: view.LastValue(),
	}

	SqlClientIdleConnectionsView = &view.View{
		Name:        "go.sql/db/connections/idle",
		Description: "The number of idle connections",
		Measure:     MeasureIdleConnections,
		Aggregation: view.LastValue(),
	}

	SqlClientActiveConnectionsView = &view.View{
		Name:        "go.sql/db/connections/active",
		Description: "The number of active connections",
		Measure:     MeasureActiveConnections,
		Aggregation: view.LastValue(),
	}

	SqlClientWaitCountView = &view.View{
		Name:        "go.sql/db/connections/wait_count",
		Description: "The total number of connections waited for",
		Measure:     MeasureWaitCount,
		Aggregation: view.LastValue(),
	}

	SqlClientWaitDurationView = &view.View{
		Name:        "go.sql/db/connections/wait_duration",
		Description: "The total time blocked waiting for a new connection",
		Measure:     MeasureWaitDuration,
		Aggregation: view.LastValue(),
	}

	SqlClientIdleClosedView = &view.View{
		Name:        "go.sql/db/connections/idle_closed_count",
		Description: "The total number of connections closed due to SetMaxIdleConns",
		Measure:     MeasureIdleClosed,
		Aggregation: view.LastValue(),
	}

	SqlClientLifetimeClosedView = &view.View{
		Name:        "go.sql/db/connections/lifetime_closed_count",
		Description: "The total number of connections closed due to SetConnMaxLifetime",
		Measure:     MeasureLifetimeClosed,
		Aggregation: view.LastValue(),
	}

	DefaultViews = []*view.View{
		SqlClientLatencyView, SqlClientCallsView, SqlClientOpenConnectionsView,
		SqlClientIdleConnectionsView, SqlClientActiveConnectionsView,
		SqlClientWaitCountView, SqlClientWaitDurationView,
		SqlClientIdleClosedView, SqlClientLifetimeClosedView,
	}
)

// RegisterAllViews registers all ocsql views to enable collection of stats.
func RegisterAllViews() {
	if err := view.Register(DefaultViews...); err != nil {
		panic(err)
	}
}

func recordCallStats(ctx context.Context, method string) func(err error) {
	var tags []tag.Mutator
	startTime := time.Now()

	return func(err error) {
		timeSpent := float64(time.Since(startTime).Nanoseconds()) / 1e6

		if err != nil {
			tags = []tag.Mutator{
				tag.Insert(GoSqlMethod, method), valueErr, tag.Insert(GoSqlError, err.Error()),
			}
		} else {
			tags = []tag.Mutator{
				tag.Insert(GoSqlMethod, method), valueOK,
			}
		}

		_ = stats.RecordWithTags(ctx, tags, MeasureLatencyMs.M(timeSpent))
	}
}

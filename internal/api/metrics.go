package api

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	metricMessagesPublished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "broker_messages_published_total",
		Help: "Total messages published, by topic.",
	}, []string{"topic"})

	metricPublishDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "broker_publish_duration_seconds",
		Help:    "Time to publish one message, by topic.",
		Buckets: prometheus.DefBuckets,
	}, []string{"topic"})

	metricWSSubscribers = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "broker_ws_subscribers_active",
		Help: "Active WebSocket subscribers, by topic.",
	}, []string{"topic"})

	metricStoreErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "broker_store_errors_total",
		Help: "Errors writing messages to the persistence store.",
	})
)

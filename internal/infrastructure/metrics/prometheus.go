// Package metrics owns every Prometheus collector, so inner layers never import Prometheus.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"example.com/scheduler/internal/domain"
)

const ns = "scheduler"

type Metrics struct {
	Registry *prometheus.Registry

	// HTTP layer
	HTTPRequests *prometheus.CounterVec
	HTTPLatency  *prometheus.HistogramVec
	HTTPInFlight prometheus.Gauge

	// Service layer
	ServiceCalls   *prometheus.CounterVec
	ServiceLatency *prometheus.HistogramVec

	// Worker
	jobs        *prometheus.CounterVec
	jobDuration *prometheus.HistogramVec
	jobLag      *prometheus.HistogramVec
	reaped      prometheus.Counter
	queue       *prometheus.GaugeVec
	localQueue  prometheus.Gauge
	isLeader    prometheus.Gauge
	terms       prometheus.Counter
	termJobs    prometheus.Histogram
	termTime    prometheus.Histogram
}

func New() *Metrics {
	exp := prometheus.ExponentialBuckets
	m := &Metrics{
		Registry: prometheus.NewRegistry(),

		HTTPRequests: counterVec("http_requests_total", "HTTP requests.", "method", "route", "status"),
		HTTPLatency:  histVec("http_request_duration_seconds", "HTTP latency.", prometheus.DefBuckets, "method", "route"),
		HTTPInFlight: gauge("http_in_flight_requests", "In-flight HTTP requests."),

		ServiceCalls:   counterVec("service_calls_total", "Service calls by outcome.", "method", "outcome"),
		ServiceLatency: histVec("service_call_duration_seconds", "Service call latency.", exp(0.0005, 2, 12), "method"),

		jobs:        counterVec("jobs_processed_total", "Jobs processed by outcome.", "type", "outcome"),
		jobDuration: histVec("job_duration_seconds", "Handler run time.", exp(0.001, 2, 16), "type"),
		jobLag:      histVec("job_schedule_lag_seconds", "Start time minus scheduled time.", exp(0.005, 2, 16), "type"),
		reaped:      counter("jobs_reaped_total", "Expired leases recovered."),
		queue:       gaugeVec("queue_jobs", "Jobs per state.", "state"),
		localQueue:  gauge("local_queue_jobs", "Jobs waiting in this node's channel."),
		isLeader:    gauge("is_leader", "1 while this node holds the leader lock."),
		terms:       counter("leader_terms_total", "Leader terms won by this node."),
		termJobs:    hist("leader_term_jobs", "Jobs fetched per leader term.", exp(1, 2, 12)),
		termTime:    hist("leader_term_duration_seconds", "Time the lock is held per term.", exp(0.001, 2, 14)),
	}
	m.Registry.MustRegister(
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.HTTPRequests, m.HTTPLatency, m.HTTPInFlight, m.ServiceCalls, m.ServiceLatency,
		m.jobs, m.jobDuration, m.jobLag, m.reaped, m.queue, m.localQueue,
		m.isLeader, m.terms, m.termJobs, m.termTime,
	)
	return m
}

// --- application.WorkerMetrics ---

func (m *Metrics) SetLeader(leading bool) {
	if leading {
		m.isLeader.Set(1)
		m.terms.Inc()
	} else {
		m.isLeader.Set(0)
	}
}

func (m *Metrics) TermFinished(jobs int, took time.Duration) {
	m.termJobs.Observe(float64(jobs))
	m.termTime.Observe(took.Seconds())
}

func (m *Metrics) LocalQueue(n int) { m.localQueue.Set(float64(n)) }

func (m *Metrics) JobFinished(jobType, outcome string, took time.Duration, lagMs int64) {
	m.jobs.WithLabelValues(jobType, outcome).Inc()
	m.jobDuration.WithLabelValues(jobType).Observe(took.Seconds())
	m.jobLag.WithLabelValues(jobType).Observe(float64(lagMs) / 1000)
}

func (m *Metrics) Reaped(n int) { m.reaped.Add(float64(n)) }

func (m *Metrics) QueueDepth(s domain.Stats) {
	m.queue.WithLabelValues("pending").Set(float64(s.Pending))
	m.queue.WithLabelValues("due").Set(float64(s.Due))
	m.queue.WithLabelValues("running").Set(float64(s.Running))
	m.queue.WithLabelValues("dead").Set(float64(s.Dead))
}

// --- constructors ---

func counter(name, help string) prometheus.Counter {
	return prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: name, Help: help})
}

func counterVec(name, help string, labels ...string) *prometheus.CounterVec {
	return prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: name, Help: help}, labels)
}

func gauge(name, help string) prometheus.Gauge {
	return prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: name, Help: help})
}

func gaugeVec(name, help string, labels ...string) *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: ns, Name: name, Help: help}, labels)
}

func hist(name, help string, buckets []float64) prometheus.Histogram {
	return prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: ns, Name: name, Help: help, Buckets: buckets})
}

func histVec(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
	return prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Name: name, Help: help, Buckets: buckets}, labels)
}

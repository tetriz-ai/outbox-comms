// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package server assembles and runs the whole service in one process: the gRPC
// API, the queue consumers (fan-out, dispatch, email feedback), the scheduler
// with its reconcile, prune and retry jobs, and an HTTP listener for /healthz and
// /metrics. cmd/outbox-comms calls Run; a program that registers its own
// usecases or gates calls it too.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"buf.build/go/protovalidate"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	awsses "github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/hibiken/asynq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/tetriz-ai/outbox-comms/config"
	"github.com/tetriz-ai/outbox-comms/gate"
	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/api"
	"github.com/tetriz-ai/outbox-comms/internal/attachments"
	"github.com/tetriz-ai/outbox-comms/internal/dao"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
	"github.com/tetriz-ai/outbox-comms/internal/dispatch"
	"github.com/tetriz-ai/outbox-comms/internal/fanout"
	"github.com/tetriz-ai/outbox-comms/internal/feedback"
	"github.com/tetriz-ai/outbox-comms/internal/jobs"
	"github.com/tetriz-ai/outbox-comms/internal/metrics"
	"github.com/tetriz-ai/outbox-comms/internal/migrations"
	"github.com/tetriz-ai/outbox-comms/internal/queue"
	"github.com/tetriz-ai/outbox-comms/internal/reconcile"
	"github.com/tetriz-ai/outbox-comms/internal/scheduler"
	"github.com/tetriz-ai/outbox-comms/internal/sender"
	"github.com/tetriz-ai/outbox-comms/internal/slackclient"
	"github.com/tetriz-ai/outbox-comms/templates"
)

// Options replaces the default collaborators. Every nil field keeps its default.
type Options struct {
	// GRPCServerOptions configures TLS credentials, authentication interceptors,
	// and other host policy. Request validation remains installed.
	GRPCServerOptions []grpc.ServerOption
	// OrgGate defaults to gate.AllowAll.
	OrgGate gate.OrgGate
	// CommsGate defaults to gate.AllowAll.
	CommsGate gate.CommsGate
	// SlackTokens defaults to the configured workspace and per-org tokens.
	// CheckSendEligibility asks the same provider: a token means connected,
	// ErrNoSlackToken means no_connection, and any other error is
	// connection_lookup_failed.
	SlackTokens TokenProvider
	// AuthFailure is called when a send fails on a revoked or missing Slack token.
	// The default logs it.
	AuthFailure AuthFailureReporter
	// Logger receives every log line the service writes. Nil means slog's
	// default logger, which `outbox-comms serve` sets to JSON on stderr. Unlike
	// the metrics registry, the default is kept: a host that set slog's default
	// gets the lines where it chose, and discarding them would hide failures.
	Logger *slog.Logger
	// Metrics is the registry the service's metrics are registered on and
	// /metrics serves. Nil means a registry of the service's own, with the Go
	// and process metrics added; Prometheus's global default registry is never
	// touched.
	Metrics MetricsRegistry
}

// MetricsRegistry is where the service registers its metrics and what /metrics
// serves. A *prometheus.Registry is one.
type MetricsRegistry interface {
	prometheus.Registerer
	prometheus.Gatherer
}

// TokenProvider returns an organization's Slack bot token. It returns
// ErrNoSlackToken when the organization has none, which fails the send as an
// auth failure instead of retrying it.
type TokenProvider = sender.TokenProvider

// ErrNoSlackToken is what a TokenProvider returns for an organization with no
// Slack token.
var ErrNoSlackToken = sender.ErrNoSlackToken

// AuthFailureReporter is told when a send fails on a revoked or missing Slack
// token, so the organization can be asked for a new one.
type AuthFailureReporter = dispatch.AuthFailureReporter

// LoadAWSConfig reads the AWS credentials and region through the AWS SDK's
// default chain. Run calls it only when SES, SQS or S3 is configured. Replace it
// to supply credentials another way.
var LoadAWSConfig = func(ctx context.Context) (aws.Config, error) { return awsconfig.LoadDefaultConfig(ctx) }

// logAuthFailures is the default AuthFailureReporter.
type logAuthFailures struct{ log *slog.Logger }

func (r logAuthFailures) ReportGrantInvalid(ctx context.Context, orgID, reason string) error {
	r.log.WarnContext(ctx, "slack token rejected; the org needs a new token", "org_id", orgID, "reason", reason)
	return nil
}

// OpenDB opens the Postgres handle the service and the admin commands share.
func OpenDB(databaseURL string) (*gorm.DB, error) {
	model.RegisterSerializers()
	db, err := gorm.Open(postgres.Open(databaseURL), &gorm.Config{Logger: gormlogger.Discard})
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	return db, nil
}

// Run starts every component and blocks until ctx is cancelled, then shuts
// them down. It returns the first start-up error, or nil after a clean stop.
func Run(ctx context.Context, cfg config.Config, opts Options) error {
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	reg := opts.Metrics
	if reg == nil {
		own := prometheus.NewRegistry()
		own.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
		reg = own
	}
	if err := metrics.Register(reg); err != nil {
		return err
	}
	metricsHandler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
	db, err := OpenDB(cfg.DatabaseURL)
	if err != nil {
		return err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("database handle: %w", err)
	}
	defer func() { _ = sqlDB.Close() }()
	if applied, err := migrations.Up(ctx, db); err != nil {
		return err
	} else if len(applied) > 0 {
		log.InfoContext(ctx, "applied migrations", "versions", applied)
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, DB: cfg.RedisDB, Password: cfg.RedisPass})
	defer func() { _ = rdb.Close() }()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}
	asynqClient := asynq.NewClientFromRedisClient(rdb)

	var awsCfg *aws.Config
	loadAWS := func() (aws.Config, error) {
		if awsCfg != nil {
			return *awsCfg, nil
		}
		c, err := LoadAWSConfig(ctx)
		if err != nil {
			return aws.Config{}, fmt.Errorf("load AWS config: %w", err)
		}
		awsCfg = &c
		return c, nil
	}

	var store attachments.Store
	switch cfg.Attachments {
	case "s3":
		ac, err := loadAWS()
		if err != nil {
			return err
		}
		store = attachments.S3{Client: s3.NewFromConfig(ac), Bucket: cfg.S3Bucket, Prefix: cfg.S3Prefix}
	default:
		store = attachments.Dir{Root: cfg.AttachmentsDir}
	}

	var pub queue.Publisher
	var sqsQueue *queue.SQS
	var sqsClient *sqs.Client
	if cfg.Queue == "sqs" || cfg.SQSFeedbackURL != "" {
		ac, err := loadAWS()
		if err != nil {
			return err
		}
		sqsClient = sqs.NewFromConfig(ac)
	}
	if cfg.Queue == "sqs" {
		sqsQueue = queue.NewSQS(sqsClient, cfg.SQSRequestsURL, cfg.SQSDispatchURL)
		pub = sqsQueue
	} else {
		pub = queue.NewAsynq(asynqClient)
	}

	senders := map[pb.Channel]dispatch.Sender{}
	vendors := fanout.StaticVendorResolver{Slack: pb.Vendor_VENDOR_SLACK}
	switch cfg.Email {
	case "smtp":
		senders[pb.Channel_CHANNEL_EMAIL] = sender.NewSMTPSender(sender.SMTPConfig{
			Addr: cfg.SMTPAddr, Username: cfg.SMTPUsername, Password: cfg.SMTPPassword, From: cfg.EmailFrom,
		}, store)
		vendors.Email = pb.Vendor_VENDOR_SMTP
	case "ses":
		ac, err := loadAWS()
		if err != nil {
			return err
		}
		senders[pb.Channel_CHANNEL_EMAIL] = sender.NewEmailSender(sesClient(ac, cfg), store, cfg.EmailFrom, cfg.SESConfigurationSet)
		vendors.Email = pb.Vendor_VENDOR_SES
	}
	tokens := opts.SlackTokens
	static := sender.StaticTokens{Default: cfg.SlackBotToken, PerOrg: cfg.SlackOrgTokens}
	if tokens == nil {
		tokens = static
	}
	slackClient, err := slackclient.New(nil, cfg.SlackAPIURL)
	if err != nil {
		return err
	}
	slackClient.SetLogger(log)
	slackSender := sender.NewSlackSender(tokens, slackClient, store)
	slackSender.SetLogger(log)
	senders[pb.Channel_CHANNEL_SLACK] = slackSender

	orgGate := opts.OrgGate
	if orgGate == nil {
		orgGate = gate.AllowAll{}
	}
	commsGate := opts.CommsGate
	if commsGate == nil {
		commsGate = gate.AllowAll{}
	}
	authFailure := opts.AuthFailure
	if authFailure == nil {
		authFailure = logAuthFailures{log}
	}

	requestDAO := dao.NewRequestRepo(db)
	messageDAO := dao.NewMessageRepo(db)
	eventDAO := dao.NewDeliveryEventRepo(db)
	suppressionDAO := dao.NewSuppressionRepo(db)
	preferenceDAO := dao.NewPreferenceRepo(db)
	destinationDAO := dao.NewDestinationRepo(db)
	reconcileDAO := dao.NewReconcileRepo(db)

	retrySched := jobs.NewRetryScheduler(asynqClient, queue.AsynqQueue)
	retrySched.Logger = log
	retry := dispatch.RetryPolicy{Ladder: cfg.RetryLadder, MaxAttempts: cfg.RetryMaxAttempts}
	dispatcher := dispatch.NewDispatcher(messageDAO, requestDAO, suppressionDAO, eventDAO, senders, retry, authFailure, retrySched, nil,
		EmailChrome(cfg))
	dispatcher.SetSendTimeout(cfg.SendTimeout)
	dispatcher.SetLogger(log)
	fan := fanout.NewFanOut(requestDAO, messageDAO, preferenceDAO, destinationDAO, orgGate, pub, vendors)
	fan.SetLogger(log)
	reconciler := reconcile.NewReconciler(reconcileDAO, pub, pub, retrySched, reconcile.Thresholds{
		StuckRequest: cfg.StuckRequestAfter, StuckSending: cfg.StuckSendingAfter,
		MissingConfirm: cfg.MissingConfirmAfter, AgedQueued: cfg.AgedQueuedAfter,
		PrunePayloadAfter: cfg.PrunePayloadAfter, OverdueAfter: cfg.OverdueAfter, TombstoneTTL: cfg.PurgeTombstoneTTL,
		ReputationWindow: cfg.ReputationWindow, ReputationMinSends: cfg.ReputationMinSends,
		Limit: cfg.ReconcileLimit, MaxAttempts: attemptCap(cfg.RetryMaxAttempts),
	}, nil)
	reconciler.SetLogger(log)
	handlers := queue.Handlers{Request: fan.Process, Dispatch: dispatcher.Dispatch}

	mux := asynq.NewServeMux()
	mux.Use(scheduler.RecoveryMiddleware(log), scheduler.LoggingMiddleware(log))
	jobs.NewHandler(reconciler, pub).Register(mux)
	if sqsQueue == nil {
		queue.RegisterAsynq(mux, handlers)
	}
	sched := scheduler.New(schedulerConfig(cfg, log), rdb, []scheduler.ScheduleSource{jobs.Source{Queue: queue.AsynqQueue, ReconcileCron: cfg.ReconcileCron, PruneCron: cfg.PruneCron}}, mux, scheduler.NewDistLockFactory(rdb))

	connChecker := sender.Connections{Tokens: tokens}
	svc := api.NewService(api.Deps{
		RequestDAO: requestDAO, MessageDAO: messageDAO, EventDAO: eventDAO,
		PreferenceDAO: preferenceDAO, DestinationDAO: destinationDAO, SuppressionDAO: suppressionDAO,
		OrgGate: orgGate, ConnChecker: connChecker, Publisher: pub, ReconcileDAO: reconcileDAO, Gate: commsGate,
		Logger: log,
	})
	validator, err := protovalidate.New()
	if err != nil {
		return fmt.Errorf("build request validator: %w", err)
	}
	serverOpts := append([]grpc.ServerOption{grpc.ChainUnaryInterceptor(validateUnary(validator))}, opts.GRPCServerOptions...)
	grpcServer := grpc.NewServer(serverOpts...)
	pb.RegisterCommsServiceServer(grpcServer, svc)
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthSrv)
	// Reflection lets grpcurl and similar tools call the API without the protos.
	if cfg.GRPCReflection {
		reflection.Register(grpcServer)
	}

	grpcLis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen gRPC %s: %w", cfg.GRPCAddr, err)
	}
	httpMux := http.NewServeMux()
	httpMux.Handle("/metrics", metricsHandler)
	httpMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		hctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := sqlDB.PingContext(hctx); err != nil {
			http.Error(w, "database: unreachable", http.StatusServiceUnavailable)
			return
		}
		if err := rdb.Ping(hctx).Err(); err != nil {
			http.Error(w, "redis: unreachable", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	})
	httpServer := &http.Server{Addr: cfg.HTTPAddr, Handler: httpMux, ReadHeaderTimeout: 5 * time.Second}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	if err := sched.Start(runCtx); err != nil {
		return err
	}
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := grpcServer.Serve(grpcLis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			errCh <- fmt.Errorf("gRPC server: %w", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("HTTP server: %w", err)
		}
	}()
	var pollers []*queue.Poller
	if sqsQueue != nil {
		pollers = append(pollers, sqsQueue.Pollers(handlers, cfg.QueueWorkers)...)
		for _, p := range pollers {
			p.Logger = log
		}
	}
	if cfg.SQSFeedbackURL != "" {
		fb := feedbackConsumer(cfg, messageDAO, eventDAO, suppressionDAO, requestDAO)
		fb.Logger = log
		pollers = append(pollers, &queue.Poller{Client: sqsClient, QueueURL: cfg.SQSFeedbackURL, Workers: cfg.QueueWorkers, Logger: log,
			Handle: func(ctx context.Context, m queue.Message) error { return fb.Handle(ctx, m.Body, m.ReceiveCount) }})
	}
	for _, p := range pollers {
		wg.Add(1)
		go func(p *queue.Poller) {
			defer wg.Done()
			p.Run(runCtx)
		}(p)
	}
	log.InfoContext(ctx, "outbox-comms started", "grpc", cfg.GRPCAddr, "http", cfg.HTTPAddr, "queue", cfg.Queue, "email", cfg.Email, "usecases", templates.Keys())

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-errCh:
	}
	log.InfoContext(ctx, "outbox-comms stopping")
	cancel()
	healthSrv.Shutdown()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), shutdownTimeout(cfg.SendTimeout)+5*time.Second)
	defer stopCancel()
	grpcServer.GracefulStop()
	_ = httpServer.Shutdown(stopCtx)
	_ = sched.Stop(stopCtx)
	_ = asynqClient.Close()
	wg.Wait()
	return runErr
}

// shutdownTimeout is how long a stop waits for tasks in flight: the send
// deadline plus 15 seconds for the claim, render and status writes around it.
// schedulerConfig is the queue worker's configuration. A task in flight at
// shutdown gets its whole send deadline and then some to finish, so a stop
// never cuts a send it could have completed.
func schedulerConfig(cfg config.Config, log *slog.Logger) scheduler.Config {
	return scheduler.Config{
		Queues:          map[string]int{queue.AsynqQueue: 1},
		Concurrency:     cfg.WorkerConcurrency,
		ShutdownTimeout: shutdownTimeout(cfg.SendTimeout),
		Logger:          log,
	}
}

func shutdownTimeout(sendTimeout time.Duration) time.Duration {
	if sendTimeout <= 0 {
		sendTimeout = dispatch.DefaultSendTimeout
	}
	return sendTimeout + 15*time.Second
}

// feedbackConsumer is the SES feedback consumer, with purge tombstones
// matching late feedback for OUTBOX_COMMS_PURGE_TOMBSTONE_TTL, the window the
// prune job keeps them.
func feedbackConsumer(cfg config.Config, messages dao.IMessageDAO, events dao.IDeliveryEventDAO, suppressions dao.ISuppressionDAO, requests dao.IRequestDAO) *feedback.Consumer {
	fb := feedback.NewConsumer(messages, events, suppressions)
	fb.TombstoneTTL = cfg.PurgeTombstoneTTL
	fb.Requests = requests
	return fb
}

// sesClient is the SES client the email sender uses: its HTTP timeout is the
// configured send deadline, so a call SES has not answered ends with the
// dispatcher's deadline, at whatever length that is set.
func sesClient(ac aws.Config, cfg config.Config) *awsses.Client {
	return sender.NewSESClientWithTimeout(ac, cfg.SendTimeout)
}

// validateUnary rejects a request that breaks its proto's buf.validate rules with
// INVALID_ARGUMENT before the handler runs.
func validateUnary(v protovalidate.Validator) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if m, ok := req.(proto.Message); ok {
			if err := v.Validate(m); err != nil {
				return nil, status.Error(codes.InvalidArgument, err.Error())
			}
		}
		return handler(ctx, req)
	}
}

// attemptCap is the reconcile's attempt cap: the configured maximum, and each
// registered usecase's own maximum where its contract overrides it, as the
// dispatcher applies them.
func attemptCap(maxAttempts int) dao.AttemptCap {
	c := dao.AttemptCap{Default: maxAttempts, ByUsecase: map[string]int{}}
	for _, k := range templates.Keys() {
		if ct, err := templates.Get(k); err == nil && ct.RetryOverride != nil && ct.RetryOverride.MaxAttempts > 0 {
			c.ByUsecase[k] = ct.RetryOverride.MaxAttempts
		}
	}
	return c
}

// EmailChrome is the brand chrome every email is wrapped in, read from cfg. The
// dispatcher and admin render both use it, so a preview matches the send.
func EmailChrome(cfg config.Config) templates.EmailChromeOptions {
	return templates.EmailChromeOptions{
		BrandName: cfg.BrandName, SiteURL: cfg.SiteURL, LogoURL: cfg.LogoURL,
		PreferencesURL: cfg.PreferencesURL, PrivacyURL: cfg.PrivacyURL, TermsURL: cfg.TermsURL,
	}
}

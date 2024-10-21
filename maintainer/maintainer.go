// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package maintainer

import (
	"encoding/json"
	"math"
	"sync"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/logservice/schemastore"
	"github.com/flowbehappy/tigate/maintainer/replica"
	"github.com/flowbehappy/tigate/maintainer/split"
	"github.com/flowbehappy/tigate/pkg/bootstrap"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	commonEvent "github.com/flowbehappy/tigate/pkg/common/event"
	"github.com/flowbehappy/tigate/pkg/config"
	"github.com/flowbehappy/tigate/pkg/filter"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/metrics"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/flowbehappy/tigate/server/watcher"
	"github.com/flowbehappy/tigate/utils"
	"github.com/flowbehappy/tigate/utils/dynstream"
	"github.com/flowbehappy/tigate/utils/threadpool"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	cdcConfig "github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// Maintainer is response for handle changefeed replication tasksMaintainer should:
// 1. schedules tables to dispatcher manager
// 2. calculate changefeed checkpoint ts
// 3. send changefeed status to coordinator
// 4. handle heartbeat reported by dispatcher
// there are four threads in maintainer:
// 1. controller thread , handled in dynstream, it handles the main logic of the maintainer, like barrier, heartbeat
// 2. scheduler thread, handled in threadpool, it schedules the tables to dispatcher manager
// 3. operator controller thread, handled in threadpool, it runs the operators
// 4. checker controller, handled in threadpool, it runs the checkers to dynamically adjust the schedule
// all threads are read/write information from/to the ReplicationDB
type Maintainer struct {
	id         model.ChangeFeedID
	config     *config.ChangeFeedInfo
	selfNode   *node.Info
	controller *Controller
	barrier    *Barrier

	stream        dynstream.DynamicStream[string, *Event, *Maintainer]
	taskScheduler threadpool.ThreadPool
	mc            messaging.MessageCenter

	watermark             *heartbeatpb.Watermark
	checkpointTsByCapture map[node.ID]heartbeatpb.Watermark

	state        heartbeatpb.ComponentState
	bootstrapper *bootstrap.Bootstrapper[heartbeatpb.MaintainerBootstrapResponse]

	changefeedSate model.FeedState

	removed *atomic.Bool

	initialized bool

	pdEndpoints []string
	nodeManager *watcher.NodeManager
	nodesClosed map[node.ID]struct{}

	statusChanged  *atomic.Bool
	nodeChanged    *atomic.Bool
	lastReportTime time.Time

	removing        bool
	cascadeRemoving bool

	lastPrintStatusTime  time.Time
	lastCheckpointTsTime time.Time

	errLock         sync.Mutex
	runningErrors   map[node.ID]*heartbeatpb.RunningError
	runningWarnings map[node.ID]*heartbeatpb.RunningError

	changefeedCheckpointTsGauge    prometheus.Gauge
	changefeedCheckpointTsLagGauge prometheus.Gauge
	changefeedResolvedTsGauge      prometheus.Gauge
	changefeedResolvedTsLagGauge   prometheus.Gauge
	changefeedStatusGauge          prometheus.Gauge
	scheduledTaskGauge             prometheus.Gauge
	runningTaskGauge               prometheus.Gauge
	tableCountGauge                prometheus.Gauge
	handleEventDuration            prometheus.Observer
}

// NewMaintainer create the maintainer for the changefeed
func NewMaintainer(cfID model.ChangeFeedID,
	conf *cdcConfig.SchedulerConfig,
	cfg *config.ChangeFeedInfo,
	selfNode *node.Info,
	stream dynstream.DynamicStream[string, *Event, *Maintainer],
	taskScheduler threadpool.ThreadPool,
	pdAPI pdutil.PDAPIClient,
	regionCache split.RegionCache,
	checkpointTs uint64,
) *Maintainer {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	m := &Maintainer{
		id:            cfID,
		selfNode:      selfNode,
		stream:        stream,
		taskScheduler: taskScheduler,
		controller: NewController(cfID.ID, checkpointTs, pdAPI, regionCache, taskScheduler,
			cfg.Config.Scheduler, conf.AddTableBatchSize, time.Duration(conf.CheckBalanceInterval)),
		mc:              mc,
		state:           heartbeatpb.ComponentState_Working,
		removed:         atomic.NewBool(false),
		nodeManager:     nodeManager,
		nodesClosed:     make(map[node.ID]struct{}),
		statusChanged:   atomic.NewBool(true),
		nodeChanged:     atomic.NewBool(false),
		cascadeRemoving: false,
		config:          cfg,
		watermark: &heartbeatpb.Watermark{
			CheckpointTs: checkpointTs,
			ResolvedTs:   checkpointTs,
		},
		checkpointTsByCapture: make(map[node.ID]heartbeatpb.Watermark),
		runningErrors:         map[node.ID]*heartbeatpb.RunningError{},
		runningWarnings:       map[node.ID]*heartbeatpb.RunningError{},

		changefeedCheckpointTsGauge:    metrics.ChangefeedCheckpointTsGauge.WithLabelValues(cfID.Namespace, cfID.ID),
		changefeedCheckpointTsLagGauge: metrics.ChangefeedCheckpointTsLagGauge.WithLabelValues(cfID.Namespace, cfID.ID),
		changefeedResolvedTsGauge:      metrics.ChangefeedResolvedTsGauge.WithLabelValues(cfID.Namespace, cfID.ID),
		changefeedResolvedTsLagGauge:   metrics.ChangefeedResolvedTsLagGauge.WithLabelValues(cfID.Namespace, cfID.ID),
		changefeedStatusGauge:          metrics.ChangefeedStatusGauge.WithLabelValues(cfID.Namespace, cfID.ID),
		scheduledTaskGauge:             metrics.ScheduleTaskGuage.WithLabelValues(cfID.Namespace, cfID.ID),
		runningTaskGauge:               metrics.RunningScheduleTaskGauge.WithLabelValues(cfID.Namespace, cfID.ID),
		tableCountGauge:                metrics.TableGauge.WithLabelValues(cfID.Namespace, cfID.ID),
		handleEventDuration:            metrics.MaintainerHandleEventDuration.WithLabelValues(cfID.Namespace, cfID.ID),
	}
	m.bootstrapper = bootstrap.NewBootstrapper[heartbeatpb.MaintainerBootstrapResponse](m.id.ID, m.getNewBootstrapFn())
	m.barrier = NewBarrier(m.controller, cfg.Config.Scheduler.EnableTableAcrossNodes)
	log.Info("maintainer is created", zap.String("id", cfID.String()))
	metrics.MaintainerGauge.WithLabelValues(cfID.Namespace, cfID.ID).Inc()
	return m
}

// HandleEvent implements the event-driven process mode
// it's the entrance of the Maintainer, it handles all types of Events
// note: the EventPeriod is a special event that submitted when initializing maintainer
// , and it will be re-submitted at the end of onPeriodTask
func (m *Maintainer) HandleEvent(event *Event) bool {
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		if duration > time.Second {
			log.Info("maintainer is too slow",
				zap.String("id", m.id.String()),
				zap.Int("type", event.eventType),
				zap.Duration("duration", duration))
		}
		m.handleEventDuration.Observe(duration.Seconds())
	}()
	if m.state == heartbeatpb.ComponentState_Stopped {
		log.Warn("maintainer is stopped, ignore",
			zap.String("changefeed", m.id.String()))
		return false
	}
	// first check the online/offline nodes
	if m.nodeChanged.Load() {
		m.onNodeChanged()
		m.nodeChanged.Store(false)
	}
	switch event.eventType {
	case EventInit:
		return m.onInit()
	case EventMessage:
		m.onMessage(event.message)
	case EventPeriod:
		m.onPeriodTask()
	}
	return false
}

// Close cleanup resources
func (m *Maintainer) Close() {
	m.cleanupMetrics()
	m.controller.Stop()
	log.Info("changefeed maintainer closed",
		zap.String("id", m.id.String()),
		zap.Bool("removed", m.removed.Load()),
		zap.Uint64("checkpointTs", m.watermark.CheckpointTs))
}

func (m *Maintainer) GetMaintainerStatus() *heartbeatpb.MaintainerStatus {
	// todo: fix data race here
	m.errLock.Lock()
	defer m.errLock.Unlock()
	var runningErrors []*heartbeatpb.RunningError
	if len(m.runningErrors) > 0 {
		runningErrors = make([]*heartbeatpb.RunningError, 0, len(m.runningErrors))
		for _, e := range m.runningErrors {
			runningErrors = append(runningErrors, e)
		}
		clear(m.runningErrors)
	}
	var runningWarnings []*heartbeatpb.RunningError
	if len(m.runningWarnings) > 0 {
		runningWarnings = make([]*heartbeatpb.RunningError, 0, len(m.runningWarnings))
		for _, e := range m.runningWarnings {
			runningWarnings = append(runningWarnings, e)
		}
		clear(m.runningWarnings)
	}

	status := &heartbeatpb.MaintainerStatus{
		ChangefeedID: m.id.ID,
		FeedState:    string(m.changefeedSate),
		State:        m.state,
		CheckpointTs: m.watermark.CheckpointTs,
		Warning:      runningWarnings,
		Err:          runningErrors,
	}
	return status
}

func (m *Maintainer) initialize() error {
	start := time.Now()
	log.Info("start to initialize changefeed maintainer",
		zap.String("id", m.id.String()))
	tables, err := m.initTables()
	if err != nil {
		return errors.Trace(err)
	}
	m.controller.SetInitialTables(tables)

	log.Info("changefeed maintainer initialized",
		zap.String("id", m.id.String()),
		zap.Duration("duration", time.Since(start)))
	m.initialized = true
	m.state = heartbeatpb.ComponentState_Working
	m.statusChanged.Store(true)

	// detect the capture changes
	m.nodeManager.RegisterNodeChangeHandler(node.ID("maintainer-"+m.id.ID), func(allNodes map[node.ID]*node.Info) {
		m.nodeChanged.Store(true)
	})
	// init bootstrapper nodes
	nodes := m.nodeManager.GetAliveNodes()
	log.Info("changefeed bootstrap initial nodes",
		zap.Int("nodes", len(nodes)))
	var newNodes = make([]*node.Info, 0, len(nodes))
	for _, n := range nodes {
		newNodes = append(newNodes, n)
	}
	m.sendMessages(m.bootstrapper.HandleNewNodes(newNodes))
	// setup period event
	SubmitScheduledEvent(m.taskScheduler, m.stream, &Event{
		changefeedID: m.id.ID,
		eventType:    EventPeriod,
	}, time.Now().Add(time.Millisecond*500))
	return nil
}

func (m *Maintainer) cleanupMetrics() {
	metrics.ChangefeedCheckpointTsGauge.DeleteLabelValues(m.id.Namespace, m.id.ID)
	metrics.ChangefeedCheckpointTsLagGauge.DeleteLabelValues(m.id.Namespace, m.id.ID)
	metrics.ChangefeedStatusGauge.DeleteLabelValues(m.id.Namespace, m.id.ID)
	metrics.ScheduleTaskGuage.DeleteLabelValues(m.id.Namespace, m.id.ID)
	metrics.RunningScheduleTaskGauge.DeleteLabelValues(m.id.Namespace, m.id.ID)
	metrics.TableGauge.DeleteLabelValues(m.id.Namespace, m.id.ID)
	metrics.MaintainerHandleEventDuration.DeleteLabelValues(m.id.Namespace, m.id.ID)
}

func (m *Maintainer) onInit() bool {
	// already initialized
	if m.initialized {
		return false
	}
	// async initialize the changefeed
	go func() {
		err := m.initialize()
		if err != nil {
			m.handleError(err)
		}
		m.stream.Wake() <- m.id.ID
		log.Info("stream waked", zap.String("changefeed", m.id.String()))
	}()
	return true
}

func (m *Maintainer) onMessage(msg *messaging.TargetMessage) {
	switch msg.Type {
	case messaging.TypeHeartBeatRequest:
		m.onHeartBeatRequest(msg)
	case messaging.TypeBlockStatusRequest:
		m.onBlockStateRequest(msg)
	case messaging.TypeMaintainerBootstrapResponse:
		m.onMaintainerBootstrapResponse(msg)
	case messaging.TypeMaintainerCloseResponse:
		m.onNodeClosed(msg.From, msg.Message[0].(*heartbeatpb.MaintainerCloseResponse))
	case messaging.TypeRemoveMaintainerRequest:
		m.onRemoveMaintainer(msg.Message[0].(*heartbeatpb.RemoveMaintainerRequest).Cascade)
	case messaging.TypeCheckpointTsMessage:
		m.onCheckpointTsPersisted(msg.Message[0].(*heartbeatpb.CheckpointTsMessage))
	default:
		log.Panic("unexpected message type",
			zap.String("changefeed", m.id.ID),
			zap.String("type", msg.Type.String()))
	}
}

func (m *Maintainer) onRemoveMaintainer(cascade bool) {
	m.removing = true
	m.cascadeRemoving = cascade
	closed := m.tryCloseChangefeed()
	if closed {
		m.removed.Store(true)
		m.state = heartbeatpb.ComponentState_Stopped
		metrics.MaintainerGauge.WithLabelValues(m.id.Namespace, m.id.ID).Dec()
	}
}

func (m *Maintainer) onCheckpointTsPersisted(msg *heartbeatpb.CheckpointTsMessage) {
	stm := m.controller.GetTask(m.controller.ddlDispatcherID)
	if stm == nil {
		log.Warn("ddl dispatcher is not found, can not send checkpoint message",
			zap.String("id", m.id.String()))
		return
	}
	m.sendMessages([]*messaging.TargetMessage{
		messaging.NewSingleTargetMessage(stm.GetNodeID(), messaging.HeartbeatCollectorTopic, msg),
	})
}

func (m *Maintainer) onNodeChanged() {
	currentNodes := m.bootstrapper.GetAllNodes()

	activeNodes := m.nodeManager.GetAliveNodes()
	var newNodes = make([]*node.Info, 0, len(activeNodes))
	for id, n := range activeNodes {
		if _, ok := currentNodes[id]; !ok {
			newNodes = append(newNodes, n)
		}
	}
	var removedNodes []node.ID
	for id, _ := range currentNodes {
		if _, ok := activeNodes[id]; !ok {
			removedNodes = append(removedNodes, id)
			m.controller.RemoveNode(id)
		}
	}
	log.Info("maintainer node changed",
		zap.String("id", m.id.String()),
		zap.Int("new", len(newNodes)),
		zap.Int("removed", len(removedNodes)))
	m.sendMessages(m.bootstrapper.HandleNewNodes(newNodes))
	cachedResponse := m.bootstrapper.HandleRemoveNodes(removedNodes)
	if cachedResponse != nil {
		log.Info("bootstrap done after removed some nodes",
			zap.String("id", m.id.String()))
		m.onBootstrapDone(cachedResponse)
	}
}

func (m *Maintainer) calCheckpointTs() {
	m.updateMetrics()
	// make sure there is no task running
	// the dispatcher changing come from:
	// 1. node change
	// 2. ddl
	// 3. interval scheduling, like balance, split
	if time.Since(m.lastCheckpointTsTime) < 2*time.Second ||
		!m.controller.ScheduleFinished() {
		return
	}
	m.lastCheckpointTsTime = time.Now()

	newWatermark := heartbeatpb.NewMaxWatermark()
	for id, _ := range m.bootstrapper.GetAllNodes() {
		if m.controller.GetTaskSizeByNodeID(id) > 0 {
			if _, ok := m.checkpointTsByCapture[id]; !ok {
				log.Debug("checkpointTs can not be advanced, since missing capture heartbeat",
					zap.String("changefeed", m.id.ID),
					zap.Any("node", id))
				return
			}
			newWatermark.UpdateMin(m.checkpointTsByCapture[id])
		}
	}
	if newWatermark.CheckpointTs != math.MaxUint64 {
		m.watermark.CheckpointTs = newWatermark.CheckpointTs
	}
	if newWatermark.ResolvedTs != math.MaxUint64 {
		m.watermark.ResolvedTs = newWatermark.ResolvedTs
	}
}

func (m *Maintainer) updateMetrics() {
	phyCkpTs := oracle.ExtractPhysical(m.watermark.CheckpointTs)
	m.changefeedCheckpointTsGauge.Set(float64(phyCkpTs))
	lag := (oracle.GetPhysical(time.Now()) - phyCkpTs) / 1e3
	m.changefeedCheckpointTsLagGauge.Set(float64(lag))

	phyResolvedTs := oracle.ExtractPhysical(m.watermark.ResolvedTs)
	m.changefeedResolvedTsGauge.Set(float64(phyResolvedTs))
	lag = (oracle.GetPhysical(time.Now()) - phyResolvedTs) / 1e3
	m.changefeedResolvedTsLagGauge.Set(float64(lag))

	m.changefeedStatusGauge.Set(float64(m.state))
}

// send message to remote
func (m *Maintainer) sendMessages(msgs []*messaging.TargetMessage) {
	for _, msg := range msgs {
		err := m.mc.SendCommand(msg)
		if err != nil {
			log.Debug("failed to send maintainer request",
				zap.String("changefeed", m.id.ID),
				zap.Any("msg", msg), zap.Error(err))
			continue
		}
	}
}

func (m *Maintainer) onHeartBeatRequest(msg *messaging.TargetMessage) {
	req := msg.Message[0].(*heartbeatpb.HeartBeatRequest)
	if req.Watermark != nil {
		m.checkpointTsByCapture[msg.From] = *req.Watermark
	}
	m.controller.HandleStatus(msg.From, req.Statuses)
	if req.Warning != nil {
		m.errLock.Lock()
		m.runningWarnings[msg.From] = req.Warning
		m.errLock.Unlock()
	}
	if req.Err != nil {
		m.errLock.Unlock()
		m.runningErrors[msg.From] = req.Err
		m.errLock.Unlock()
	}
}

func (m *Maintainer) onBlockStateRequest(msg *messaging.TargetMessage) {
	req := msg.Message[0].(*heartbeatpb.BlockStatusRequest)
	ackMsg := m.barrier.HandleStatus(msg.From, req)
	m.sendMessages([]*messaging.TargetMessage{ackMsg})
}

func (m *Maintainer) onMaintainerBootstrapResponse(msg *messaging.TargetMessage) {
	log.Info("received maintainer bootstrap response",
		zap.String("changefeed", m.id.ID),
		zap.Any("server", msg.From))
	cachedResp := m.bootstrapper.HandleBootstrapResponse(msg.From, msg.Message[0].(*heartbeatpb.MaintainerBootstrapResponse))
	m.onBootstrapDone(cachedResp)
}

func (m *Maintainer) onBootstrapDone(cachedResp map[node.ID]*heartbeatpb.MaintainerBootstrapResponse) {
	if cachedResp == nil {
		return
	}
	log.Info("all nodes have sent bootstrap response",
		zap.String("changefeed", m.id.ID),
		zap.Int("size", len(cachedResp)))
	workingMap := make(map[int64]utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication])
	for server, bootstrapMsg := range cachedResp {
		log.Info("received bootstrap response",
			zap.String("changefeed", m.id.ID),
			zap.Any("server", server),
			zap.Int("size", len(bootstrapMsg.Spans)))
		for _, info := range bootstrapMsg.Spans {
			dispatcherID := common.NewDispatcherIDFromPB(info.ID)
			status := &heartbeatpb.TableSpanStatus{
				ComponentStatus: info.ComponentStatus,
				ID:              info.ID,
				CheckpointTs:    info.CheckpointTs,
			}
			span := info.Span

			//working on remote, the state must be absent or working since it's reported by remote
			stm := replica.NewWorkingReplicaSet(m.id, dispatcherID, info.SchemaID, span, status, server)
			tableMap, ok := workingMap[span.TableID]
			if !ok {
				tableMap = utils.NewBtreeMap[*heartbeatpb.TableSpan, *replica.SpanReplication](heartbeatpb.LessTableSpan)
				workingMap[span.TableID] = tableMap
			}
			tableMap.ReplaceOrInsert(span, stm)
		}
	}
	m.controller.FinishBootstrap(workingMap)
}

// initTableIDs get tables ids base on the filter and checkpoint ts
func (m *Maintainer) initTables() ([]commonEvent.Table, error) {
	startTs := m.watermark.CheckpointTs
	f, err := filter.NewFilter(m.config.Config.Filter, "", m.config.Config.ForceReplicate)
	if err != nil {
		return nil, errors.Cause(err)
	}

	schemaStore := appcontext.GetService[schemastore.SchemaStore](appcontext.SchemaStore)
	tables, err := schemaStore.GetAllPhysicalTables(startTs, f)
	log.Info("get table ids", zap.Int("count", len(tables)), zap.String("changefeed", m.id.String()))
	return tables, nil
}

func (m *Maintainer) onNodeClosed(from node.ID, response *heartbeatpb.MaintainerCloseResponse) {
	if response.Success {
		m.nodesClosed[from] = struct{}{}
	}
	// check if all nodes have sent response
	m.onRemoveMaintainer(m.cascadeRemoving)
}

func (m *Maintainer) handleResendMessage() {
	// resend bootstrap message
	m.sendMessages(m.bootstrapper.ResendBootstrapMessage())
	// resend closing message
	if m.removing {
		m.sendMaintainerCloseRequestToAllNode()
	}
	// resend barrier ack messages
	m.sendMessages(m.barrier.Resend())
}

func (m *Maintainer) tryCloseChangefeed() bool {
	if m.state != heartbeatpb.ComponentState_Stopped {
		m.statusChanged.Store(true)
	}
	if !m.cascadeRemoving {
		return true
	}
	return m.sendMaintainerCloseRequestToAllNode()
}

func (m *Maintainer) sendMaintainerCloseRequestToAllNode() bool {
	msgs := make([]*messaging.TargetMessage, 0)
	for n := range m.nodeManager.GetAliveNodes() {
		if _, ok := m.nodesClosed[n]; !ok {
			msgs = append(msgs, messaging.NewSingleTargetMessage(
				n,
				messaging.DispatcherManagerManagerTopic,
				&heartbeatpb.MaintainerCloseRequest{
					ChangefeedID: m.id.ID,
				}))
		}
	}
	m.sendMessages(msgs)
	return len(msgs) == 0
}

// handleError set the caches the error, the error will be reported to coordinator
// and coordinator remove this maintainer
// todo: stop maintainer immediately?
func (m *Maintainer) handleError(err error) {
	log.Error("an error occurred in Owner",
		zap.String("changefeed", m.id.ID), zap.Error(err))
	var code string
	if rfcCode, ok := errors.RFCCode(err); ok {
		code = string(rfcCode)
	} else {
		code = string(errors.ErrOwnerUnknown.RFCCode())
	}
	m.runningErrors = map[node.ID]*heartbeatpb.RunningError{
		m.selfNode.ID: {
			Time:    time.Now().String(),
			Node:    m.selfNode.AdvertiseAddr,
			Code:    code,
			Message: err.Error(),
		},
	}
	m.statusChanged.Store(true)
}

// getNewBootstrapFn returns a function that creates a new bootstrap message to initialize
// a changefeed dispatcher manager.
func (m *Maintainer) getNewBootstrapFn() scheduler.NewBootstrapFn {
	cfg := m.config
	changefeedConfig := config.ChangefeedConfig{
		Namespace:          cfg.Namespace,
		ID:                 cfg.ID,
		StartTS:            cfg.StartTs,
		TargetTS:           cfg.TargetTs,
		SinkURI:            cfg.SinkURI,
		ForceReplicate:     cfg.Config.ForceReplicate,
		SinkConfig:         cfg.Config.Sink,
		Filter:             cfg.Config.Filter,
		EnableSyncPoint:    *cfg.Config.EnableSyncPoint,
		SyncPointInterval:  cfg.Config.SyncPointInterval,
		SyncPointRetention: cfg.Config.SyncPointRetention,
		// other fileds are not necessary for maintainer
	}
	// cfgBytes only holds necessary fields to initialize a changefeed dispatcher.
	cfgBytes, err := json.Marshal(changefeedConfig)
	if err != nil {
		log.Panic("marshal changefeed config failed",
			zap.String("changefeed", m.id.ID),
			zap.Error(err))
	}
	return func(id node.ID) *messaging.TargetMessage {
		log.Info("send maintainer bootstrap message",
			zap.String("changefeed", m.id.String()),
			zap.Any("server", id))
		return messaging.NewSingleTargetMessage(
			id,
			messaging.DispatcherManagerManagerTopic,
			&heartbeatpb.MaintainerBootstrapRequest{
				ChangefeedID: m.id.ID,
				Config:       cfgBytes,
			})
	}
}

func (m *Maintainer) onPeriodTask() {
	// send scheduling messages
	m.handleResendMessage()
	m.collectMetrics()
	m.calCheckpointTs()
	SubmitScheduledEvent(m.taskScheduler, m.stream, &Event{
		changefeedID: m.id.ID,
		eventType:    EventPeriod,
	}, time.Now().Add(time.Millisecond*500))
}

func (m *Maintainer) collectMetrics() {
	if time.Since(m.lastPrintStatusTime) > time.Second*20 {
		total := m.controller.TaskSize()
		scheduling := m.controller.replicationDB.GetSchedulingSize()
		working := m.controller.replicationDB.GetReplicatingSize()
		absent := m.controller.replicationDB.GetAbsentSize()

		m.tableCountGauge.Set(float64(total))
		m.scheduledTaskGauge.Set(float64(scheduling))
		metrics.TableStateGauge.WithLabelValues(m.id.Namespace, m.id.ID, "Absent").Set(float64(absent))
		metrics.TableStateGauge.WithLabelValues(m.id.Namespace, m.id.ID, "Working").Set(float64(working))
		m.lastPrintStatusTime = time.Now()
		log.Info("maintainer status",
			zap.String("changefeed", m.id.ID),
			zap.Int("total", total),
			zap.Int("scheduling", scheduling),
			zap.Int("working", working))
	}
}

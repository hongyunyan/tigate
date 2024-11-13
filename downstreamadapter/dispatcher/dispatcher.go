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

package dispatcher

import (
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	tisink "github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/downstreamadapter/sink/types"
	"github.com/pingcap/ticdc/downstreamadapter/syncpoint"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"github.com/pingcap/tiflow/pkg/spanz"
	"go.uber.org/zap"
)

/*
Dispatcher is responsible for getting events from LogService and sending them to Sink in appropriate order.
Each dispatcher only deal with the events of one tableSpan in one changefeed.
Here is a special dispatcher will deal with the events of the DDLSpan in one changefeed, we call it TableTriggerEventDispatcher
Each EventDispatcherManager will have multiple dispatchers.

All dispatchers in the changefeed of the same node will share the same Sink.
All dispatchers will communicate with the Maintainer about self progress and whether can push down the blocked ddl event.

Because Sink does not flush events to the downstream in strict order.
the dispatcher can't send event to Sink continuously all the time,
1. The ddl event/sync point event can be send to Sink only when the previous event has beed flushed to downstream successfully.
2. Only when the ddl event/sync point event is flushed to downstream successfully, the dispatcher can send the following event to Sink.
3. For the cross table ddl event/sync point event, dispatcher needs to negotiate with the maintainer to decide whether and when send it to Sink.

The workflow related to the dispatcher is as follows:

	+--------------+       +----------------+       +------------+       +--------+       +--------+       +------------+
	| EventService |  -->  | EventCollector |  -->  | Dispatcher |  -->  |  Sink  |  -->  | Worker |  -->  | Downstream |
	+--------------+       +----------------+       +------------+       +--------+       +--------+       +------------+
	                                                        |
										  HeartBeatResponse | HeartBeatRequest
															|
	                                              +--------------------+
	                                              | HeartBeatCollector |
												  +--------------------+
												            |
															|
												      +------------+
	                                                  | Maintainer |
												      +------------+
*/

type Dispatcher struct {
	changefeedID common.ChangeFeedID
	id           common.DispatcherID
	// tableInfo is the latest table info of the dispatcher
	tableInfo atomic.Pointer[common.TableInfo]
	tableSpan *heartbeatpb.TableSpan
	sink      tisink.Sink
	// startTs is the start timestamp of the dispatcher
	startTs uint64

	// blockStatusesChan use to report block status of ddl/sync point event to Maintainer
	blockStatusesChan chan *heartbeatpb.TableSpanBlockStatus

	SyncPointInfo   *syncpoint.SyncPointInfo
	lastSyncPointTs atomic.Uint64

	componentStatus *ComponentStateWithMutex

	filterConfig *config.FilterConfig
	filter       filter.Filter

	resolvedTs *TsWithMutex // The largest commitTs - 1 of the events received by the dispatcher.

	blockStatus BlockStatus

	isRemoving atomic.Bool

	tableProgress *types.TableProgress

	resendTaskMap *ResendTaskMap

	schemaIDToDispatchers *SchemaIDToDispatchers
	schemaID              int64

	// only exist when the dispatcher is a table trigger event dispatcher
	tableSchemaStore *util.TableSchemaStore

	errCh chan error
}

func NewDispatcher(
	changefeedID common.ChangeFeedID,
	id common.DispatcherID,
	tableSpan *heartbeatpb.TableSpan,
	sink tisink.Sink,
	startTs uint64,
	blockStatusesChan chan *heartbeatpb.TableSpanBlockStatus,
	filter filter.Filter,
	schemaID int64,
	schemaIDToDispatchers *SchemaIDToDispatchers,
	syncPointInfo *syncpoint.SyncPointInfo,
	filterConfig *config.FilterConfig,
	errCh chan error) *Dispatcher {
	dispatcher := &Dispatcher{
		changefeedID:          changefeedID,
		id:                    id,
		tableSpan:             tableSpan,
		sink:                  sink,
		startTs:               startTs,
		blockStatusesChan:     blockStatusesChan,
		SyncPointInfo:         syncPointInfo,
		componentStatus:       newComponentStateWithMutex(heartbeatpb.ComponentState_Working),
		resolvedTs:            newTsWithMutex(startTs),
		filterConfig:          filterConfig,
		filter:                filter,
		isRemoving:            atomic.Bool{},
		blockStatus:           BlockStatus{blockPendingEvent: nil},
		tableProgress:         types.NewTableProgress(),
		schemaID:              schemaID,
		schemaIDToDispatchers: schemaIDToDispatchers,
		resendTaskMap:         newResendTaskMap(),
		errCh:                 errCh,
	}
	dispatcher.lastSyncPointTs.Store(syncPointInfo.InitSyncPointTs)

	// only when is not mysql sink, table trigger event dispatcher need tableSchemaStore to store the table name
	// in order to calculate all the topics when sending checkpointTs to downstream
	if tableSpan.Equal(heartbeatpb.DDLSpan) {
		dispatcher.tableSchemaStore = util.NewTableSchemaStore()
		dispatcher.sink.SetTableSchemaStore(dispatcher.tableSchemaStore)
	}

	dispatcher.addToStatusDynamicStream()

	return dispatcher
}

// Each dispatcher status may contain an ACK info or a dispatcher action or both.
// If we get an ack info, we need to check whether the ack is for the current pending ddl event. If so, we can cancel the resend task.
// If we get a dispatcher action, we need to check whether the action is for the current pending ddl event. If so, we can deal the ddl event based on the action.
// 1. If the action is a write, we need to add the ddl event to the sink for writing to downstream(async).
// 2. If the action is a pass, we just need to pass the event in tableProgress(for correct calculation) and wake the dispatcherEventHandler
func (d *Dispatcher) HandleDispatcherStatus(dispatcherStatus *heartbeatpb.DispatcherStatus) {
	// deal with the ack info
	ack := dispatcherStatus.GetAck()
	if ack != nil {
		identifier := BlockEventIdentifier{
			CommitTs:    ack.CommitTs,
			IsSyncPoint: ack.IsSyncPoint,
		}
		d.cancelResendTask(identifier)
	}

	// deal with the dispatcher action
	action := dispatcherStatus.GetAction()
	if action != nil {
		pendingEvent, blockStatus := d.blockStatus.getEventAndStage()
		if pendingEvent != nil && action.CommitTs == pendingEvent.GetCommitTs() && blockStatus == heartbeatpb.BlockStage_WAITING {
			d.blockStatus.updateBlockStage(heartbeatpb.BlockStage_WRITING)
			if action.Action == heartbeatpb.Action_Write {
				err := d.sink.WriteBlockEvent(pendingEvent, d.tableProgress)
				if err != nil {
					d.errCh <- err
					return
				}
			} else {
				d.sink.PassBlockEvent(pendingEvent, d.tableProgress)
			}
		}

		// whether the outdate message or not, we need to return message show we have finished the event.
		d.blockStatusesChan <- &heartbeatpb.TableSpanBlockStatus{
			ID: d.id.ToPB(),
			State: &heartbeatpb.State{
				IsBlocked:   true,
				BlockTs:     dispatcherStatus.GetAction().CommitTs,
				IsSyncPoint: dispatcherStatus.GetAction().IsSyncPoint,
				Stage:       heartbeatpb.BlockStage_DONE,
			},
		}

		d.blockStatus.clear()
	}
}

// HandleEvents can batch handle events about resolvedTs Event and DML Event.
// While for DDLEvent and SyncPointEvent, they should be handled singly,
// because they are block events.
// We ensure we only will receive one event when it's ddl event or sync point event
// by setting them with different event types in DispatcherEventsHandler.GetType
// When we handle events, we don't have any previous events still in sink.
func (d *Dispatcher) HandleEvents(dispatcherEvents []DispatcherEvent, wakeCallback func()) (block bool) {
	// Only return false when all events are resolvedTs Event.
	block = false
	// Dispatcher is ready, handle the events
	for _, dispatcherEvent := range dispatcherEvents {
		event := dispatcherEvent.Event
		// Pre-check, make sure the event is not stale
		if event.GetCommitTs() < d.resolvedTs.Get() {
			log.Info("Received a stale event, should ignore it",
				zap.Any("commitTs", event.GetCommitTs()),
				zap.Any("seq", event.GetSeq()),
				zap.Any("eventType", event.GetType()),
				zap.Any("dispatcher", d.id),
				zap.Any("event", event))
			continue
		}

		switch event.GetType() {
		case commonEvent.TypeResolvedEvent:
			d.resolvedTs.Set(event.(commonEvent.ResolvedEvent).ResolvedTs)
		case commonEvent.TypeDMLEvent:
			block = true
			dml := event.(*commonEvent.DMLEvent)
			dml.AssembleRows(d.tableInfo.Load())
			dml.AddPostFlushFunc(func() {
				// Considering dml event in sink may be write to downstream not in order,
				// thus, we use tableProgress.Empty() to ensure these events are flushed to downstream completely
				// and wake dynamic stream to handle the next events.
				if d.tableProgress.Empty() {
					wakeCallback()
				}
			})
			d.sink.AddDMLEvent(dml, d.tableProgress)
		case commonEvent.TypeDDLEvent:
			if len(dispatcherEvents) != 1 {
				log.Panic("ddl event should only be singly handled", zap.Any("dispatcherID", d.id))
			}
			block = true
			event := event.(*commonEvent.DDLEvent)
			// Update the table info of the dispatcher, when it receive ddl event.
			d.tableInfo.Store(event.TableInfo)
			log.Info("dispatcher receive ddl event",
				zap.Stringer("dispatcher", d.id),
				zap.String("query", event.Query),
				zap.Int64("table", event.TableID),
				zap.Uint64("commitTs", event.GetCommitTs()),
				zap.Uint64("seq", event.GetSeq()))
			if d.tableSchemaStore != nil {
				d.tableSchemaStore.AddEvent(event)
			}
			event.AddPostFlushFunc(func() {
				wakeCallback()
			})
			d.dealWithBlockEvent(event)
		case commonEvent.TypeSyncPointEvent:
			if len(dispatcherEvents) != 1 {
				log.Panic("sync point event should only be singly handled", zap.Any("dispatcherID", d.id))
			}
			block = true
			event := event.(*commonEvent.SyncPointEvent)
			event.AddPostFlushFunc(func() {
				wakeCallback()
			})
			d.dealWithBlockEvent(event)
			d.lastSyncPointTs.Store(event.GetCommitTs())
		case commonEvent.TypeHandshakeEvent:
			log.Warn("Receive handshake event unexpectedly", zap.Any("event", event), zap.Stringer("dispatcher", d.id))
		default:
			log.Panic("Unexpected event type", zap.Any("event Type", event.GetType()), zap.Stringer("dispatcher", d.id), zap.Uint64("commitTs", event.GetCommitTs()))
		}
	}
	return block
}

func (d *Dispatcher) SetInitialTableInfo(tableInfo *common.TableInfo) {
	d.tableInfo.Store(tableInfo)
}

func isCompleteSpan(tableSpan *heartbeatpb.TableSpan) bool {
	spanz.TableIDToComparableSpan(tableSpan.TableID)
	startKey, endKey := spanz.GetTableRange(tableSpan.TableID)
	if spanz.StartCompare(spanz.ToComparableKey(startKey), tableSpan.StartKey) == 0 && spanz.EndCompare(spanz.ToComparableKey(endKey), tableSpan.EndKey) == 0 {
		return true
	}
	return false
}

func (d *Dispatcher) shouldBlock(event commonEvent.BlockEvent) bool {
	switch event.GetType() {
	case commonEvent.TypeDDLEvent:
		ddlEvent := event.(*commonEvent.DDLEvent)
		if ddlEvent.BlockedTables != nil {
			switch ddlEvent.GetBlockedTables().InfluenceType {
			case commonEvent.InfluenceTypeNormal:
				if len(ddlEvent.GetBlockedTables().TableIDs) > 1 {
					return true
				} else if !isCompleteSpan(d.tableSpan) {
					// if the table is split, even the blockTable only itself, it should block
					return true
				}
				return false
			case commonEvent.InfluenceTypeDB, commonEvent.InfluenceTypeAll:
				return true
			}
		}
		return false
	case commonEvent.TypeSyncPointEvent:
		return true
	default:
		log.Error("invalid event type", zap.Any("event Type", event.GetType()))
	}
	return false
}

// 1.If the event is a single table DDL, it will be added to the sink for writing to downstream(async).
// If the ddl leads to add new tables or drop tables, it should send heartbeat to maintainer
// 2. If the event is a multi-table DDL / sync point Event, it will generate a TableSpanBlockStatus message with ddl info to send to maintainer.
func (d *Dispatcher) dealWithBlockEvent(event commonEvent.BlockEvent) {
	if !d.shouldBlock(event) {
		err := d.sink.WriteBlockEvent(event, d.tableProgress)
		if err != nil {
			d.errCh <- err
			return
		}
		if event.GetNeedAddedTables() != nil || event.GetNeedDroppedTables() != nil {
			message := &heartbeatpb.TableSpanBlockStatus{
				ID: d.id.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked:         false,
					BlockTs:           event.GetCommitTs(),
					NeedDroppedTables: event.GetNeedDroppedTables().ToPB(),
					NeedAddedTables:   commonEvent.ToTablesPB(event.GetNeedAddedTables()),
					IsSyncPoint:       false, // sync point event must should block
					Stage:             heartbeatpb.BlockStage_NONE,
				},
			}
			identifier := BlockEventIdentifier{
				CommitTs:    event.GetCommitTs(),
				IsSyncPoint: false,
			}
			d.resendTaskMap.Set(identifier, newResendTask(message, d))
			d.blockStatusesChan <- message
		}
	} else {
		d.blockStatus.setBlockEvent(event, heartbeatpb.BlockStage_WAITING)
		message := &heartbeatpb.TableSpanBlockStatus{
			ID: d.id.ToPB(),
			State: &heartbeatpb.State{
				IsBlocked:         true,
				BlockTs:           event.GetCommitTs(),
				BlockTables:       event.GetBlockedTables().ToPB(),
				NeedDroppedTables: event.GetNeedDroppedTables().ToPB(),
				NeedAddedTables:   commonEvent.ToTablesPB(event.GetNeedAddedTables()),
				UpdatedSchemas:    commonEvent.ToSchemaIDChangePB(event.GetUpdatedSchemas()), // only exists for rename table and rename tables
				IsSyncPoint:       event.GetType() == commonEvent.TypeSyncPointEvent,         // sync point event must should block
				Stage:             heartbeatpb.BlockStage_WAITING,
			},
		}
		identifier := BlockEventIdentifier{
			CommitTs:    event.GetCommitTs(),
			IsSyncPoint: event.GetType() == commonEvent.TypeSyncPointEvent,
		}
		d.resendTaskMap.Set(identifier, newResendTask(message, d))
		d.blockStatusesChan <- message
	}

	// dealing with events which update schema ids
	// Only rename table and rename tables may update schema ids(rename db1.table1 to db2.table2)
	// Here we directly update schema id of dispatcher when we begin to handle the ddl event,
	// but not waiting maintainer response for ready to write/pass the ddl event.
	// Because the schemaID of each dispatcher is only use to dealing with the db-level ddl event(like drop db) or drop table.
	// Both the rename table/rename tables, drop table and db-level ddl event will be send to the table trigger event dispatcher in order.
	// So there won't be a related db-level ddl event is in dealing when we get update schema id events.
	// Thus, whether to update schema id before or after current ddl event is not important.
	// To make it easier, we choose to directly update schema id here.
	if event.GetUpdatedSchemas() != nil && d.tableSpan != heartbeatpb.DDLSpan {
		for _, schemaIDChange := range event.GetUpdatedSchemas() {
			if schemaIDChange.TableID == d.tableSpan.TableID {
				if schemaIDChange.OldSchemaID != d.schemaID {
					log.Error("Wrong Schema ID", zap.Any("dispatcherID", d.id), zap.Any("except schemaID", schemaIDChange.OldSchemaID), zap.Any("actual schemaID", d.schemaID), zap.Any("tableSpan", d.tableSpan.String()))
					return
				} else {
					d.schemaID = schemaIDChange.NewSchemaID
					d.schemaIDToDispatchers.Update(schemaIDChange.OldSchemaID, schemaIDChange.NewSchemaID)
					return
				}
			}
		}
	}
}

func (d *Dispatcher) GetTableSpan() *heartbeatpb.TableSpan {
	return d.tableSpan
}

func (d *Dispatcher) GetStartTs() uint64 {
	return d.startTs
}

func (d *Dispatcher) GetResolvedTs() uint64 {
	return d.resolvedTs.Get()
}

func (d *Dispatcher) GetCheckpointTs() uint64 {
	checkpointTs, isEmpty := d.tableProgress.GetCheckpointTs()
	if checkpointTs == 0 {
		// This means the dispatcher has never send events to the sink,
		// so we use resolvedTs as checkpointTs
		return d.GetResolvedTs()
	}

	if isEmpty {
		return max(checkpointTs, d.GetResolvedTs())
	}
	return checkpointTs
}

func (d *Dispatcher) GetId() common.DispatcherID {
	return d.id
}

func (d *Dispatcher) GetChangefeedID() common.ChangeFeedID {
	return d.changefeedID
}

func (d *Dispatcher) cancelResendTask(identifier BlockEventIdentifier) {
	task := d.resendTaskMap.Get(identifier)
	if task == nil {
		return
	}

	task.Cancel()
	d.resendTaskMap.Delete(identifier)

}

func (d *Dispatcher) GetSchemaID() int64 {
	return d.schemaID
}

func (d *Dispatcher) EnableSyncPoint() bool {
	return d.SyncPointInfo.EnableSyncPoint
}

func (d *Dispatcher) GetSyncPointTs() uint64 {
	if d.SyncPointInfo.EnableSyncPoint {
		return d.lastSyncPointTs.Load()
	} else {
		return 0
	}
}

func (d *Dispatcher) GetFilterConfig() *eventpb.FilterConfig {
	return toFilterConfigPB(d.filterConfig)
}

func toFilterConfigPB(filter *config.FilterConfig) *eventpb.FilterConfig {
	filterConfig := &eventpb.FilterConfig{
		Rules:            filter.Rules,
		IgnoreTxnStartTs: filter.IgnoreTxnStartTs,
		EventFilters:     make([]*eventpb.EventFilterRule, len(filter.EventFilters)),
	}

	for _, eventFilter := range filter.EventFilters {
		ignoreEvent := make([]string, len(eventFilter.IgnoreEvent))
		for _, event := range eventFilter.IgnoreEvent {
			ignoreEvent = append(ignoreEvent, string(event))
		}
		filterConfig.EventFilters = append(filterConfig.EventFilters, &eventpb.EventFilterRule{
			Matcher:                  eventFilter.Matcher,
			IgnoreEvent:              ignoreEvent,
			IgnoreSql:                eventFilter.IgnoreSQL,
			IgnoreInsertValueExpr:    eventFilter.IgnoreInsertValueExpr,
			IgnoreUpdateNewValueExpr: eventFilter.IgnoreUpdateNewValueExpr,
			IgnoreUpdateOldValueExpr: eventFilter.IgnoreUpdateOldValueExpr,
			IgnoreDeleteValueExpr:    eventFilter.IgnoreDeleteValueExpr,
		})
	}

	return filterConfig
}

func (d *Dispatcher) GetSyncPointInterval() time.Duration {
	if d.SyncPointInfo.EnableSyncPoint {
		return d.SyncPointInfo.SyncPointConfig.SyncPointInterval
	} else {
		return time.Duration(0)
	}
}

func (d *Dispatcher) Remove() {
	log.Info("table event dispatcher component status changed to stopping", zap.String("table", d.tableSpan.String()))
	d.isRemoving.Store(true)

	dispatcherStatusDynamicStream := GetDispatcherStatusDynamicStream()
	err := dispatcherStatusDynamicStream.RemovePath(d.id)
	if err != nil {
		log.Error("remove dispatcher from dynamic stream failed", zap.Error(err))
	}
}

// addToDynamicStream add self to dynamic stream
func (d *Dispatcher) addToStatusDynamicStream() {
	dispatcherStatusDynamicStream := GetDispatcherStatusDynamicStream()
	err := dispatcherStatusDynamicStream.AddPath(d.id, d)
	if err != nil {
		log.Error("add dispatcher to dynamic stream failed", zap.Error(err))
	}
}

func (d *Dispatcher) TryClose() (w heartbeatpb.Watermark, ok bool) {
	// If sink is normal(not meet error), we need to wait all the events in sink to flushed downstream successfully.
	// If sink is not normal, we can close the dispatcher immediately.
	if (d.sink.IsNormal() && d.tableProgress.Empty()) || !d.sink.IsNormal() {
		w.CheckpointTs = d.GetCheckpointTs()
		w.ResolvedTs = d.GetResolvedTs()

		d.componentStatus.Set(heartbeatpb.ComponentState_Stopped)
		return w, true
	}
	return w, false
}

func (d *Dispatcher) GetComponentStatus() heartbeatpb.ComponentState {
	return d.componentStatus.Get()
}

func (d *Dispatcher) GetRemovingStatus() bool {
	return d.isRemoving.Load()
}

func (d *Dispatcher) GetBlockStatus() *heartbeatpb.State {
	pendingEvent, blockStage := d.blockStatus.getEventAndStage()

	// we only need to report the block status for the ddl that block others and not finished.
	if pendingEvent == nil || !d.shouldBlock(pendingEvent) {
		return nil
	}

	// we only need to report the block status of these block ddls when maintainer is restarted.
	// For the non-block but with needDroppedTables and needAddTables ddls,
	// we don't need to report it when maintainer is restarted, because:
	// 1. the ddl not block other dispatchers
	// 2. maintainer can get current available tables based on table trigger event dispatcher's startTs,
	//    so don't need to do extra add and drop actions.

	return &heartbeatpb.State{
		IsBlocked:         true,
		BlockTs:           pendingEvent.GetCommitTs(),
		BlockTables:       pendingEvent.GetBlockedTables().ToPB(),
		NeedDroppedTables: pendingEvent.GetNeedDroppedTables().ToPB(),
		NeedAddedTables:   commonEvent.ToTablesPB(pendingEvent.GetNeedAddedTables()),
		UpdatedSchemas:    commonEvent.ToSchemaIDChangePB(pendingEvent.GetUpdatedSchemas()), // only exists for rename table and rename tables
		IsSyncPoint:       pendingEvent.GetType() == commonEvent.TypeSyncPointEvent,         // sync point event must should block
		Stage:             blockStage,
	}
}

func (d *Dispatcher) GetHeartBeatInfo(h *HeartBeatInfo) {
	h.Watermark.CheckpointTs = d.GetCheckpointTs()
	h.Watermark.ResolvedTs = d.GetResolvedTs()
	h.EventSizePerSecond = d.tableProgress.GetEventSizePerSecond()
	h.Id = d.GetId()
	h.ComponentStatus = d.GetComponentStatus()
	h.TableSpan = d.GetTableSpan()
	h.IsRemoving = d.GetRemovingStatus()
}

func (d *Dispatcher) HandleCheckpointTs(checkpointTs uint64) {
	d.sink.AddCheckpointTs(checkpointTs)
}

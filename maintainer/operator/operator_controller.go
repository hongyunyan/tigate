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

package operator

import (
	"container/heap"
	"math/rand"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/scheduler/operator"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

var _ operator.Controller[common.DispatcherID, *heartbeatpb.TableSpanStatus] = &Controller{}

// Controller is the operator controller, it manages all operators.
// And the Controller is responsible for the execution of the operator.
type Controller struct {
	role          string
	changefeedID  common.ChangeFeedID
	batchSize     int
	messageCenter messaging.MessageCenter
	replicationDB *replica.ReplicationDB
	nodeManager   *watcher.NodeManager

	lock         sync.RWMutex // protect the following fields
	operators    map[common.DispatcherID]*operator.OperatorWithTime[common.DispatcherID, *heartbeatpb.TableSpanStatus]
	runningQueue operator.OperatorQueue[common.DispatcherID, *heartbeatpb.TableSpanStatus]
}

func NewOperatorController(
	changefeedID common.ChangeFeedID, mc messaging.MessageCenter,
	db *replica.ReplicationDB, nodeManager *watcher.NodeManager,
	batchSize int,
) *Controller {
	oc := &Controller{
		role:          "maintainer",
		changefeedID:  changefeedID,
		operators:     make(map[common.DispatcherID]*operator.OperatorWithTime[common.DispatcherID, *heartbeatpb.TableSpanStatus]),
		runningQueue:  make(operator.OperatorQueue[common.DispatcherID, *heartbeatpb.TableSpanStatus], 0),
		messageCenter: mc,
		batchSize:     batchSize,
		replicationDB: db,
		nodeManager:   nodeManager,
	}
	return oc
}

// Execute periodically execute the operator
// todo: use a better way to control the execution frequency
func (oc *Controller) Execute() time.Time {
	executedItem := 0
	for {
		r, next := oc.pollQueueingOperator()
		if !next {
			return time.Now().Add(time.Millisecond * 200)
		}
		if r == nil {
			continue
		}

		msg := r.Schedule()

		if msg != nil {
			_ = oc.messageCenter.SendCommand(msg)
			log.Info("send command to dispatcher",
				zap.String("role", oc.role),
				zap.String("changefeed", oc.changefeedID.Name()),
				zap.String("operator", r.String()))
		}
		executedItem++
		if executedItem >= oc.batchSize {
			return time.Now().Add(time.Millisecond * 50)
		}
	}
}

// RemoveAllTasks remove all tasks, and notify all operators to stop.
// it is only called by the barrier when the changefeed is stopped.
func (oc *Controller) RemoveAllTasks() {
	oc.lock.Lock()
	defer oc.lock.Unlock()

	for _, replicaSet := range oc.replicationDB.TryRemoveAll() {
		oc.removeReplicaSet(NewRemoveDispatcherOperator(oc.replicationDB, replicaSet))
	}
}

// RemoveTasksBySchemaID remove all tasks by schema id.
// it is only by the barrier when the schema is dropped by ddl
func (oc *Controller) RemoveTasksBySchemaID(schemaID int64) {
	oc.lock.Lock()
	defer oc.lock.Unlock()
	for _, replicaSet := range oc.replicationDB.TryRemoveBySchemaID(schemaID) {
		oc.removeReplicaSet(NewRemoveDispatcherOperator(oc.replicationDB, replicaSet))
	}
}

// RemoveTasksByTableIDs remove all tasks by table ids.
// it is only called by the barrier when the table is dropped by ddl
func (oc *Controller) RemoveTasksByTableIDs(tables ...int64) {
	oc.lock.Lock()
	defer oc.lock.Unlock()
	for _, replicaSet := range oc.replicationDB.TryRemoveByTableIDs(tables...) {
		oc.removeReplicaSet(NewRemoveDispatcherOperator(oc.replicationDB, replicaSet))
	}
}

// AddOperator adds an operator to the controller, if the operator already exists, return false.
func (oc *Controller) AddOperator(op operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus]) bool {
	oc.lock.Lock()
	defer oc.lock.Unlock()

	if _, ok := oc.operators[op.ID()]; ok {
		log.Info("add operator failed, operator already exists",
			zap.String("role", oc.role),
			zap.String("changefeed", oc.changefeedID.Name()),
			zap.String("operator", op.String()))
		return false
	}
	span := oc.replicationDB.GetTaskByID(op.ID())
	if span == nil {
		log.Warn("add operator failed, span not found",
			zap.String("role", oc.role),
			zap.String("changefeed", oc.changefeedID.Name()),
			zap.String("operator", op.String()))
		return false
	}
	oc.pushOperator(op)
	return true
}

func (oc *Controller) UpdateOperatorStatus(id common.DispatcherID, from node.ID, status *heartbeatpb.TableSpanStatus) {
	oc.lock.RLock()
	defer oc.lock.RUnlock()

	op, ok := oc.operators[id]
	if ok {
		op.OP.Check(from, status)
	}
}

// OnNodeRemoved is called when a node is offline,
// the controller will mark all spans on the node as absent if no operator is handling it,
// then the controller will notify all operators.
func (oc *Controller) OnNodeRemoved(n node.ID) {
	oc.lock.RLock()
	defer oc.lock.RUnlock()

	for _, span := range oc.replicationDB.GetTaskByNodeID(n) {
		_, ok := oc.operators[span.ID]
		if !ok {
			oc.replicationDB.MarkSpanAbsent(span)
		}
	}
	for _, op := range oc.operators {
		op.OP.OnNodeRemove(n)
	}
}

// GetOperator returns the operator by id.
func (oc *Controller) GetOperator(id common.DispatcherID) operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus] {
	oc.lock.RLock()
	defer oc.lock.RUnlock()

	if op, ok := oc.operators[id]; !ok {
		return nil
	} else {
		return op.OP
	}
}

// OperatorSize returns the number of operators in the controller.
func (oc *Controller) OperatorSizeWithLock() int {
	return len(oc.operators)
}

// pollQueueingOperator returns the operator need to be executed,
// "next" is true to indicate that it may exist in next attempt,
// and false is the end for the poll.
func (oc *Controller) pollQueueingOperator() (operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus], bool) {
	oc.lock.Lock()
	defer oc.lock.Unlock()
	if oc.runningQueue.Len() == 0 {
		return nil, false
	}
	item := heap.Pop(&oc.runningQueue).(*operator.OperatorWithTime[common.DispatcherID, *heartbeatpb.TableSpanStatus])
	if item.Removed {
		return nil, true
	}
	op := item.OP
	opID := op.ID()
	// always call the PostFinish method to ensure the operator is cleaned up by itself.
	if op.IsFinished() {
		op.PostFinish()
		item.Removed = true
		delete(oc.operators, opID)
		metrics.FinishedOperatorCount.WithLabelValues(model.DefaultNamespace, oc.changefeedID.Name(), op.Type()).Inc()
		metrics.OperatorDuration.WithLabelValues(model.DefaultNamespace, oc.changefeedID.Name(), op.Type()).Observe(time.Since(item.EnqueueTime).Seconds())
		log.Info("operator finished",
			zap.String("role", oc.role),
			zap.String("changefeed", oc.changefeedID.Name()),
			zap.String("operator", opID.String()),
			zap.String("operator", op.String()))
		return nil, true
	}
	now := time.Now()
	if now.Before(item.Time) {
		heap.Push(&oc.runningQueue, item)
		return nil, false
	}
	// pushes with new notify time.
	item.Time = time.Now().Add(time.Millisecond * 500)
	heap.Push(&oc.runningQueue, item)
	return op, true
}

// ReplicaSetRemoved if the replica set is removed,
// the controller will remove the operator. add a new operator to the controller.
func (oc *Controller) removeReplicaSet(op *RemoveDispatcherOperator) {
	if old, ok := oc.operators[op.ID()]; ok {
		log.Info("replica set is removed , replace the old one",
			zap.String("role", oc.role),
			zap.String("changefeed", oc.changefeedID.Name()),
			zap.String("replicaset", old.OP.ID().String()),
			zap.String("operator", old.OP.String()))
		old.OP.OnTaskRemoved()
		old.OP.PostFinish()
		old.Removed = true
		delete(oc.operators, op.ID())
	}
	oc.pushOperator(op)
}

// pushOperator add an operator to the controller queue.
func (oc *Controller) pushOperator(op operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus]) {
	oc.checkAffectedNodes(op)
	log.Info("add operator to running queue",
		zap.String("role", oc.role),
		zap.String("changefeed", oc.changefeedID.Name()),
		zap.String("operator", op.String()))
	withTime := operator.NewOperatorWithTime(op, time.Now())
	oc.operators[op.ID()] = withTime
	op.Start()
	heap.Push(&oc.runningQueue, withTime)
	metrics.CreatedOperatorCount.WithLabelValues(model.DefaultNamespace, oc.changefeedID.Name(), op.Type()).Inc()
}

func (oc *Controller) checkAffectedNodes(op operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus]) {
	aliveNodes := oc.nodeManager.GetAliveNodes()
	for _, nodeID := range op.AffectedNodes() {
		if _, ok := aliveNodes[nodeID]; !ok {
			op.OnNodeRemove(nodeID)
		}
	}
}

func (oc *Controller) NewAddOperator(replicaSet *replica.SpanReplication, id node.ID) operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus] {
	return &AddDispatcherOperator{
		replicaSet: replicaSet,
		dest:       id,
		db:         oc.replicationDB,
	}
}

func (oc *Controller) NewMoveOperator(replicaSet *replica.SpanReplication, origin, dest node.ID) operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus] {
	return &MoveDispatcherOperator{
		replicaSet: replicaSet,
		origin:     origin,
		dest:       dest,
		db:         oc.replicationDB,
	}
}

func (oc *Controller) NewRemoveOperator(replicaSet *replica.SpanReplication) operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus] {
	return &RemoveDispatcherOperator{
		replicaSet: replicaSet,
		db:         oc.replicationDB,
	}
}

func (oc *Controller) NewSplitOperator(
	replicaSet *replica.SpanReplication, originNode node.ID, splitSpans []*heartbeatpb.TableSpan,
) operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus] {
	return NewSplitDispatcherOperator(oc.replicationDB, replicaSet, originNode, splitSpans)
}

// AddMergeSplitOperator adds a merge split operator to the controller.
//  1. Merge Operator: len(affectedReplicaSets) > 1, len(splitSpans) == 1
//  2. Split Operator: len(affectedReplicaSets) == 1, len(splitSpans) > 1
//  3. MergeAndSplit Operator: len(affectedReplicaSets) > 1, len(splitSpans) > 1
func (oc *Controller) AddMergeSplitOperator(
	affectedReplicaSets []*replica.SpanReplication,
	splitSpans []*heartbeatpb.TableSpan,
) bool {
	oc.lock.Lock()
	defer oc.lock.Unlock()
	// TODO: check if there are some intersection between `ret.Replications` and `spans`.
	// Ignore the intersection spans to prevent meaningless split operation.
	for _, replicaSet := range affectedReplicaSets {
		if _, ok := oc.operators[replicaSet.ID]; ok {
			log.Info("add operator failed, operator already exists",
				zap.String("role", oc.role),
				zap.String("changefeed", oc.changefeedID.Name()),
				zap.String("dispatcherID", replicaSet.ID.String()),
			)
			return false
		}
		span := oc.replicationDB.GetTaskByID(replicaSet.ID)
		if span == nil {
			log.Warn("add operator failed, span not found",
				zap.String("role", oc.role),
				zap.String("changefeed", oc.changefeedID.Name()),
				zap.String("dispatcherID", replicaSet.ID.String()))
			return false
		}
	}
	randomIdx := rand.Intn(len(affectedReplicaSets))
	primaryID := affectedReplicaSets[randomIdx].ID
	primaryOp := NewMergeSplitDispatcherOperator(oc.replicationDB, primaryID, affectedReplicaSets[randomIdx], affectedReplicaSets, splitSpans, nil)
	for _, replicaSet := range affectedReplicaSets {
		var op *MergeSplitDispatcherOperator
		if replicaSet.ID == primaryID {
			op = primaryOp
		} else {
			op = NewMergeSplitDispatcherOperator(oc.replicationDB, primaryID, replicaSet, nil, nil, primaryOp.onFinished)
		}
		oc.pushOperator(op)
	}
	log.Info("add merge split operator",
		zap.String("role", oc.role),
		zap.String("changefeed", oc.changefeedID.Name()),
		zap.String("primary", primaryID.String()),
		zap.Int64("tableID", splitSpans[0].TableID),
		zap.Int("oldSpans", len(affectedReplicaSets)),
		zap.Int("newSpans", len(splitSpans)),
	)
	return true
}

func (oc *Controller) GetLock() *sync.RWMutex {
	return &oc.lock
}

func (oc *Controller) ReleaseLock(mutex *sync.RWMutex) {
	mutex.Unlock()
}

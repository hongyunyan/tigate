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

package schemastore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/filter"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// The parent folder to store schema data
const dataDir = "schema_store"

// persistentStorage stores the following kinds of data on disk:
//  1. table info and database info from upstream snapshot
//  2. incremental ddl jobs
//  3. metadata which describes the valid data range on disk
type persistentStorage struct {
	gcRunning atomic.Bool

	pdCli pd.Client

	kvStorage kv.Storage

	db *pebble.DB

	mu sync.RWMutex

	// the current gcTs on disk
	gcTs uint64

	upperBound upperBoundMeta

	upperBoundChanged bool

	tableMap map[int64]*BasicTableInfo

	// schemaID -> database info
	// it contains all databases and deleted databases
	// will only be removed when its delete version is smaller than gc ts
	databaseMap map[int64]*BasicDatabaseInfo

	// table id -> a sorted list of finished ts for the table's ddl events
	tablesDDLHistory map[int64][]uint64

	// it has two use cases:
	// 1. store the ddl events need to send to a table dispatcher
	//    Note: some ddl events in the history may never be send,
	//          for example the create table ddl, truncate table ddl(usually the first event)
	// 2. build table info store for a table
	tableTriggerDDLHistory []uint64

	// tableID -> versioned store
	// it just contains tables which is used by dispatchers
	tableInfoStoreMap map[int64]*versionedTableInfoStore

	// tableID -> total registered count
	tableRegisteredCount map[int64]int
}

type upperBoundMeta struct {
	FinishedDDLTs uint64 `json:"finished_ddl_ts"`
	SchemaVersion int64  `json:"schema_version"`
	ResolvedTs    uint64 `json:"resolved_ts"`
}

func newPersistentStorage(
	ctx context.Context,
	root string,
	pdCli pd.Client,
	storage kv.Storage,
) *persistentStorage {
	gcSafePoint, err := pdCli.UpdateServiceGCSafePoint(ctx, "cdc-new-store", 0, 0)
	if err != nil {
		log.Panic("get ts failed", zap.Error(err))
	}

	dbPath := fmt.Sprintf("%s/%s", root, dataDir)
	// FIXME: avoid remove
	if err := os.RemoveAll(dbPath); err != nil {
		log.Panic("fail to remove path")
	}

	// TODO: update pebble options
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		log.Fatal("open db failed", zap.Error(err))
	}

	// check whether the data on disk is reusable
	isDataReusable := true
	gcTs, err := readGcTs(db)
	// TODO: distiguish non-exist key with other io errors
	if err != nil {
		isDataReusable = false
	}
	if gcSafePoint < gcTs {
		log.Panic("gc safe point should never go back")
	}
	upperBound, err := readUpperBoundMeta(db)
	if err != nil {
		isDataReusable = false
	}
	if gcSafePoint >= upperBound.ResolvedTs {
		isDataReusable = false
	}

	// initialize persistent storage
	dataStorage := &persistentStorage{
		pdCli:                  pdCli,
		kvStorage:              storage,
		db:                     db,
		gcTs:                   gcTs,
		upperBound:             upperBound,
		tableMap:               make(map[int64]*BasicTableInfo),
		databaseMap:            make(map[int64]*BasicDatabaseInfo),
		tablesDDLHistory:       make(map[int64][]uint64),
		tableTriggerDDLHistory: make([]uint64, 0),
		tableInfoStoreMap:      make(map[int64]*versionedTableInfoStore),
		tableRegisteredCount:   make(map[int64]int),
	}
	if isDataReusable {
		dataStorage.initializeFromDisk()
	} else {
		db.Close()
		dataStorage.db = nil
		dataStorage.initializeFromKVStorage(dbPath, storage, gcSafePoint)
	}

	go func() {
		dataStorage.gc(ctx)
	}()

	go func() {
		dataStorage.persistUpperBoundPeriodically(ctx)
	}()

	return dataStorage
}

func (p *persistentStorage) initializeFromKVStorage(dbPath string, storage kv.Storage, gcTs uint64) {
	// TODO: avoid recreate db if the path is empty at start
	if err := os.RemoveAll(dbPath); err != nil {
		log.Panic("fail to remove path")
	}

	var err error
	// TODO: update pebble options
	if p.db, err = pebble.Open(dbPath, &pebble.Options{}); err != nil {
		log.Fatal("open db failed", zap.Error(err))
	}
	log.Info("schema store initialize from kv storage begin",
		zap.Uint64("snapTs", gcTs))

	if p.databaseMap, p.tableMap, err = writeSchemaSnapshotAndMeta(p.db, storage, gcTs); err != nil {
		// TODO: retry
		log.Fatal("fail to initialize from kv snapshot")
	}
	p.gcTs = gcTs
	p.upperBound = upperBoundMeta{
		FinishedDDLTs: 0,
		SchemaVersion: 0,
		ResolvedTs:    gcTs,
	}
	writeUpperBoundMeta(p.db, p.upperBound)
	log.Info("schema store initialize from kv storage done",
		zap.Int("databaseMapLen", len(p.databaseMap)),
		zap.Int("tableMapLen", len(p.tableMap)))
}

func (p *persistentStorage) initializeFromDisk() {
	cleanObseleteData(p.db, 0, p.gcTs)

	storageSnap := p.db.NewSnapshot()
	defer storageSnap.Close()

	var err error
	if p.databaseMap, err = loadDatabasesInKVSnap(storageSnap, p.gcTs); err != nil {
		log.Fatal("load database info from disk failed")
	}

	if p.tableMap, err = loadTablesInKVSnap(storageSnap, p.gcTs); err != nil {
		log.Fatal("load tables in kv snapshot failed")
	}

	if p.tablesDDLHistory, p.tableTriggerDDLHistory, err = loadAndApplyDDLHistory(
		storageSnap,
		p.gcTs,
		p.upperBound.FinishedDDLTs,
		p.databaseMap,
		p.tableMap); err != nil {
		log.Fatal("fail to initialize from disk")
	}
}

// getAllPhysicalTables returns all physical tables in the snapshot
// caller must ensure current resolve ts is larger than snapTs
func (p *persistentStorage) getAllPhysicalTables(snapTs uint64, tableFilter filter.Filter) ([]common.Table, error) {
	storageSnap := p.db.NewSnapshot()
	defer storageSnap.Close()

	p.mu.Lock()
	if snapTs < p.gcTs {
		return nil, fmt.Errorf("snapTs %d is smaller than gcTs %d", snapTs, p.gcTs)
	}
	gcTs := p.gcTs
	p.mu.Unlock()

	return loadAllPhysicalTablesInSnap(storageSnap, gcTs, snapTs, tableFilter)
}

// only return when table info is initialized
func (p *persistentStorage) registerTable(tableID int64, startTs uint64) error {
	p.mu.Lock()
	if startTs < p.gcTs {
		p.mu.Unlock()
		return fmt.Errorf("startTs %d is smaller than gcTs %d", startTs, p.gcTs)
	}
	p.tableRegisteredCount[tableID] += 1
	store, ok := p.tableInfoStoreMap[tableID]
	if !ok {
		store = newEmptyVersionedTableInfoStore(tableID)
		p.tableInfoStoreMap[tableID] = store
	}
	p.mu.Unlock()

	if !ok {
		return p.buildVersionedTableInfoStore(store)
	}

	store.waitTableInfoInitialized()
	return nil
}

func (p *persistentStorage) unregisterTable(tableID int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tableRegisteredCount[tableID] -= 1
	if p.tableRegisteredCount[tableID] <= 0 {
		if _, ok := p.tableInfoStoreMap[tableID]; !ok {
			return fmt.Errorf(fmt.Sprintf("table %d not found", tableID))
		}
		delete(p.tableInfoStoreMap, tableID)
	}
	return nil
}

func (p *persistentStorage) getTableInfo(tableID int64, ts uint64) (*common.TableInfo, error) {
	p.mu.Lock()
	store, ok := p.tableInfoStoreMap[tableID]
	if !ok {
		return nil, fmt.Errorf(fmt.Sprintf("table %d not found", tableID))
	}
	p.mu.Unlock()
	return store.getTableInfo(ts)
}

// TODO: not all ddl in p.tablesDDLHistory should be sent to the dispatcher, verify dispatcher will set the right range
func (p *persistentStorage) fetchTableDDLEvents(tableID int64, start, end uint64) ([]common.DDLEvent, error) {
	p.mu.RLock()
	if start < p.gcTs {
		p.mu.Unlock()
		return nil, fmt.Errorf("startTs %d is smaller than gcTs %d", start, p.gcTs)
	}
	history, ok := p.tablesDDLHistory[tableID]
	if !ok {
		p.mu.RUnlock()
		return nil, nil
	}
	index := sort.Search(len(history), func(i int) bool {
		return history[i] > start
	})
	// no events to read
	if index == len(history) {
		p.mu.RUnlock()
		return nil, nil
	}
	// copy all target ts to a new slice
	allTargetTs := make([]uint64, 0)
	for i := index; i < len(history); i++ {
		if history[i] <= end {
			allTargetTs = append(allTargetTs, history[i])
		}
	}

	storageSnap := p.db.NewSnapshot()
	defer storageSnap.Close()
	p.mu.RUnlock()

	events := make([]common.DDLEvent, len(allTargetTs))
	for i, ts := range allTargetTs {
		rawEvent := readDDLEvent(storageSnap, ts)
		events[i] = buildDDLEvent(&rawEvent)
	}

	return events, nil
}

func (p *persistentStorage) fetchTableTriggerDDLEvents(tableFilter filter.Filter, start uint64, limit int) ([]common.DDLEvent, error) {
	p.mu.Lock()
	if start < p.gcTs {
		p.mu.Unlock()
		return nil, fmt.Errorf("startTs %d is smaller than gcTs %d", start, p.gcTs)
	}
	p.mu.Unlock()

	events := make([]common.DDLEvent, 0)
	nextStartTs := start
	storageSnap := p.db.NewSnapshot()
	defer storageSnap.Close()
	for {
		allTargetTs := make([]uint64, 0, limit)
		p.mu.RLock()
		// log.Info("fetchTableTriggerDDLEvents",
		// 	zap.Any("start", start),
		// 	zap.Int("limit", limit),
		// 	zap.Any("tableTriggerDDLHistory", p.tableTriggerDDLHistory))
		index := sort.Search(len(p.tableTriggerDDLHistory), func(i int) bool {
			return p.tableTriggerDDLHistory[i] > nextStartTs
		})
		// no more events to read
		if index == len(p.tableTriggerDDLHistory) {
			p.mu.RUnlock()
			return events, nil
		}
		for i := index; i < len(p.tableTriggerDDLHistory); i++ {
			allTargetTs = append(allTargetTs, p.tableTriggerDDLHistory[i])
			if len(allTargetTs) >= limit-len(events) {
				break
			}
		}
		p.mu.RUnlock()

		if len(allTargetTs) == 0 {
			return events, nil
		}
		for _, ts := range allTargetTs {
			rawEvent := readDDLEvent(storageSnap, ts)
			var tableName string
			if rawEvent.TableInfo != nil {
				tableName = rawEvent.TableInfo.Name.O
			}
			if tableFilter.ShouldDiscardDDL(model.ActionType(rawEvent.Type), rawEvent.SchemaName, tableName) {
				continue
			}
			events = append(events, buildDDLEvent(&rawEvent))
		}
		if len(events) >= limit {
			return events, nil
		}
		nextStartTs = allTargetTs[len(allTargetTs)-1]
	}
}

func (p *persistentStorage) buildVersionedTableInfoStore(
	store *versionedTableInfoStore,
) error {
	tableID := store.getTableID()
	// get snapshot from disk before get current gc ts to make sure data is not deleted by gc process
	storageSnap := p.db.NewSnapshot()
	defer storageSnap.Close()

	p.mu.RLock()
	kvSnapVersion := p.gcTs
	tableBasicInfo, ok := p.tableMap[tableID]
	if !ok {
		log.Panic("table not found", zap.Int64("tableID", tableID))
	}
	inKVSnap := tableBasicInfo.InKVSnap
	var allDDLFinishedTs []uint64
	allDDLFinishedTs = append(allDDLFinishedTs, p.tablesDDLHistory[tableID]...)
	p.mu.RUnlock()

	getSchemaName := func(schemaID int64) (string, error) {
		p.mu.RLock()
		defer func() {
			p.mu.RUnlock()
		}()

		databaseInfo, ok := p.databaseMap[schemaID]
		if !ok {
			return "", errors.New("database not found")
		}
		return databaseInfo.Name, nil
	}

	if inKVSnap {
		if err := addTableInfoFromKVSnap(store, kvSnapVersion, storageSnap, getSchemaName); err != nil {
			return err
		}
	}

	for _, version := range allDDLFinishedTs {
		ddlEvent := readDDLEvent(storageSnap, version)
		// TODO: check ddlEvent type
		// TODO: no need fill it here, schemaName should be in event
		schemaName, err := getSchemaName(ddlEvent.SchemaID)
		if err != nil {
			log.Fatal("get schema name failed", zap.Error(err))
		}
		ddlEvent.SchemaName = schemaName

		store.applyDDLFromPersistStorage(ddlEvent)
	}
	store.setTableInfoInitialized()
	return nil
}

func addTableInfoFromKVSnap(
	store *versionedTableInfoStore,
	kvSnapVersion uint64,
	snap *pebble.Snapshot,
	getSchemaName func(schemaID int64) (string, error),
) error {
	schemaID, rawTableInfo := readSchemaIDAndTableInfoFromKVSnap(snap, store.getTableID(), kvSnapVersion)
	schemaName, err := getSchemaName(schemaID)
	if err != nil {
		return err
	}
	tableInfo := common.WrapTableInfo(schemaID, schemaName, kvSnapVersion, rawTableInfo)
	tableInfo.InitPreSQLs()
	store.addInitialTableInfo(tableInfo)
	return nil
}

func (p *persistentStorage) gc(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Minute)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			gcSafePoint, err := p.pdCli.UpdateServiceGCSafePoint(ctx, "cdc-new-store", 0, 0)
			if err != nil {
				log.Warn("get ts failed", zap.Error(err))
				continue
			}
			p.doGc(gcSafePoint)
		}
	}
}

func (p *persistentStorage) doGc(gcTs uint64) error {
	p.mu.Lock()
	if gcTs > p.upperBound.ResolvedTs {
		log.Panic("gc safe point is larger than resolvedTs",
			zap.Uint64("gcTs", gcTs),
			zap.Uint64("resolvedTs", p.upperBound.ResolvedTs))
	}
	if gcTs <= p.gcTs {
		p.mu.Unlock()
		return nil
	}
	oldGcTs := p.gcTs
	p.mu.Unlock()

	start := time.Now()
	_, tablesInKVSnap, err := writeSchemaSnapshotAndMeta(p.db, p.kvStorage, gcTs)
	if err != nil {
		// TODO: retry
		log.Warn("fail to write kv snapshot during gc",
			zap.Uint64("gcTs", gcTs))
	}

	// clean data in memeory before clean data on disk
	p.cleanObseleteDataInMemory(gcTs, tablesInKVSnap)
	cleanObseleteData(p.db, oldGcTs, gcTs)

	log.Info("gc finish",
		zap.Uint64("gcTs", gcTs),
		zap.Any("duration", time.Since(start).Seconds()))

	return nil
}

func (p *persistentStorage) cleanObseleteDataInMemory(gcTs uint64, tablesInKVSnap map[int64]*BasicTableInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.gcTs = gcTs

	for tableID := range tablesInKVSnap {
		p.tableMap[tableID].InKVSnap = true
	}

	// clean tablesDDLHistory
	for tableID, history := range p.tablesDDLHistory {
		if _, ok := tablesInKVSnap[tableID]; !ok {
			delete(p.tablesDDLHistory, tableID)
			continue
		}

		i := sort.Search(len(history), func(i int) bool {
			return history[i] > gcTs
		})
		if i == len(history) {
			delete(p.tablesDDLHistory, tableID)
			continue
		}
		p.tablesDDLHistory[tableID] = history[i:]
	}

	// clean tableTriggerDDLHistory
	i := sort.Search(len(p.tableTriggerDDLHistory), func(i int) bool {
		return p.tableTriggerDDLHistory[i] > gcTs
	})
	p.tableTriggerDDLHistory = p.tableTriggerDDLHistory[i:]

	// clean tableInfoStoreMap
	for _, store := range p.tableInfoStoreMap {
		store.gc(gcTs)
	}
}

func (p *persistentStorage) updateUpperBound(upperBound upperBoundMeta) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.upperBound = upperBound
	p.upperBoundChanged = true
}

func (p *persistentStorage) getUpperBound() upperBoundMeta {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.upperBound
}

func (p *persistentStorage) persistUpperBoundPeriodically(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			p.mu.Lock()
			if !p.upperBoundChanged {
				log.Warn("schema store upper bound not changed")
				p.mu.Unlock()
				continue
			}
			upperBound := p.upperBound
			p.upperBoundChanged = false
			p.mu.Unlock()

			writeUpperBoundMeta(p.db, upperBound)
		}
	}
}

func (p *persistentStorage) handleSortedDDLEvents(ddlEvents ...PersistedDDLEvent) error {
	// TODO: ignore some ddl event
	// TODO: check ddl events are sorted

	p.mu.Lock()
	for i := range ddlEvents {
		fillSchemaName(&ddlEvents[i], p.databaseMap)
		fillInfluencedTableInfo(&ddlEvents[i], p.databaseMap, p.tableMap)
		// even if the ddl is skipped here, it can still be written to disk.
		// because when apply this ddl at restart, it will be skipped again.
		if shouldSkipDDL(&ddlEvents[i], p.databaseMap, p.tableMap) {
			continue
		}
		var err error
		if p.tableTriggerDDLHistory, err = updateDDLHistory(
			&ddlEvents[i],
			p.databaseMap,
			p.tableMap,
			p.tablesDDLHistory,
			p.tableTriggerDDLHistory); err != nil {
			return err
		}
		if err := updateDatabaseInfoAndTableInfo(&ddlEvents[i], p.databaseMap, p.tableMap); err != nil {
			return err
		}
		if err := updateRegisteredTableInfoStore(ddlEvents[i], p.tableInfoStoreMap); err != nil {
			return err
		}
	}
	p.mu.Unlock()

	writeDDLEvents(p.db, ddlEvents...)
	return nil
}

func fillSchemaName(
	event *PersistedDDLEvent,
	databaseMap map[int64]*BasicDatabaseInfo,
) {
	switch model.ActionType(event.Type) {
	case model.ActionCreateSchema,
		model.ActionDropSchema:
		event.SchemaName = event.DBInfo.Name.O
	default:
		databaseInfo, ok := databaseMap[event.SchemaID]
		if !ok {
			log.Panic("database not found",
				zap.Int64("schemaID", event.SchemaID))
		}
		event.SchemaName = databaseInfo.Name
	}
}

func fillInfluencedTableInfo(
	event *PersistedDDLEvent,
	databaseMap map[int64]*BasicDatabaseInfo,
	tableMap map[int64]*BasicTableInfo,
) {
	switch model.ActionType(event.Type) {
	case model.ActionCreateSchema,
		model.ActionAddColumn,
		model.ActionDropColumn,
		model.ActionAddIndex,
		model.ActionDropIndex,
		model.ActionAddForeignKey,
		model.ActionDropForeignKey,
		model.ActionModifyColumn,
		model.ActionRebaseAutoID,
		model.ActionSetDefaultValue,
		model.ActionShardRowID,
		model.ActionModifyTableComment,
		model.ActionRenameIndex:
		// ignore
	case model.ActionDropSchema:
		event.NeedDroppedTables = &common.InfluencedTables{
			InfluenceType: common.DB,
			SchemaID:      event.SchemaID,
			SchemaName:    event.SchemaName,
		}
	case model.ActionCreateTable:
		event.NeedAddedTables = []common.Table{
			{
				SchemaID:   event.SchemaID,
				SchemaName: event.SchemaName,
				TableID:    event.TableID,
				TableName:  event.TableInfo.Name.O,
			},
		}
	case model.ActionDropTable:
		event.NeedDroppedTables = &common.InfluencedTables{
			InfluenceType: common.Normal,
			TableIDs:      []int64{event.TableID},
		}
	case model.ActionTruncateTable:
		event.NeedDroppedTables = &common.InfluencedTables{
			InfluenceType: common.Normal,
			TableIDs:      []int64{event.TableID},
		}
		event.NeedAddedTables = []common.Table{
			{
				SchemaID: event.SchemaID,
				TableID:  event.TableInfo.ID,
			},
		}
	case model.ActionRenameTable:
		event.BlockedTables = &common.InfluencedTables{
			InfluenceType: common.Normal,
			TableIDs:      []int64{event.TableID},
		}
	case model.ActionCreateView:
		event.BlockedTables = &common.InfluencedTables{
			InfluenceType: common.All,
		}
	default:
		log.Panic("unknown ddl type",
			zap.Any("ddlType", event.Type),
			zap.String("DDL", event.Query))
	}
}

func shouldSkipDDL(
	event *PersistedDDLEvent,
	databaseMap map[int64]*BasicDatabaseInfo,
	tableMap map[int64]*BasicTableInfo,
) bool {
	switch model.ActionType(event.Type) {
	case model.ActionCreateSchema:
		if _, ok := databaseMap[event.SchemaID]; ok {
			log.Warn("database already exists. ignore DDL ",
				zap.String("DDL", event.Query),
				zap.Int64("jobID", event.ID),
				zap.Int64("schemaID", event.SchemaID),
				zap.Uint64("finishTs", event.FinishedTs),
				zap.Int64("jobSchemaVersion", event.SchemaVersion))
			return true
		}
	case model.ActionCreateTable:
		if _, ok := tableMap[event.TableID]; ok {
			log.Warn("table already exists. ignore DDL ",
				zap.String("DDL", event.Query),
				zap.Int64("jobID", event.ID),
				zap.Int64("schemaID", event.SchemaID),
				zap.Int64("tableID", event.TableID),
				zap.Uint64("finishTs", event.FinishedTs),
				zap.Int64("jobSchemaVersion", event.SchemaVersion))
			return true
		}
	}
	return false
}

func updateDDLHistory(
	ddlEvent *PersistedDDLEvent,
	databaseMap map[int64]*BasicDatabaseInfo,
	tableMap map[int64]*BasicTableInfo,
	tablesDDLHistory map[int64][]uint64,
	tableTriggerDDLHistory []uint64,
) ([]uint64, error) {
	addTableHistory := func(tableID int64) {
		tablesDDLHistory[tableID] = append(tablesDDLHistory[tableID], ddlEvent.FinishedTs)
	}

	switch model.ActionType(ddlEvent.Type) {
	case model.ActionCreateSchema,
		model.ActionCreateView:
		tableTriggerDDLHistory = append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
		for tableID := range tableMap {
			addTableHistory(tableID)
		}
	case model.ActionDropSchema:
		tableTriggerDDLHistory = append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
		for tableID := range databaseMap[ddlEvent.SchemaID].Tables {
			addTableHistory(tableID)
		}
	case model.ActionCreateTable,
		model.ActionDropTable:
		tableTriggerDDLHistory = append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
		addTableHistory(ddlEvent.TableID)
	case model.ActionAddColumn,
		model.ActionDropColumn,
		model.ActionAddIndex,
		model.ActionDropIndex,
		model.ActionAddForeignKey,
		model.ActionDropForeignKey,
		model.ActionModifyColumn,
		model.ActionRebaseAutoID,
		model.ActionSetDefaultValue,
		model.ActionShardRowID,
		model.ActionModifyTableComment,
		model.ActionRenameIndex:
		addTableHistory(ddlEvent.TableID)
	case model.ActionTruncateTable:
		addTableHistory(ddlEvent.TableID)
		addTableHistory(ddlEvent.TableInfo.ID)
	case model.ActionRenameTable:
		tableTriggerDDLHistory = append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
		addTableHistory(ddlEvent.TableID)
	default:
		log.Panic("unknown ddl type",
			zap.Any("ddlType", ddlEvent.Type),
			zap.String("DDL", ddlEvent.Query))
	}

	return tableTriggerDDLHistory, nil
}

func updateDatabaseInfoAndTableInfo(
	event *PersistedDDLEvent,
	databaseMap map[int64]*BasicDatabaseInfo,
	tableMap map[int64]*BasicTableInfo,
) error {
	addTableToDB := func(schemaID int64, tableID int64) {
		databaseInfo, ok := databaseMap[schemaID]
		if !ok {
			log.Panic("database not found.",
				zap.String("DDL", event.Query),
				zap.Int64("jobID", event.ID),
				zap.Int64("schemaID", schemaID),
				zap.Int64("tableID", tableID),
				zap.Uint64("finishTs", event.FinishedTs),
				zap.Int64("jobSchemaVersion", event.SchemaVersion))
		}
		databaseInfo.Tables[tableID] = true
	}

	removeTableFromDB := func(schemaID int64, tableID int64) {
		databaseInfo, ok := databaseMap[schemaID]
		if !ok {
			log.Panic("database not found. ",
				zap.String("DDL", event.Query),
				zap.Int64("jobID", event.ID),
				zap.Int64("schemaID", schemaID),
				zap.Int64("tableID", tableID),
				zap.Uint64("finishTs", event.FinishedTs),
				zap.Int64("jobSchemaVersion", event.SchemaVersion))
		}
		delete(databaseInfo.Tables, tableID)
	}

	createTable := func(schemaID int64, tableID int64) {
		addTableToDB(schemaID, tableID)
		tableMap[tableID] = &BasicTableInfo{
			SchemaID: schemaID,
			Name:     event.TableInfo.Name.O,
			InKVSnap: false,
		}
	}

	dropTable := func(schemaID int64, tableID int64) {
		removeTableFromDB(schemaID, tableID)
		delete(tableMap, tableID)
	}

	switch model.ActionType(event.Type) {
	case model.ActionCreateSchema:
		databaseMap[event.SchemaID] = &BasicDatabaseInfo{
			Name:   event.SchemaName,
			Tables: make(map[int64]bool),
		}
	case model.ActionDropSchema:
		delete(databaseMap, event.SchemaID)
	case model.ActionCreateTable:
		createTable(event.SchemaID, event.TableID)
	case model.ActionDropTable:
		dropTable(event.SchemaID, event.TableID)
	case model.ActionAddColumn,
		model.ActionDropColumn,
		model.ActionAddIndex,
		model.ActionDropIndex,
		model.ActionAddForeignKey,
		model.ActionDropForeignKey,
		model.ActionModifyColumn,
		model.ActionRebaseAutoID:
		// ignore
	case model.ActionTruncateTable:
		dropTable(event.SchemaID, event.TableID)
		// TODO: do we need check the return value of createTable?
		createTable(event.SchemaID, event.TableInfo.ID)
	case model.ActionRenameTable:
		oldSchemaID := tableMap[event.TableID].SchemaID
		if oldSchemaID != event.SchemaID {
			tableMap[event.TableID].SchemaID = event.SchemaID
			removeTableFromDB(oldSchemaID, event.TableID)
			addTableToDB(event.SchemaID, event.TableID)
		}
		tableMap[event.TableID].Name = event.TableInfo.Name.O
	case model.ActionSetDefaultValue,
		model.ActionShardRowID,
		model.ActionModifyTableComment,
		model.ActionRenameIndex,
		model.ActionCreateView:
		// TODO
		// seems can be ignored
	case model.ActionAddTablePartition:
		// TODO
	default:
		log.Panic("unknown ddl type",
			zap.Any("ddlType", event.Type),
			zap.String("DDL", event.Query))
	}

	return nil
}

func updateRegisteredTableInfoStore(
	event PersistedDDLEvent,
	tableInfoStoreMap map[int64]*versionedTableInfoStore,
) error {
	switch model.ActionType(event.Type) {
	case model.ActionCreateSchema,
		model.ActionDropSchema,
		model.ActionCreateTable,
		model.ActionAddIndex,
		model.ActionDropIndex,
		model.ActionAddForeignKey,
		model.ActionDropForeignKey,
		model.ActionRenameTable,
		model.ActionCreateView:
		// ignore
	case model.ActionDropTable,
		model.ActionAddColumn,
		model.ActionDropColumn,
		model.ActionTruncateTable,
		model.ActionModifyColumn,
		model.ActionRebaseAutoID,
		model.ActionSetDefaultValue,
		model.ActionShardRowID,
		model.ActionModifyTableComment,
		model.ActionRenameIndex:
		store, ok := tableInfoStoreMap[event.TableID]
		if ok {
			store.applyDDL(event)
		}
	default:
		log.Panic("unknown ddl type",
			zap.Any("ddlType", event.Type),
			zap.String("DDL", event.Query))
	}
	return nil
}

func buildDDLEvent(rawEvent *PersistedDDLEvent) common.DDLEvent {
	event := common.DDLEvent{
		Type:       rawEvent.Type,
		SchemaID:   rawEvent.SchemaID,
		TableID:    rawEvent.TableID,
		SchemaName: rawEvent.SchemaName,
		Query:      rawEvent.Query,
		TableInfo:  rawEvent.TableInfo,
		FinishedTs: rawEvent.FinishedTs,
	}
	if rawEvent.TableInfo != nil {
		event.TableName = rawEvent.TableInfo.Name.O
	}

	// TODO: respect filter
	event.BlockedTables = rawEvent.BlockedTables
	event.NeedDroppedTables = rawEvent.NeedDroppedTables
	event.NeedAddedTables = rawEvent.NeedAddedTables

	return event
}

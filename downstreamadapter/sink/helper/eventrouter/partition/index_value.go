// Copyright 2022 PingCAP, Inc.
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

package partition

import (
	"strconv"
	"sync"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/hash"
	"go.uber.org/zap"
)

type IndexValuePartitionGenerator struct {
	hasher *hash.PositionInertia
	lock   sync.Mutex

	IndexName string
}

func newIndexValuePartitionGenerator(indexName string) *IndexValuePartitionGenerator {
	return &IndexValuePartitionGenerator{
		hasher:    hash.NewPositionInertia(),
		IndexName: indexName,
	}
}

func (r *IndexValuePartitionGenerator) GeneratePartitionIndexAndKey(row *common.RowChangedEvent, partitionNum int32) (int32, string, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.hasher.Reset()
	r.hasher.Write([]byte(row.TableInfo.GetSchemaName()), []byte(row.TableInfo.GetTableName()))

	dispatchCols := row.Columns
	if len(row.Columns) == 0 {
		dispatchCols = row.PreColumns
	}

	// the most normal case, index-name is not set, use the handle key columns.
	if r.IndexName == "" {
		for _, col := range dispatchCols {
			if col == nil {
				continue
			}
			if col.Flag.IsHandleKey() {
				r.hasher.Write([]byte(col.Name), []byte(model.ColumnValueString(col.Value)))
			}
		}
	} else {
		names, offsets, ok := row.TableInfo.IndexByName(r.IndexName)
		if !ok {
			log.Error("index not found when dispatch event",
				zap.Any("tableName", row.TableInfo.GetTableName()),
				zap.String("indexName", r.IndexName))
			return 0, "", errors.ErrDispatcherFailed.GenWithStack(
				"index not found when dispatch event, table: %v, index: %s", row.TableInfo.GetTableName(), r.IndexName)
		}
		for idx := 0; idx < len(names); idx++ {
			col := dispatchCols[offsets[idx]]
			if col == nil {
				continue
			}
			r.hasher.Write([]byte(names[idx]), []byte(model.ColumnValueString(col.Value)))
		}
	}

	sum32 := r.hasher.Sum32()
	return int32(sum32 % uint32(partitionNum)), strconv.FormatInt(int64(sum32), 10), nil
}

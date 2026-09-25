package lakehouse

import (
	"fmt"
	"time"
)

// SnapshotKind 区分快照来源，用于日志与清理判定。
type SnapshotKind string

const (
	SnapshotInit       SnapshotKind = "init"
	SnapshotAppend     SnapshotKind = "append"
	SnapshotUpsert     SnapshotKind = "upsert"
	SnapshotEvolution  SnapshotKind = "evolution"
	SnapshotCompaction SnapshotKind = "compaction"
)

// DataFile 是一个不可变数据文件清单条目。
type DataFile struct {
	Path        string `json:"path"`
	Seq         int64  `json:"seq"`       // 产生该文件的提交序号
	SchemaID    int    `json:"schema_id"` // 写入该文件时的 schema 版本（用于按 FieldID 投影）
	RecordCount int64  `json:"record_count"`
	// Deletes 仅 compaction/upsert 快照使用：被新文件替换、逻辑删除的旧文件。
	Deletes []string `json:"deletes,omitempty"`
}

// Snapshot 是一次提交产生的不可变快照。
// Files 是该快照读视图的完整、自包含文件清单（类似 Iceberg 的 manifest list），
// 因此读取任一快照都不依赖祖先快照——旧快照可被独立清理。
// AddedFiles/DeletedFiles 仅作为审计与判定依据保留。
type Snapshot struct {
	ID           string       `json:"id"`
	Seq          int64        `json:"seq"`
	ParentID     string       `json:"parent_id"`
	SchemaID     int          `json:"schema_id"`
	Kind         SnapshotKind `json:"kind"`
	Files        []DataFile   `json:"files"`
	AddedFiles   []DataFile   `json:"added_files,omitempty"`
	DeletedFiles []string     `json:"deleted_files,omitempty"`
	CreatedAt    time.Time    `json:"created_at"`
	Reason       string       `json:"reason"` // 判定依据摘要，便于日志解释
}

// TableMetadata 是元数据头文件的内容（版本化、整体不可变）。
type TableMetadata struct {
	Version       int         `json:"version"`
	TableID       string      `json:"table_id"`
	LastSeq       int64       `json:"last_seq"`
	CurrentSnapID string      `json:"current_snapshot_id"`
	Schemas       []*Schema   `json:"schemas"`
	Snapshots     []*Snapshot `json:"snapshots"`
	// PinnedSnapshots holder -> snapshotID，记录被显式引用、禁止清理的快照（长读/分支）。
	PinnedSnapshots map[string]string `json:"pinned_snapshots,omitempty"`
	AdminToken      string            `json:"admin_token"`
	CreatedAt       time.Time         `json:"created_at"`
}

const (
	metadataDir     = "_metadata"
	dataDir         = "data"
	lockFileName    = "_metadata/LOCK"
	pointerFileName = "_metadata/version-pointer.json"
)

func versionFileName(version int) string {
	return fmt.Sprintf("%s/v%05d.metadata.json", metadataDir, version)
}

func dataFileName(seq int64, suffix string) string {
	return fmt.Sprintf("%s/seq-%010d-%s.jsonl", dataDir, seq, suffix)
}

func (m *TableMetadata) snapshotByID(id string) (*Snapshot, bool) {
	for _, s := range m.Snapshots {
		if s.ID == id {
			return s, true
		}
	}
	return nil, false
}

func (m *TableMetadata) schemaByID(id int) (*Schema, bool) {
	for _, s := range m.Schemas {
		if s.ID == id {
			return s, true
		}
	}
	return nil, false
}

func (m *TableMetadata) currentSchema() *Schema {
	s, ok := m.snapshotByID(m.CurrentSnapID)
	if !ok || s == nil {
		return nil
	}
	sc, ok := m.schemaByID(s.SchemaID)
	if !ok {
		return nil
	}
	return sc
}

func (m *TableMetadata) shallowCopy() *TableMetadata {
	cp := &TableMetadata{
		Version:         m.Version,
		TableID:         m.TableID,
		LastSeq:         m.LastSeq,
		CurrentSnapID:   m.CurrentSnapID,
		Schemas:         append([]*Schema(nil), m.Schemas...),
		Snapshots:       append([]*Snapshot(nil), m.Snapshots...),
		PinnedSnapshots: map[string]string{},
		AdminToken:      m.AdminToken,
		CreatedAt:       m.CreatedAt,
	}
	for k, v := range m.PinnedSnapshots {
		cp.PinnedSnapshots[k] = v
	}
	return cp
}

// dataFilesFor 返回某快照读视图下的数据文件集合。
// 快照自包含完整清单，读取不依赖祖先快照，故旧快照可被独立过期。
func dataFilesFor(m *TableMetadata, s *Snapshot) ([]DataFile, error) {
	return append([]DataFile(nil), s.Files...), nil
}

// ancestorIDs 返回快照祖先链：从最早祖先到父快照（不含自身），仅用于审计解释。
// 祖先可被合法过期，链在缺失处自然终止。
func ancestorIDs(m *TableMetadata, s *Snapshot) ([]string, error) {
	var chain []string
	cur := s.ParentID
	guard := 0
	for cur != "" {
		parent, ok := m.snapshotByID(cur)
		if !ok {
			break
		}
		chain = append(chain, cur)
		cur = parent.ParentID
		guard++
		if guard > len(m.Snapshots)+1 {
			return nil, newError(KindUnknown, "快照父链出现环路（起始快照 %s）", s.ID)
		}
	}
	// 反转：从最早到最近。
	for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
		chain[i], chain[j] = chain[j], chain[i]
	}
	return chain, nil
}

// retainSet 计算保留快照集合。每个快照自包含读视图，只需保留根本身
// （current 与被显式 pin 的快照），不再强制保留祖先。
func retainSet(m *TableMetadata, roots []string) (map[string]string, error) {
	retain := map[string]string{}
	for _, id := range roots {
		if _, ok := m.snapshotByID(id); !ok {
			return nil, newError(KindCleanupSnapshotMissing, "待保留快照 %s 不存在", id)
		}
		retain[id] = "root"
	}
	return retain, nil
}

type versionPointer struct {
	CurrentVersion int `json:"current_version"`
}

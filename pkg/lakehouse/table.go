package lakehouse

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"time"
)

// CommitRequest 是一次写入提交请求。
type CommitRequest struct {
	Kind SnapshotKind
	// Records 待写入记录（追加：原样落盘；upsert：按主键合并）。
	Records []Record
	// ExpectedBaseSeq 客户端基于哪个快照序号发起提交（乐观锁）。0 表示不指定，
	// 此时若在等待表锁期间被其它提交抢先，则以 KindConflictConcurrentCommit 拒绝。
	ExpectedBaseSeq int64
	Reason          string
}

// CommitResult 描述成功提交。
type CommitResult struct {
	SnapshotID string
	Seq        int64
	SchemaID   int
}

// EvolutionRequest 是一次结构演进提交（支持一个提交内按序应用多条 spec）。
type EvolutionRequest struct {
	ExpectedBaseSeq int64
	Specs           []EvolutionSpec
	Reason          string
}

// CompactionRequest 合并当前读视图的全部小文件为一个文件。
type CompactionRequest struct {
	ExpectedBaseSeq int64
	// FileSetBase 客户端计划时读到的文件路径集合。
	// seq 未变但文件集已变（理论上的同序文件集竞争/陈旧计划）时，
	// 以 KindConflictFileSetChanged 拒绝；为空则按调用时最新文件集计划。
	FileSetBase []string
	Reason      string
}

// ExpireRequest 清理旧快照与无引用数据文件。
type ExpireRequest struct {
	// SnapshotsToExpire 指定待删除快照；为空表示自动选择最旧可过期快照（保留 current）。
	SnapshotsToExpire []string
	// Token 管理凭证；不匹配则拒绝（越权清理）。
	Token string
	// RequestTime 客户端发起时刻；早于 now-TTL 视为过期请求拒绝（防陈旧请求）。
	RequestTime time.Time
	TTL         time.Duration
	Reason      string
}

// ExpireResult 描述清理结果。
type ExpireResult struct {
	ExpiredSnapshots []string
	DeletedDataFiles []string
	RetainedReasons  map[string]string
}

// Table 是本地湖仓表句柄。
type Table struct {
	fs         *LocalFS
	path       string
	mu         chan struct{} // 容量 1，用作进程内提交串行锁
	closed     bool
	adminToken string
	logger     CommitLogger
}

// CommitLogger 用于测试日志：打印提交序号、快照标识与判定依据。
type CommitLogger interface {
	LogCommit(seq int64, snapshotID, kind, verdict, reason string)
}

type nopLogger struct{}

func (nopLogger) LogCommit(int64, string, string, string, string) {}

// Options 打开/建表选项。
type Options struct {
	AdminToken string
	Logger     CommitLogger
}

// ColumnSpec 建表时的列定义；FieldID 按声明顺序从 1 自动分配（稳定不可变）。
type ColumnSpec struct {
	Name     string
	Type     DataType
	Nullable bool
}

func newID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Create 在本地目录新建一张表，写入初始空快照（seq=1）。
func Create(ctx context.Context, path, tableID string, columns []ColumnSpec, primaryKeys []string, opts *Options) (*Table, error) {
	o := Options{}
	if opts != nil {
		o = *opts
	}
	fs := NewLocalFS(path)
	if ok, err := fs.exists(pointerFileName); err != nil {
		return nil, err
	} else if ok {
		return nil, newError(KindInvalidArgument, "目录 %s 已存在表（%s 存在）", path, pointerFileName)
	}
	if err := fs.mkdirAll(metadataDir); err != nil {
		return nil, err
	}
	if err := fs.mkdirAll(dataDir); err != nil {
		return nil, err
	}

	// 校验建表列。
	seen := map[string]bool{}
	cols := make([]Column, 0, len(columns))
	for i, c := range columns {
		if c.Name == "" {
			return nil, newError(KindInvalidArgument, "第 %d 列名字为空", i+1)
		}
		if seen[c.Name] {
			return nil, newError(KindInvalidArgument, "列名重复: %s", c.Name)
		}
		seen[c.Name] = true
		if !validDataType(c.Type) {
			return nil, newError(KindInvalidArgument, "列 %s 类型无效: %s", c.Name, c.Type)
		}
		cols = append(cols, Column{ID: FieldID(i + 1), Name: c.Name, Type: c.Type, Nullable: c.Nullable})
	}
	s0 := &Schema{ID: 1, Columns: cols, PrimaryKeys: append([]string(nil), primaryKeys...)}
	for _, pk := range primaryKeys {
		c, ok := s0.columnByName(pk)
		if !ok {
			return nil, newError(KindInvalidArgument, "主键列 %s 不存在", pk)
		}
		if c.Nullable {
			return nil, newError(KindInvalidArgument, "主键列 %s 必须非空", pk)
		}
	}

	now := time.Now()
	initSnap := &Snapshot{
		ID:        newID(),
		Seq:       1,
		SchemaID:  1,
		Kind:      SnapshotInit,
		Files:     []DataFile{},
		CreatedAt: now,
		Reason:    "建表初始空快照",
	}
	meta := &TableMetadata{
		Version:         1,
		TableID:         tableID,
		LastSeq:         1,
		CurrentSnapID:   initSnap.ID,
		Schemas:         []*Schema{s0},
		Snapshots:       []*Snapshot{initSnap},
		PinnedSnapshots: map[string]string{},
		AdminToken:      o.AdminToken,
		CreatedAt:       now,
	}
	if err := publish(fs, meta); err != nil {
		return nil, err
	}
	t := newTable(fs, path, o)
	t.logCommit(1, initSnap.ID, string(SnapshotInit), "committed", initSnap.Reason)
	return t, nil
}

func newTable(fs *LocalFS, path string, o Options) *Table {
	lg := o.Logger
	if lg == nil {
		lg = nopLogger{}
	}
	return &Table{
		fs:         fs,
		path:       path,
		mu:         make(chan struct{}, 1),
		adminToken: o.AdminToken,
		logger:     lg,
	}
}

// Open 打开一张已存在的表。
func Open(ctx context.Context, path string, opts *Options) (*Table, error) {
	o := Options{}
	if opts != nil {
		o = *opts
	}
	fs := NewLocalFS(path)
	var ptr versionPointer
	if err := fs.readJSON(pointerFileName, &ptr); err != nil {
		return nil, err
	}
	var meta TableMetadata
	if err := fs.readJSON(versionFileName(ptr.CurrentVersion), &meta); err != nil {
		return nil, err
	}
	t := newTable(fs, path, o)
	if o.AdminToken == "" {
		t.adminToken = meta.AdminToken
	}
	return t, nil
}

func (t *Table) Close() error {
	t.closed = true
	return nil
}

func (t *Table) logCommit(seq int64, snapshotID, kind, verdict, reason string) {
	if t.logger != nil {
		t.logger.LogCommit(seq, snapshotID, kind, verdict, reason)
	}
}

// loadMeta 读取当前元数据。读取路径永远不加提交锁；元数据文件不可变，
// 指针文件原子切换，因此任何读取都落在某个一致版本上。
func (t *Table) loadMeta() (*TableMetadata, error) {
	var ptr versionPointer
	if err := t.fs.readJSON(pointerFileName, &ptr); err != nil {
		return nil, err
	}
	var meta TableMetadata
	if err := t.fs.readJSON(versionFileName(ptr.CurrentVersion), &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

// publish 原子发布新版本：先写不可变 vN.metadata.json，再原子切换指针。
// 崩溃只会留下未被引用的版本文件，不会出现部分可见。
func publish(fs *LocalFS, meta *TableMetadata) error {
	if err := fs.atomicWriteJSON(versionFileName(meta.Version), meta); err != nil {
		return err
	}
	return fs.atomicWriteJSON(pointerFileName, versionPointer{CurrentVersion: meta.Version})
}

// plan 描述提交计划。数据准备可在锁外完成，冲突检测在锁内完成。
type plan struct {
	// planAtSeq 计划时刻所基于的当前序号（调用方未显式给 ExpectedBaseSeq 时使用）。
	planAtSeq int64
	// basedFiles 准备阶段所依据的文件路径集合（compaction 文件集冲突判定用）。
	basedFiles map[string]struct{}
	// write 在锁内、冲突检测通过后落盘数据文件，返回新增文件。
	write func(seq int64) ([]DataFile, error)
	// apply 在锁内基于 latest 元数据构造新快照（必要时可在 latest.Schemas 登记新 schema）。
	apply  func(latest *TableMetadata, seq int64, written []DataFile) (*Snapshot, error)
	kind   SnapshotKind
	reason string
}

// commitMeta 统一提交协议：
//  1. 取表锁（进程内 + flock）；
//  2. 重读最新元数据，做冲突检测：
//     - 显式 ExpectedBaseSeq：不等于当前 seq -> KindConflictBaseSnapshot；
//     - compaction：文件集在准备后被改动 -> KindConflictFileSetChanged；
//     - 未给基准而计划时刻序号已落后 -> KindConflictConcurrentCommit；
//  3. 冲突检测通过后才写数据文件、构造新元数据并原子发布。
//
// 任何冲突都在数据生效前被拒绝，杜绝部分可见与重复生效。
func (t *Table) commitMeta(ctx context.Context, expectedSeq int64, p plan) (*CommitResult, error) {
	var result *CommitResult
	err := t.withTableLock(ctx, func() error {
		latest, err := t.loadMeta()
		if err != nil {
			return err
		}
		currentSeq := latest.LastSeq

		if expectedSeq != 0 && expectedSeq != currentSeq {
			r := fmt.Sprintf("基准快照序号已过期: expected_base_seq=%d current_seq=%d", expectedSeq, currentSeq)
			t.logCommit(currentSeq, "", string(p.kind), "rejected:base_snapshot", r)
			return newError(KindConflictBaseSnapshot, "%s", r)
		}
		if p.basedFiles != nil {
			cur := latest.currentSnapshot()
			curSet := map[string]struct{}{}
			for _, f := range cur.Files {
				curSet[f.Path] = struct{}{}
			}
			if !samePathSet(curSet, p.basedFiles) {
				r := fmt.Sprintf("文件集在准备阶段后被并发修改: plan_at_seq=%d current_seq=%d", p.planAtSeq, currentSeq)
				t.logCommit(currentSeq, "", string(p.kind), "rejected:file_set_changed", r)
				return newError(KindConflictFileSetChanged, "%s", r)
			}
		}
		if expectedSeq == 0 && p.planAtSeq != 0 && p.planAtSeq != currentSeq {
			r := fmt.Sprintf("等待提交锁期间已有其它提交生效: plan_at_seq=%d current_seq=%d", p.planAtSeq, currentSeq)
			t.logCommit(currentSeq, "", string(p.kind), "rejected:concurrent_commit", r)
			return newError(KindConflictConcurrentCommit, "%s", r)
		}

		newSeq := currentSeq + 1
		written, err := p.write(newSeq)
		if err != nil {
			return err
		}
		snap, err := p.apply(latest, newSeq, written)
		if err != nil {
			return err
		}
		snap.ID = newID()
		snap.Seq = newSeq
		snap.ParentID = latest.CurrentSnapID
		snap.CreatedAt = time.Now()

		next := latest.shallowCopy()
		next.Version = latest.Version + 1
		next.LastSeq = newSeq
		next.CurrentSnapID = snap.ID
		next.Snapshots = append(next.Snapshots, snap)

		if err := publish(t.fs, next); err != nil {
			return err
		}
		result = &CommitResult{SnapshotID: snap.ID, Seq: newSeq, SchemaID: snap.SchemaID}
		t.logCommit(newSeq, snap.ID, string(p.kind), "committed", p.reason)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (m *TableMetadata) currentSnapshot() *Snapshot {
	s, _ := m.snapshotByID(m.CurrentSnapID)
	return s
}

func samePathSet(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

// CommitAppend 以快照为单位追加写入：新文件直接加入读视图，旧文件保持不变。
func (t *Table) CommitAppend(ctx context.Context, req CommitRequest) (*CommitResult, error) {
	if len(req.Records) == 0 {
		return nil, newError(KindInvalidArgument, "append 记录不能为空")
	}
	meta, err := t.loadMeta()
	if err != nil {
		return nil, err
	}
	schema := meta.currentSchema()
	for i, r := range req.Records {
		if err := validateRecord(schema, r, false); err != nil {
			return nil, newError(err.(*Error).Kind, "第 %d 条记录校验失败: %s", i+1, err.(*Error).Reason)
		}
	}
	schemaID := schema.ID
	records := req.Records

	p := plan{
		// append 只新增不可变文件并并入读视图，文件集合并可交换，
		// 故并发 append 天然不冲突（串行化后逐个生效，不丢不重）。
		kind:   SnapshotAppend,
		reason: req.Reason,
		write: func(seq int64) ([]DataFile, error) {
			path := dataFileName(seq, "append")
			var n int64
			err := t.fs.writeAtomic(path, func(w io.Writer) error {
				nw, werr := writeJSONL(w, records)
				n = nw
				return werr
			})
			if err != nil {
				return nil, err
			}
			return []DataFile{{Path: path, Seq: seq, SchemaID: schemaID, RecordCount: n}}, nil
		},
		apply: func(latest *TableMetadata, seq int64, written []DataFile) (*Snapshot, error) {
			cur := latest.currentSnapshot()
			files := append(append([]DataFile(nil), cur.Files...), written...)
			return &Snapshot{
				SchemaID:     schemaID,
				Kind:         SnapshotAppend,
				Files:        files,
				AddedFiles:   written,
				DeletedFiles: []string{},
				Reason:       req.Reason,
			}, nil
		},
	}
	return t.commitMeta(ctx, req.ExpectedBaseSeq, p)
}

// writer 是 writeAtomic 回调的写出接口别名，避免额外 import。
type writer = interface {
	Write(p []byte) (int, error)
}

// CommitUpsert 按主键 upsert：读当前视图全量、按 PK 合并后重写为单个新文件；
// 旧文件整体进入删除集。整个过程是一次原子快照提交。
func (t *Table) CommitUpsert(ctx context.Context, req CommitRequest) (*CommitResult, error) {
	if len(req.Records) == 0 {
		return nil, newError(KindInvalidArgument, "upsert 记录不能为空")
	}
	meta, err := t.loadMeta()
	if err != nil {
		return nil, err
	}
	schema := meta.currentSchema()
	if len(schema.PrimaryKeys) == 0 {
		return nil, newError(KindPrimaryKeyMissing, "表 %s 未定义主键，无法 upsert", meta.TableID)
	}
	for i, r := range req.Records {
		if err := validateRecord(schema, r, true); err != nil {
			return nil, newError(err.(*Error).Kind, "第 %d 条记录校验失败: %s", i+1, err.(*Error).Reason)
		}
	}

	cur := meta.currentSnapshot()

	// 读当前视图并按主键合并（同批次后写覆盖先写；新记录覆盖旧记录）。
	existing, err := readAllMerged(t.fs, meta, cur, schema, req.Records)
	if err != nil {
		return nil, err
	}
	schemaID := schema.ID
	oldPaths := filePaths(cur.Files)

	p := plan{
		planAtSeq: meta.LastSeq,
		kind:      SnapshotUpsert,
		reason:    req.Reason,
		write: func(seq int64) ([]DataFile, error) {
			path := dataFileName(seq, "upsert")
			var n int64
			err := t.fs.writeAtomic(path, func(w io.Writer) error {
				nw, werr := writeJSONL(w, existing)
				n = nw
				return werr
			})
			if err != nil {
				return nil, err
			}
			return []DataFile{{Path: path, Seq: seq, SchemaID: schemaID, RecordCount: n}}, nil
		},
		apply: func(latest *TableMetadata, seq int64, written []DataFile) (*Snapshot, error) {
			return &Snapshot{
				SchemaID:     schemaID,
				Kind:         SnapshotUpsert,
				Files:        written,
				AddedFiles:   written,
				DeletedFiles: oldPaths,
				Reason:       req.Reason,
			}, nil
		},
	}
	if req.ExpectedBaseSeq == 0 {
		return nil, newError(KindInvalidArgument, "upsert 必须提供 ExpectedBaseSeq（读取-修改-写入需要显式乐观锁基准）")
	}
	return t.commitMeta(ctx, req.ExpectedBaseSeq, p)
}

func filePaths(fs []DataFile) []string {
	out := make([]string, 0, len(fs))
	for _, f := range fs {
		out = append(out, f.Path)
	}
	return out
}

// readAllMerged 读取快照视图记录，并应用一批 upsert 输入，返回合并后的完整记录集。
func readAllMerged(fs *LocalFS, meta *TableMetadata, snap *Snapshot, schema *Schema, incoming []Record) ([]Record, error) {
	files, err := dataFilesFor(meta, snap)
	if err != nil {
		return nil, err
	}
	merged := map[string]Record{}
	var order []string
	for _, f := range files {
		fileSchema, ok := meta.schemaByID(f.SchemaID)
		if !ok {
			return nil, newError(KindSchemaNotFound, "数据文件 %s 的 schema v%d 缺失", f.Path, f.SchemaID)
		}
		rc, err := fs.openRead(f.Path)
		if err != nil {
			return nil, err
		}
		_, err = readJSONL(rc, func(rec Record) error {
			proj := projectRecord(schema, fileSchema, rec)
			k := pkKey(schema, proj)
			if _, ok := merged[k]; !ok {
				order = append(order, k)
			}
			merged[k] = proj
			return nil
		})
		_ = rc.Close()
		if err != nil {
			return nil, err
		}
	}
	for _, r := range incoming {
		k := pkKey(schema, r)
		if _, ok := merged[k]; !ok {
			order = append(order, k)
		}
		merged[k] = r
	}
	out := make([]Record, 0, len(order))
	for _, k := range order {
		out = append(out, merged[k])
	}
	return out, nil
}

// CommitEvolution 提交结构演进。
// 多条 spec 在同一提交内按序应用；兼容性在"写任何数据之前"判定，
// 对类型收窄还会扫描当前视图全部数据，越界即提交期拒绝。
func (t *Table) CommitEvolution(ctx context.Context, req EvolutionRequest) (*CommitResult, error) {
	if len(req.Specs) == 0 {
		return nil, newError(KindInvalidArgument, "演进 spec 不能为空")
	}
	meta, err := t.loadMeta()
	if err != nil {
		return nil, err
	}
	curSchema := meta.currentSchema()
	nextSchema := curSchema.copyShallow()
	for _, spec := range req.Specs {
		ns, err := validateEvolution(nextSchema, spec)
		if err != nil {
			return nil, err
		}
		nextSchema = ns
	}
	// 若含收窄操作，扫描当前视图数据验证值域（提交期拒绝，不拖到读取）。
	for _, spec := range req.Specs {
		if spec.Op != EvolutionNarrow {
			continue
		}
		c, ok := nextSchema.columnByName(spec.Column)
		if !ok {
			return nil, newError(KindSchemaIncompatible, "收窄列 %q 不存在", spec.Column)
		}
		if err := scanNarrow(t.fs, meta, meta.currentSnapshot(), nextSchema, spec.Column, c.Type); err != nil {
			return nil, err
		}
	}

	p := plan{
		planAtSeq: meta.LastSeq,
		kind:      SnapshotEvolution,
		reason:    req.Reason,
		write:     func(seq int64) ([]DataFile, error) { return nil, nil },
		apply: func(latest *TableMetadata, seq int64, written []DataFile) (*Snapshot, error) {
			ns := nextSchema.copyShallow()
			ns.ID = len(latest.Schemas) + 1
			latest.Schemas = append(latest.Schemas, ns) // 在 apply 中登记新 schema
			cur := latest.currentSnapshot()
			return &Snapshot{
				SchemaID:     ns.ID,
				Kind:         SnapshotEvolution,
				Files:        append([]DataFile(nil), cur.Files...),
				AddedFiles:   []DataFile{},
				DeletedFiles: []string{},
				Reason:       req.Reason,
			}, nil
		},
	}
	return t.commitMeta(ctx, req.ExpectedBaseSeq, p)
}

// scanNarrow 扫描快照视图全部行，确认指定列现有值落在收窄后的值域内。
func scanNarrow(fs *LocalFS, meta *TableMetadata, snap *Snapshot, readSchema *Schema, colName string, t DataType) error {
	files, err := dataFilesFor(meta, snap)
	if err != nil {
		return err
	}
	for _, f := range files {
		fileSchema, ok := meta.schemaByID(f.SchemaID)
		if !ok {
			return newError(KindSchemaNotFound, "数据文件 %s 的 schema v%d 缺失", f.Path, f.SchemaID)
		}
		rc, err := fs.openRead(f.Path)
		if err != nil {
			return err
		}
		scanErr := error(nil)
		_, rerr := readJSONL(rc, func(rec Record) error {
			proj := projectRecord(readSchema, fileSchema, rec)
			v, present := proj[colName]
			if !present || v == nil {
				return nil
			}
			n, ok := v.(int64)
			if !ok {
				return newError(KindSchemaIncompatible, "列 %q 存在非整型值 %v", colName, v)
			}
			if !fitsType(n, t) {
				return newError(KindSchemaIncompatible,
					"类型收窄被拒绝: 列 %q 存在值 %d 超出目标类型 %s 值域（提交期数据校验失败）",
					colName, n, t)
			}
			return nil
		})
		_ = rc.Close()
		if rerr != nil {
			scanErr = rerr
		}
		if scanErr != nil {
			return scanErr
		}
	}
	return nil
}

// Compact 小文件合并：读当前视图全部文件，合并成一个新文件，旧文件进入删除集。
// 若准备阶段所依据的文件集在获锁后已被并发写改变，则以
// KindConflictFileSetChanged 明确拒绝（不重复、不丢失）。
func (t *Table) Compact(ctx context.Context, req CompactionRequest) (*CommitResult, error) {
	meta, err := t.loadMeta()
	if err != nil {
		return nil, err
	}
	cur := meta.currentSnapshot()
	if len(cur.Files) <= 1 {
		return nil, newError(KindInvalidArgument, "当前视图只有 %d 个文件，无需合并", len(cur.Files))
	}
	schema := meta.currentSchema()
	based := map[string]struct{}{}
	if len(req.FileSetBase) > 0 {
		// 客户端在锁外计划时捕获的文件集；获锁后若已变化即拒绝。
		for _, p := range req.FileSetBase {
			based[p] = struct{}{}
		}
	} else {
		for _, f := range cur.Files {
			based[f.Path] = struct{}{}
		}
	}
	// 锁外读取并合并（upsert 语义去重，非主键表按出现顺序保留）。
	merged, err := readAllForCompaction(t.fs, meta, cur, schema)
	if err != nil {
		return nil, err
	}
	schemaID := schema.ID
	oldPaths := filePaths(cur.Files)

	p := plan{
		planAtSeq:  meta.LastSeq,
		basedFiles: based,
		kind:       SnapshotCompaction,
		reason:     req.Reason,
		write: func(seq int64) ([]DataFile, error) {
			path := dataFileName(seq, "compact")
			var n int64
			err := t.fs.writeAtomic(path, func(w io.Writer) error {
				nw, werr := writeJSONL(w, merged)
				n = nw
				return werr
			})
			if err != nil {
				return nil, err
			}
			return []DataFile{{Path: path, Seq: seq, SchemaID: schemaID, RecordCount: n}}, nil
		},
		apply: func(latest *TableMetadata, seq int64, written []DataFile) (*Snapshot, error) {
			return &Snapshot{
				SchemaID:     schemaID,
				Kind:         SnapshotCompaction,
				Files:        written,
				AddedFiles:   written,
				DeletedFiles: oldPaths,
				Reason:       req.Reason,
			}, nil
		},
	}
	return t.commitMeta(ctx, req.ExpectedBaseSeq, p)
}

// readAllForCompaction 读取视图；有主键时按 PK 去重（后文件覆盖先文件），
// 无主键时保持行序全量保留。
func readAllForCompaction(fs *LocalFS, meta *TableMetadata, snap *Snapshot, schema *Schema) ([]Record, error) {
	if len(schema.PrimaryKeys) > 0 {
		return readAllMerged(fs, meta, snap, schema, nil)
	}
	var out []Record
	files, err := dataFilesFor(meta, snap)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		fileSchema, ok := meta.schemaByID(f.SchemaID)
		if !ok {
			return nil, newError(KindSchemaNotFound, "数据文件 %s 的 schema v%d 缺失", f.Path, f.SchemaID)
		}
		rc, err := fs.openRead(f.Path)
		if err != nil {
			return nil, err
		}
		_, rerr := readJSONL(rc, func(rec Record) error {
			out = append(out, projectRecord(schema, fileSchema, rec))
			return nil
		})
		_ = rc.Close()
		if rerr != nil {
			return nil, rerr
		}
	}
	return out, nil
}

// ExpireSnapshots 清理旧快照与无引用数据文件。
// 保留集 = 当前快照 + 被显式 pin 的快照；每个快照自包含读视图，无需保留祖先。
// 授权（Token）、请求新鲜度（RequestTime/TTL）、保留冲突均被明确归类拒绝。
func (t *Table) ExpireSnapshots(ctx context.Context, req ExpireRequest) (*ExpireResult, error) {
	var result *ExpireResult
	err := t.withTableLock(ctx, func() error {
		latest, err := t.loadMeta()
		if err != nil {
			return err
		}
		// 1) 越权校验。
		if req.Token == "" || req.Token != t.adminToken {
			r := "清理请求缺少凭证或凭证不匹配"
			t.logCommit(latest.LastSeq, "", "expire", "rejected:unauthorized", r)
			return newError(KindCleanupUnauthorized, "%s", r)
		}
		// 2) 新鲜度校验（过期/陈旧请求拒绝）。
		now := time.Now()
		ttl := req.TTL
		if ttl <= 0 {
			ttl = time.Minute
		}
		rt := req.RequestTime
		if rt.IsZero() {
			r := "清理请求缺少 RequestTime"
			t.logCommit(latest.LastSeq, "", "expire", "rejected:expired", r)
			return newError(KindCleanupRequestExpired, "%s", r)
		}
		if now.Sub(rt) > ttl {
			r := fmt.Sprintf("清理请求已过期: request_time=%s now=%s ttl=%s", rt.Format(time.RFC3339), now.Format(time.RFC3339), ttl)
			t.logCommit(latest.LastSeq, "", "expire", "rejected:expired", r)
			return newError(KindCleanupRequestExpired, "%s", r)
		}
		if rt.Sub(now) > ttl {
			r := "清理请求 RequestTime 远超当前时间（非法时钟）"
			t.logCommit(latest.LastSeq, "", "expire", "rejected:expired", r)
			return newError(KindCleanupRequestExpired, "%s", r)
		}

		// 3) 计算保留集。
		roots := []string{latest.CurrentSnapID}
		pinReasons := map[string]string{}
		for holder, sid := range latest.PinnedSnapshots {
			roots = append(roots, sid)
			pinReasons[sid] = "pinned-by:" + holder
		}
		retain, err := retainSet(latest, uniqueStrings(roots))
		if err != nil {
			return err
		}

		// 4) 确定待过期快照。
		var targets []*Snapshot
		retained := map[string]string{}
		if len(req.SnapshotsToExpire) == 0 {
			for _, s := range latest.Snapshots {
				if s.ID == latest.CurrentSnapID {
					continue
				}
				if _, keep := retain[s.ID]; keep {
					retained[s.ID] = pinReasons[s.ID]
					if retained[s.ID] == "" {
						retained[s.ID] = "current"
					}
					continue
				}
				targets = append(targets, s)
			}
		} else {
			for _, id := range req.SnapshotsToExpire {
				s, ok := latest.snapshotByID(id)
				if !ok {
					return newError(KindCleanupSnapshotMissing, "待删除快照 %s 不存在", id)
				}
				if why, keep := retain[id]; keep {
					reason := why
					if id == latest.CurrentSnapID {
						reason = "current"
					} else if pr := pinReasons[id]; pr != "" {
						reason = pr
					}
					r := fmt.Sprintf("快照 %s 仍被引用，拒绝清理（%s）", id, reason)
					t.logCommit(latest.LastSeq, id, "expire", "rejected:retained", r)
					return newError(KindCleanupSnapshotRetained, "%s", r)
				}
				targets = append(targets, s)
			}
		}
		if len(targets) == 0 {
			result = &ExpireResult{RetainedReasons: map[string]string{}}
			return nil
		}

		// 5) 构造新元数据：移除过期快照。
		next := latest.shallowCopy()
		next.Version = latest.Version + 1
		targetIDs := map[string]bool{}
		for _, s := range targets {
			targetIDs[s.ID] = true
		}
		kept := make([]*Snapshot, 0, len(next.Snapshots)-len(targets))
		for _, s := range next.Snapshots {
			if !targetIDs[s.ID] {
				kept = append(kept, s)
			}
		}
		next.Snapshots = kept
		// 保留仍被任何存活快照引用的 schema；无引用的旧 schema 一并从清单移除
		// （文件已随之删除，schema 也不再可达）。
		usedSchema := map[int]bool{}
		for _, s := range kept {
			usedSchema[s.SchemaID] = true
		}
		keptSchemas := []*Schema{}
		for _, sc := range next.Schemas {
			if usedSchema[sc.ID] {
				keptSchemas = append(keptSchemas, sc)
			}
		}
		next.Schemas = keptSchemas

		// 6) 计算保留快照仍引用的数据文件，其余为垃圾。
		liveFiles := map[string]bool{}
		for _, s := range kept {
			for _, f := range s.Files {
				liveFiles[f.Path] = true
			}
		}
		var garbage []string
		for _, s := range targets {
			for _, f := range s.Files {
				if !liveFiles[f.Path] {
					garbage = append(garbage, f.Path)
				}
			}
		}
		// 目标文件中也可能包含在更早快照已标记删除的路径；去重。
		garbage = uniqueStrings(garbage)

		if err := publish(t.fs, next); err != nil {
			return err
		}
		// 元数据先发布，再物理删文件：崩溃只会留下孤儿文件，绝不误删存活数据。
		var deleted []string
		for _, p := range garbage {
			if err := t.fs.remove(p); err != nil {
				// 删除失败不影响正确性（成为孤儿文件）；记录但继续。
				continue
			}
			deleted = append(deleted, p)
		}

		expiredIDs := make([]string, 0, len(targets))
		for _, s := range targets {
			expiredIDs = append(expiredIDs, s.ID)
		}
		result = &ExpireResult{ExpiredSnapshots: expiredIDs, DeletedDataFiles: deleted, RetainedReasons: retained}
		for _, id := range expiredIDs {
			t.logCommit(latest.LastSeq, id, "expire", "expired", req.Reason)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func uniqueStrings(in []string) []string {
	seen := map[string]bool{}
	out := in[:0]
	for _, s := range in {
		if seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

// PinSnapshot 显式引用一个快照（长读/分支），被引用快照不可清理。
func (t *Table) PinSnapshot(ctx context.Context, snapshotID, holder string) error {
	if holder == "" {
		return newError(KindInvalidArgument, "pin holder 不能为空")
	}
	return t.withTableLock(ctx, func() error {
		latest, err := t.loadMeta()
		if err != nil {
			return err
		}
		if _, ok := latest.snapshotByID(snapshotID); !ok {
			return newError(KindSnapshotNotFound, "快照 %s 不存在", snapshotID)
		}
		next := latest.shallowCopy()
		next.Version = latest.Version + 1
		next.PinnedSnapshots[holder] = snapshotID
		return publish(t.fs, next)
	})
}

// UnpinSnapshot 解除某个 holder 的显式引用。
func (t *Table) UnpinSnapshot(ctx context.Context, holder string) error {
	return t.withTableLock(ctx, func() error {
		latest, err := t.loadMeta()
		if err != nil {
			return err
		}
		if _, ok := latest.PinnedSnapshots[holder]; !ok {
			return newError(KindInvalidArgument, "holder %s 未持有 pin", holder)
		}
		next := latest.shallowCopy()
		next.Version = latest.Version + 1
		delete(next.PinnedSnapshots, holder)
		return publish(t.fs, next)
	})
}

// Snapshot 取快照读取句柄；id 为空/"latest" 表示当前快照。
func (t *Table) Snapshot(ctx context.Context, id string) (*SnapshotReader, error) {
	meta, err := t.loadMeta()
	if err != nil {
		return nil, err
	}
	if id == "" || id == "latest" {
		id = meta.CurrentSnapID
	}
	snap, ok := meta.snapshotByID(id)
	if !ok {
		return nil, newError(KindSnapshotNotFound, "快照 %s 不存在（可能已被过期清理）", id)
	}
	schema, ok := meta.schemaByID(snap.SchemaID)
	if !ok {
		return nil, newError(KindSchemaNotFound, "快照 %s 引用的 schema v%d 缺失", id, snap.SchemaID)
	}
	files, err := dataFilesFor(meta, snap)
	if err != nil {
		return nil, err
	}
	return &SnapshotReader{table: t, meta: meta, snap: snap, schema: schema, files: files}, nil
}

// CurrentSnapshotID 返回当前快照标识。
func (t *Table) CurrentSnapshotID(ctx context.Context) (string, error) {
	meta, err := t.loadMeta()
	if err != nil {
		return "", err
	}
	return meta.CurrentSnapID, nil
}

// ListSnapshots 列出全部仍可读取的快照（按序号升序）。
func (t *Table) ListSnapshots(ctx context.Context) ([]*Snapshot, error) {
	meta, err := t.loadMeta()
	if err != nil {
		return nil, err
	}
	return append([]*Snapshot(nil), meta.Snapshots...), nil
}

# 本地湖仓表格式（Go）

用本地文件系统模拟存储层的极简湖仓表格式，目标是让**并发写入、结构演进与快照读取**下的表状态始终可预期：

- 写入以**快照**为单位原子提交（追加 / 主键 upsert / 结构演进 / 小文件合并）；
- 任意读取都绑定某个**不可变快照**与其 schema 版本；
- 并发提交冲突在数据生效**之前**被明确拒绝，并给出**可区分的错误类别**；
- 不兼容的结构演进在**提交期**拒绝（含对既有数据的值域扫描），不拖到读取期；
- compaction 与旧快照清理期间读写不丢不重；仍被引用的快照不可清理，越权/过期清理请求被拒绝。

代码位于 `pkg/lakehouse`，无第三方依赖，仅用标准库。

## 目录布局（表目录内）

```
_metadata/
  LOCK                     # flock 排他锁文件（跨进程提交串行化）
  version-pointer.json     # 指针：当前元数据版本号（原子 rename 切换）
  v00001.metadata.json     # 版本化、不可变的表元数据
  v00002.metadata.json
  ...
data/
  seq-0000000002-append.jsonl    # 不可变数据文件（JSON Lines）
  seq-0000000003-upsert.jsonl
  seq-0000000005-compact.jsonl
```

- 数据文件**只增不改**：追加写新文件；upsert/compact 写新文件并把旧文件记入删除集。
- 元数据也是**版本化不可变文件**：`publish` 先写 `vN.metadata.json`（临时文件 + fsync + rename），再原子切换 `version-pointer.json`。读取方要么看到完整旧版本，要么看到完整新版本，不存在部分可见。崩溃只可能留下未被指针引用的孤儿版本/数据文件。

## 快照与提交协议

### 快照结构

每个快照（`Snapshot`）**自包含**读视图的完整文件清单（`Files`），而不是依赖祖先链实时回放：

- 读任意快照 = 顺序读取它列出的文件；不需要祖先快照，因此旧快照可以被**独立过期**。
- `AddedFiles` / `DeletedFiles` 仅作审计与判定依据。
- 快照携带 `Seq`（单调递增提交序号）、`SchemaID`、`ParentID`、`Kind`、`Reason`（判定依据摘要）。

### 提交流程（`commitMeta`）

1. **取表锁**：进程内 channel 锁 + `flock` 跨进程排他锁（等待可被 `context` 中断）。
2. **重读最新元数据并做冲突检测**（此时尚未写任何数据）：
   - 显式 `ExpectedBaseSeq != 当前 Seq` → `conflict_base_snapshot`；
   - compaction 计划依据的文件集已变化 → `conflict_file_set_changed`；
   - 需要显式基准但缺失（如 upsert 未给基准）→ `invalid_argument`。
3. 冲突检测通过后才**写数据文件**、构造新快照与新元数据。
4. **原子发布**：新 `vN.metadata.json` 落盘后切换指针；指针切换完成即提交生效，否则完全无效。

| 写入类型 | 冲突处理 |
|---|---|
| `CommitAppend` | 纯新增不可变文件并集，天然可交换：并发 append 串行化后逐个生效，**不丢不重**；显式基准过期仍报 `conflict_base_snapshot`。 |
| `CommitUpsert` | 读—改—写，**必须**携带 `ExpectedBaseSeq`；基准过期报 `conflict_base_snapshot`，失败者整体不生效（无部分可见、无重复）。 |
| `Compact` | 携带计划时文件集；文件集变化报 `conflict_file_set_changed`；显式基准过期报 `conflict_base_snapshot`。由调用方重试。 |

> 冲突错误统一属于大类 `commit_conflict`，可用 `IsKind(err, lakehouse.KindCommitConflict)` 粗判，也可用细类别（`conflict_base_snapshot` / `conflict_concurrent_commit` / `conflict_file_set_changed`）精确区分。

### 一致性读取（`SnapshotReader`）

- `table.Snapshot(ctx, id)` 绑定快照与 schema；`id == ""/"latest"` 表示当前快照。
- 读取**不加提交锁**：文件不可变，且被存活快照引用的文件不会被清理删除。
- compaction 用"先发布新元数据、后删旧文件"的顺序；长读可通过 `PinSnapshot` 显式持有引用，清理时会被保留。
- 物理行按**稳定列 ID（FieldID）**投影到快照 schema，见下。

## 结构演进兼容规则

列用 `FieldID` 唯一标识，**重命名与类型收窄不改 ID**；每次演进产生新 `Schema` 版本（ID 递增），快照引用其 `SchemaID`。所有兼容性判定在**提交期**完成：

| 操作 | 规则 | 不兼容时 |
|---|---|---|
| 新增列 `add_column` | 必须是 `nullable`（历史行没有默认值）；列名不得与现有列冲突 | `schema_incompatible` |
| 列重命名 `rename` | 目标列须存在、新名不得占用；主键按列名引用，重命名时同步改写，主键语义不变 | `schema_incompatible` |
| 类型收窄 `narrow_type` | 仅允许有符号整型**单向**收窄：`int64 > int32 > int16 > int8`；禁止放宽、禁止同类型空操作、禁止 string/boolean 跨类型转换。提交时**扫描当前快照全部数据**，任一值超出目标值域即拒绝 | `schema_incompatible` |

### 跨快照读取语义（稳定且可解释）

- **重命名**：读旧快照仍看到旧列名；读新快照时，旧文件中的列按相同 `FieldID` 映射到新列名（对读者透明）。
- **新增列**：历史行该列为 `NULL`。
- **收窄**：提交期已保证既有值合法，读取按 `int64` 原值返回，不发生读取期错误。
- 已过期的快照读取返回 `snapshot_not_found`（而不是脏数据）。

记录校验同样在提交期：未知列、类型不符、非空列为 NULL、整型越界 → `record_validation`；upsert 缺主键 → `primary_key_missing`。

## 小文件合并与快照清理

### Compact

把当前读视图全部文件合并为一个新文件，旧文件进入删除集；有主键表按 PK 去重（后写覆盖先写），无主键表保序全量保留。合并是一次普通快照提交，冲突即明确拒绝，调用方可安全重试。

### ExpireSnapshots

- **保留集** = 当前快照 + 被 `PinSnapshot` 显式引用的快照。因为快照自包含读视图，无需保留祖先。
- 请求三重校验，失败均有独立类别：
  - `Token` 与建表 `AdminToken` 不符/缺失 → `cleanup_unauthorized`（越权）；
  - `RequestTime` 早于 `now - TTL`（默认 1 分钟）或时钟异常超前 → `cleanup_request_expired`（陈旧/重放）；
  - 目标是当前快照、被 pin 快照或不存在 → `cleanup_snapshot_retained` / `cleanup_snapshot_missing`。
- 删除顺序：**先发布移除快照后的新元数据，再物理删除无引用数据文件**。删除失败只产生孤儿文件，绝不影响存活数据。
- 自动模式（不指定目标）清理所有非保留快照；也可显式指定目标列表。

## 错误分类

`Error.Kind` 为稳定字符串，`IsKind(err, kind)` 判定；冲突细类同时归属 `commit_conflict` 大类：

`schema_incompatible`、`commit_conflict`（`conflict_base_snapshot` / `conflict_concurrent_commit` / `conflict_file_set_changed`）、`snapshot_not_found`、`schema_not_found`、`primary_key_missing`、`record_validation`、`cleanup_unauthorized`、`cleanup_request_expired`、`cleanup_snapshot_retained`、`cleanup_snapshot_missing`、`compaction_conflict`、`table_closed`、`invalid_argument`、`io_error`。

## 快速上手

```go
tb, _ := lakehouse.Create(ctx, "/data/tbl", "orders",
    []lakehouse.ColumnSpec{
        {Name: "id", Type: lakehouse.TypeInt64},
        {Name: "amount", Type: lakehouse.TypeInt32, Nullable: true},
    },
    []string{"id"},
    &lakehouse.Options{AdminToken: "secret"})

r1, _ := tb.CommitAppend(ctx, lakehouse.CommitRequest{
    Records: []lakehouse.Record{{"id": int64(1), "amount": int64(100)}},
})

// 读—改—写：以返回的 Seq 作为乐观锁基准
r2, err := tb.CommitUpsert(ctx, lakehouse.CommitRequest{
    Records:         []lakehouse.Record{{"id": int64(1), "amount": int64(80)}},
    ExpectedBaseSeq: r1.Seq,
})
if lakehouse.IsKind(err, lakehouse.KindCommitConflict) {
    // 重读最新快照后重试
}

rd, _ := tb.Snapshot(ctx, r2.SnapshotID) // 绑定一致快照（time travel）
rows, _ := rd.ReadAll(ctx)

// 结构演进（提交期兼容性判定）
_, err = tb.CommitEvolution(ctx, lakehouse.EvolutionRequest{
    Specs: []lakehouse.EvolutionSpec{
        {Op: lakehouse.EvolutionRename, Column: "amount", NewName: "amount_cny"},
        {Op: lakehouse.EvolutionAddColumn, Column: "tag", Type: lakehouse.TypeString, Nullable: true},
        {Op: lakehouse.EvolutionNarrow, Column: "amount_cny", NewType: lakehouse.TypeInt16},
    },
})

// 维护
_, _ = tb.Compact(ctx, lakehouse.CompactionRequest{})
_, _ = tb.ExpireSnapshots(ctx, lakehouse.ExpireRequest{
    Token: "secret", RequestTime: time.Now(),
})
```

## 验证方法

```bash
go test ./...            # 全部用例
go test -race ./...      # 竞态检测
go test -v ./...         # 查看提交日志：seq / snapshot / kind / verdict / reason
go vet ./...
```

`Options.Logger` 可注入 `CommitLogger`，每次提交/拒绝都会打印：

```
[commit-log] seq=3 snapshot=d7a28d9688 kind=upsert verdict=committed reason=worker-1
[commit-log] seq=3 snapshot= kind=upsert verdict=rejected:base_snapshot reason=基准快照序号已过期: expected_base_seq=2 current_seq=3
```

### 测试覆盖

| 用例 | 覆盖点 |
|---|---|
| `TestConcurrentUpsertConflict` | 8 路并发 upsert 同一基准：恰 1 成功，其余归类 `conflict_base_snapshot`，最终 1 行无重复 |
| `TestConcurrentAppendBaseConflict` | 过期基准 append 被拒 |
| `TestAppendNoBaseConcurrentNoLoss` | 6×20 并发 append 全部生效、120 行守恒 |
| `TestCompactionConflict` | 文件集变化→`conflict_file_set_changed`；基准过期→`conflict_base_snapshot` |
| `TestConcurrentReadDuringCompaction` | 10 轮 append/compact 交错中持续读锚点快照，行数恒定；清理后 current 完好、过期快照 `not_found` |
| `TestSchemaEvolutionAcrossSnapshots` | rename / add nullable / narrow 后，旧快照保旧名、新快照按 FieldID 映射、历史新列为 NULL |
| `TestIncompatibleEvolutionsRejectedAtCommit` | 6 种不兼容演进全部提交期拒绝 |
| `TestNarrowRejectedDueToExistingData` | 越界收窄提交期拒绝且表状态不变 |
| `TestRecordValidationAtCommit` | 类型/非空/主键错误归类 |
| `TestUpsertIdempotent` | upsert 覆盖+插入，旧快照不变 |
| `TestCompactionNoLossNoDup` | 4→1 文件合并，行数守恒、删除集正确 |
| `TestExpireSnapshots` | 越权、过期请求、current/pinned 保留、过期后垃圾文件物理删除且 current 可读 |
| `TestExpireMissingSnapshot` | 清理不存在快照 → `cleanup_snapshot_missing` |

## 设计边界（有意为之）

- 单机本地文件系统：锁语义假设同一文件系统（`flock` + rename）；不模拟分布式锁。
- compaction/upsert 失败不自动重试，交由调用方基于最新快照重试，错误原因足够区分。
- 旧版本 `vN.metadata.json` 默认保留（可审计），只对无引用**数据文件**做物理 GC。

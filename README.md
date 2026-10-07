# lakehouse-table-format-go

用本地文件系统模拟存储层的迷你湖仓表格式，提供快照提交、结构演进、
小文件合并、快照清理、**提交历史与回滚**、**快照固定（pin）**能力，
保证并发写入与跨快照读取下表状态可预期。

代码位于 `lakehouse/` 包。

## 存储布局

```
<dir>/
  data/part-XXXXXX-N.jsonl   数据文件（JSON Lines，行内按列 ID 存值）
  snapshots/NNNNNN.json      快照清单（schema + 数据文件清单 + 父序号）
  pins/NNNNNN.json           快照固定标记（O_EXCL 创建，存在即表示被固定）
  table.lock                 表级跨进程互斥锁（flock）
```

快照清单字段：`seq / parent / kind / schema / data_files / keys /
row_count / rollback_to`。`row_count` 是提交时物化的逻辑行数，
历史列表直接读它，**不需要扫描数据文件**。

## 快照与提交协议

- 每次提交产生一个**不可变快照**，序号为单调递增整数；快照文件以
  `O_EXCL` 排他创建，是唯一的提交点——跨进程并发时只有一个提交者能成功。
- 事务（`Table.Begin`）采用**乐观并发控制**：记录基线快照序号，
  `Commit` 时校验基线未被推进，然后先写数据文件、再创建快照文件。
  快照出现之前，其数据对任何读取者不可见，因此**不存在部分可见**。
- 所有元数据变更（提交/合并/回滚/固定/清理）在进程内互斥锁之外再叠加
  一层对 `table.lock` 的跨进程 `flock`，保证"检查-落盘"类复合操作
  在多进程下也不交错。
- 读取（`Table.Scan(seq)`）总是基于某个已提交快照的完整文件清单，
  按文件顺序合并，同主键行后者覆盖前者（upsert 语义），不重复、不丢失。
- 冲突归类（`errors.Is` 可判定）：
  - `ErrConflictConcurrentCommit`：基线快照已被其他提交推进；
  - `ErrConflictPrimaryKey`：upsert 触碰的主键与基线之后的提交重叠
    （每次 upsert 提交在快照中记录 `keys`，提交时做交集检测）；
  - `ErrSnapshotNotFound`：读取/回滚/固定的快照不存在或已被清理。

## 结构演进兼容规则

列以**稳定列 ID** 寻址，数据文件只存列 ID，因此演进与历史数据解耦。
所有演进在**提交时**校验（`validateEvolution`），不兼容即拒绝，
读取阶段不会再报结构错误。

| 演进 | 规则 | 读取语义 |
|---|---|---|
| 新增列 | 必须可空 | 旧快照看不到新列；新快照中历史行补 `null` |
| 类型收窄 | 仅 `int64 -> int32`，且提交时扫描存量数据验证全部值可表示 | 新旧快照读取值一致 |
| 列重命名 | 按列 ID 匹配，新名不得与其他列冲突 | 新旧快照读取均正常 |
| 删列 / 主键变更 / 其他类型变更 / 新增非空列 / 重名 | **拒绝**（`ErrSchemaIncompatible`） | — |

被拒绝的提交不会推进快照序号。

## 合并与清理

- `Table.Compact()`：把当前快照的全部数据文件合并为单文件，以新快照
  （`kind=compact`）发布。旧快照及文件保持不动，合并期间的并发读写
  不受影响；若合并与提交竞争同一序号，合并以
  `ErrConflictConcurrentCommit` 失败，不会覆盖他人数据。
- `Table.Cleanup(beforeSeq, retain)`：删除序号 `<= beforeSeq` 且不在
  最近 `retain` 个快照之内的快照文件；数据文件仅当不被任何存活快照
  **（含所有被固定快照）**引用时才删除。任何拒绝都保证**零删除**——
  一个文件都不会删，表保持完整可读。拒绝原因可分别用 `errors.Is` 判定，
  且都同时匹配外层 `ErrCleanupRejected`（老代码不回归）：

  | 细分错误 | 触发条件 |
  |---|---|
  | `ErrCleanupOutOfRange` | 请求越权/超出允许范围：`retain < 1`，或 `beforeSeq` 已伸入最近 `retain` 个快照（会删掉当前必须保留的版本） |
  | `ErrCleanupPinned` | 待删候选中存在被固定的快照（哪怕它已掉到保留窗口之外） |
  | `ErrCleanupNothing` | 按当前规则没有任何可清理对象（`beforeSeq` 之前没有快照） |

## 提交历史

- `Table.History(afterSeq, limit)` 按序号升序返回 `CommitInfo`：
  `seq`（序号）、`parent`（基于哪个历史版本产生）、`kind`
  （`schema/append/upsert/compact/rollback`）、`row_count`（该版本
  逻辑行数，提交时物化）、`rollback_to`（仅回滚提交非 0）。
- **规模友好**：只读 `snapshots/*.json` 元数据，不打开任何数据文件；
  `afterSeq` 游标 + `limit` 分页，历史再长也能增量拉取，不会一次读全量。

## 回滚

- `Table.Rollback(targetSeq)` 是 `BeginRollback(targetSeq) + Commit()`
  的便捷封装；需要和并发写入明确分出胜负时，请显式使用
  `BeginRollback`（它在开启时记录基线，并立即校验目标快照存在）。
- 回滚是一次**正常的、向后追加的提交**：新快照 `parent=回滚前表头`、
  `kind=rollback`、`rollback_to=目标序号`，内容（schema 与文件清单）
  复用目标快照。历史快照及其读取结果**永不被改写或抹掉**，回滚后仍可
  继续正常写入（被回退掉的主键不会复活）。
- 回滚走与写入相同的乐观协议：基线表头在提交期间被写入/合并/另一次
  回滚推进，后到者收到 `ErrConflictConcurrentCommit`——绝不会出现
  回滚盖掉并发写入，也不会两笔都生效；冲突后基于新表头重新
  `BeginRollback` 即可重试。目标快照不存在返回 `ErrSnapshotNotFound`。

## 快照固定（pin）

供对账/重放类离线任务长期盯住某个历史快照反复读。

- `Table.Pin(seq)`：固定快照。快照不存在 → `ErrSnapshotNotFound`
  （明确是找不到，不当成功）；已固定 → `ErrAlreadyPinned`（幂等命中）。
- `Table.Unpin(seq)`：解除固定。快照不存在 → `ErrSnapshotNotFound`；
  快照存在但未固定 → `ErrNotPinned`。
- `Table.Pins()`：当前固定序号列表（升序）。
- 保护边界：**清理**命中固定快照时整批拒绝（`ErrCleanupPinned`，
  零删除）；固定快照引用的数据文件一律视为存活，不会被回收。
  **合并不删除任何文件**，固定快照在合并后自然保持完整可读。
  固定只影响清理决策，不阻止回滚或写入，也不修改被固定的快照。

## 验证方法

```bash
go test ./... -v
```

测试覆盖：

- `table_test.go`：并发提交冲突（同基线双提交、8 路并发竞抢仅 1 胜）、
  主键冲突归类、快照隔离（旧快照不受新提交影响）、快照不存在归类；
- `schema_test.go`：新增列/重命名/收窄的跨快照读取语义，收窄溢出与
  各类不兼容演进在提交期被拒绝；
- `compact_test.go`：合并去重保序、合并与并发写交错不丢不重、
  清理保留线、越权/过期清理拒绝（`ErrCleanupRejected` 外层归类不回归）；
- `history_test.go`：历史字段与分页、回滚追加形态与内容恢复、
  历史快照不改写、回滚后继续写入、目标不存在归类、
  回滚 vs 写入同基线竞争恰一方胜、过期回滚冲突后重试；
- `pin_test.go`：固定/重复固定/解固/未固定/目标不存在的结果判定，
  固定快照在清理（整批拒绝零删除）与合并后存活，三类清理拒绝分类；
- `concurrency_test.go`：写入/合并/回滚/清理 + 固定锚点混合并发，
  验证唯一提交点、固定锚点内容不变、每个存活快照物化行数与实际一致、
  竞态检测通过。

单测日志通过 `t.Logf` 打印每一步在验什么、依据是什么（"判定依据: ..."）。

### 手工复现要点

```go
// 历史：只拉最近 20 条元数据，不扫数据
hist, _ := tb.History(afterSeq, 20)

// 固定老快照供离线重放；清理时它会挡住回收
_ = tb.Pin(oldSeq)

// 回滚：与写事务一样记基线，并发竞争有明确胜负
rt, _ := tb.BeginRollback(oldSeq)
newSeq, err := rt.Commit() // 冲突 => ErrConflictConcurrentCommit，重试即可

// 清理拒绝可细分
err = tb.Cleanup(before, retain)
errors.Is(err, ErrCleanupPinned)     // 命中固定快照
errors.Is(err, ErrCleanupNothing)    // 无对象可删
errors.Is(err, ErrCleanupOutOfRange) // 越权/超范围
errors.Is(err, ErrCleanupRejected)   // 外层归类始终成立
```

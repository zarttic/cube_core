# OpenGauss 连接池泄露问题报告

更新时间：2026-08-09

审计对象：`cube_web/cube_web/services/db_pool.py`（`_PostgresPool` / `_PoolContext`）

结论：存在 2 个真实泄露点和 1 处会触发池耗尽的嵌套借用。其中问题 1 会让服务在一次数据库
抖动后永久失效，必须修复。所有问题均已用最小复现脚本验证，非静态推断。

## 问题清单

| 编号 | 位置 | 问题 | 影响 | 严重度 |
| --- | --- | --- | --- | --- |
| 1 | `db_pool.py:63-65`、`71-74` | `_new_conn()` 失败后 `_created` 不回滚 | 池永久失效，需重启进程 | 致命 |
| 2 | `db_pool.py:118-127` | rollback 失败的连接仍被放回池 | 槽位被坏连接占死 | 高 |
| 3 | `partition_job_store.py:1469`、`1493` | 持有连接时再借第二个连接 | 并发下集体挂死 30s | 高 |
| 4 | `db_pool.py:97-102` | 建连接时持有全局 `_pools_lock` | 数据库慢时全局线程阻塞 | 中 |
| 5 | `db_pool.py:39`、`89-90` | `_pools` 只增不减；`_release` 无校验 | 潜在隐患，当前不触发 | 低 |

---

## 问题 1：`_created` 计数器泄露 → 池永久失效

### 现象

```python
# db_pool.py:62-65
with self._lock:
    if self._created < self._max_size:
        self._created += 1       # 先加计数
        return self._new_conn()  # 这里抛异常，计数不回滚
```

`_new_conn()` 抛异常时（OpenGauss 短暂不可用、`max_connections` 打满、网络抖动），
`_created` 已经加了 1，但连接并不存在，也没有任何路径把它减回来。异常直接向上抛给调用方，
计数就永久漏了一个。`db_pool.py:71-74` 的超时兜底分支有完全相同的缺陷。

### 复现

`max_size=3`，模拟 3 次连接失败后数据库恢复：

```text
attempt 1 failed: connection refused -> _created=1
attempt 2 failed: connection refused -> _created=2
attempt 3 failed: connection refused -> _created=3

DB recovered. _created=3, max_size=3
STILL BROKEN: RuntimeError: Timed out acquiring a database connection after 2s
              (pool max_size=3, dsn pool exhausted)
```

数据库已完全恢复，池却认为自己已满（`_created == _max_size`），而队列里一个连接都没有。
此后每次 `_acquire` 都先干等 `CUBE_WEB_PG_POOL_ACQUIRE_TIMEOUT_SECONDS`（默认 30 秒）再抛错。

### 影响

只能重启进程恢复。生产表现为「数据库抖了一下之后 cube_web 再也连不上，重启就好」，
且失败次数累积到 `max_size`（默认 8）即彻底不可用。

### 解决办法

失败时在同一把锁内回滚计数：

```python
def _create_tracked(self):
    """在锁内占位后建连接，失败必须把占位还回去。"""
    try:
        return self._new_conn()
    except BaseException:
        with self._lock:
            self._created -= 1
        raise
```

`_acquire` 的两个分支改为：先在锁内 `self._created += 1`，出锁后调用 `_create_tracked()`。
注意占位和建连接要分开——建连接是网络 I/O，不应持锁执行（与问题 4 一并解决）。

### 附带问题

现有测试 `cube_web/tests/test_db_pool.py:74`（`test_pool_timeout_retries_creation_when_capacity_freed`）
用手动 `pool._created -= 1` 模拟「失败连接把计数减回去」，它断言了一个生产代码里并不存在的
行为。修复后应改为真实触发 `_new_conn()` 异常。

---

## 问题 2：rollback 失败的连接被放回池

### 现象

```python
# db_pool.py:117-127
if exc_type is not None:
    try:
        self._conn.rollback()
    except Exception:
        pass                                     # 异常被咽掉
    if not getattr(self._conn, "closed", False):
        self._pool._release(self._conn)          # 照样还回池子
```

rollback 失败但 `closed` 仍为 `False` 时（连接半死、事务处于 aborted 状态），这个连接原样
回到队列。

### 复现

```text
connection returned to pool after FAILED rollback: True
pool _created: 1  queue size now: 0
-> next borrower inherits an aborted/dead transaction
```

### 影响

下一个借到它的线程会一路 `InFailedSqlTransaction`。坏连接在池里反复流转且永不淘汰，
等效于一个槽位被永久占死。

### 解决办法

rollback 失败即视为连接不可复用，关闭并回收计数，与 commit 失败路径保持一致：

```python
if exc_type is not None:
    try:
        self._conn.rollback()
    except Exception:
        self._discard(self._conn)   # close + _created -= 1，不还池
    else:
        if not getattr(self._conn, "closed", False):
            self._pool._release(self._conn)
        else:
            self._discard(self._conn)
```

建议把「关闭 + 减计数」抽成 `_discard()`，`__exit__` 的三处回收路径和 `_acquire` 的
死连接分支共用，避免计数增减逻辑分散在 5 个地方。

---

## 问题 3：持有连接时再借第二个连接

### 现象

AST 递归扫描 `with self._connect()` 块内的取连接调用，命中 3 处：

- `partition_job_store.py:1469` `request_cancel` → `self.get_attempt()`
- `partition_job_store.py:1493` `mark_cancelled` → `self.get_attempt()`
- `partition_domain_store.py:1782` `complete_output` → `self._recover_ambiguous_commit()`

`get_attempt()`（`partition_job_store.py:1508-1514`）自己会 `with self._connect()`，所以走到
这些分支的线程同时持有 2 个连接。

前两处是纯 bug：`get_attempt()` 只是读一行，完全可以在 `with` 块外调用。第 3 处是刻意设计
（commit 结果不明时用全新连接验证提交事实，见 `_recover_ambiguous_commit` 的 docstring），
但它恰好在数据库已经出问题时触发，风险叠加。

### 影响

需要和并发配置一起看才知道有多紧。共用同一个池（`max_size` 默认 8）的线程来源：

| 来源 | 线程数 | 出处 |
| --- | --- | --- |
| partition 执行池 | 4 | `partition_service.py:21` `DEFAULT_PARTITION_MAX_WORKERS` |
| quality 执行池 | 4 | `quality_worker.py:47` `DEFAULT_QUALITY_MAX_WORKERS` |
| ingest 执行池 | 4 | `ingest_worker.py:16` `DEFAULT_INGEST_MAX_WORKERS` |
| 常驻 loop 线程 | 3 | `quality_worker.py:312-314` dispatch/execute/ingest |
| FastAPI 同步路由 | 最多 40 | anyio 默认线程池，未调优 |

8 个线程各持 1 个连接并同时去要第 2 个，就是 30 秒集体挂死后一起抛 `RuntimeError`。

### 解决办法

1. 把 `request_cancel` / `mark_cancelled` 里的 `get_attempt()` 调用移到 `with` 块外。
   `mark_cancelled` 末尾那次（`partition_job_store.py:1496`）已经在块外，可作为参照。
2. `complete_output` 的嵌套保留，但建议加注释说明它有意占用 2 个槽位。
3. 复核 `CUBE_WEB_PG_POOL_SIZE` 默认值 8 是否匹配 12 个工作线程 + 3 个常驻线程的实际并发；
   建议将默认值提到并发线程总数以上，或显式在 `.cube_web.env` 中配置。

---

## 问题 4：建连接时持有全局锁

`for_dsn`（`db_pool.py:96-102`）在持有类级 `_pools_lock` 的同时构造 `_PostgresPool`，而
`__init__`（`db_pool.py:49-51`）会建立 `min_size` 个真实 TCP 连接。数据库慢时，所有线程
（含其他 DSN 的调用方）一起卡在这把全局锁上。

解决办法：把首次建连接移出全局锁——锁内只创建空池对象并登记，`min_size` 预热改为惰性，
或在出锁后用池自身的 `_lock` 完成。

---

## 问题 5：低优先级隐患

- `_pools`（`db_pool.py:39`）只增不减，无 close/evict。DSN 运行时变更会把旧池的连接全部
  漏掉。当前 DSN 统一来自 `runtime_config.postgres_dsn()`，实际不变，因此暂不触发。
- `_release`（`db_pool.py:89-90`）不校验 `_created`，且 `queue.Queue()` 无上界，重复 release
  会让队列涨过 `max_size`。现有调用路径不会这样调，属于缺少防护。

---

## 修复建议顺序

1. 问题 1（致命，默认配置下一次数据库抖动即永久失效）
2. 问题 2 —— 与问题 1 共用 `_discard()` 抽取，一并改动
3. 问题 3 —— 移动两处 `get_attempt()` 调用，并复核池大小配置
4. 问题 4 —— 锁范围收窄
5. 问题 5 —— 视需要补防护

### 回归测试

现有 `cube_web/tests/test_db_pool.py` 需要补充：

- `_new_conn()` 抛异常后 `_created` 归零，数据库恢复后能正常取到连接
- rollback 失败的连接不回到队列，且 `_created` 相应减少
- 修正 `test_pool_timeout_retries_creation_when_capacity_freed` 的手工计数改为真实失败触发

按仓库约定，改动后跑：

```bash
cd cube_web && PYTHONPATH=../cube_encoder:../cube_split:. python3.11 -m pytest tests
```

## 修复记录（2026-08-09）

本次已完成以下修复：

- 连接容量占位与真实建连分离；建连异常会回滚 `_created`，数据库恢复后可重新建连。
- 最小连接数改为首次借用时惰性预热，`_pools_lock` 不再包住网络 I/O。
- rollback、commit 和死连接路径统一关闭并回收坏连接；rollback 失败的连接不会重新入池。
- `request_cancel` 和 `mark_cancelled` 不再在持有连接时嵌套调用 `get_attempt()`。
- `_release` 增加 closed 连接回收；DSN 池注册表仍按当前运行时 DSN 长生命周期保留，未引入未请求的自动淘汰策略。

回归测试覆盖连接创建失败、rollback 失败、惰性预热和嵌套借用修复。

# 双流事件配对服务

`stream_pairing.py` 提供按接收顺序处理左、右两路事件的 Python API。

## 数据模型

```python
from stream_pairing import Event, PairingService, LEFT, RIGHT

service = PairingService()
left = Event(id="l1", key="order-1", minute=10, revision=0)
right = Event(id="r1", key="order-1", minute=12, revision=0)

result = service.add(LEFT, left)
result = service.add(RIGHT, right)
service.current_pairs()
```

- `Event(id, key, minute, revision)`：稳定 ID、业务键、整数分钟和递增修订号。
- `Pair.left / Pair.right`：组成配对的两个事件。
- `Pair.final`：两侧水位均严格越过本侧事件时间后为 `True`。
- 配对身份是 `(left_event.id, right_event.id)`，不包含修订号。
- 同键且 `abs(left.minute - right.minute) <= 3` 即形成配对。

## 操作

```python
service.add(LEFT, event)
service.correct(LEFT, Event("l1", "new-key", 11, revision=1))
service.retract(LEFT, "l1")
service.advance_watermark(LEFT, 20)
```

所有操作都返回 `OperationResult`：

- `accepted`：是否接受。
- `error`：拒绝原因，例如 `EVENT_LATE`、`OLD_REVISION`、`TOO_MANY_ACTIVE_EVENTS`。
- `added` / `retracted`：本次稳定排序后的新增、撤回差量。
- `pairs`：操作后的全部当前配对，按 `(key, left.minute, right.minute, left.id, right.id)` 稳定排序。

更正会先计算完整的旧配对撤回和新配对发布，再一次性更新状态；同一对事件仅修订内容变化时不产生差量，只更新当前配对快照。

## 水位和活动事件

- 每路水位只能增大。
- 事件时间 `<=` 当前路水位时，该事件不得新增、更正或撤回。
- 当两侧水位都严格越过一对中两个事件各自的时间时，该配对标记为最终结果。
- 每路最多保留 500 个活动事件；当两侧水位都保证不会再有可匹配的迟到事件后，旧事件会移出活动集合，但仍保留在有效原始事件快照中，直到显式撤回。

测试和参考重算入口：

```python
service.valid_events(LEFT)
service.recompute_pairs_from_valid_events()
```

运行测试：

```bash
python3 -m unittest -v test_stream_pairing.py
```

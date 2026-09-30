# 双流事件配对 API

`EventJoinService` 按左、右两路接收事件新增、更正、撤回和水位推进，并返回稳定排序的差量与当前配对。

## 语义

- 左右事件只有在 `key` 相同且 `abs(left.minute - right.minute) <= 3` 时配对。
- 配对身份固定为 `(left_id, right_id)`；一个事件可以参与多个满足条件的配对。
- 修订号必须严格递增。
- 每路水位只能前进；事件时间 `<=` 本路水位时，新增、更正和撤回都会被拒绝。
- `correct` 在校验全部通过后才原子替换状态；一次结果同时包含旧配对撤回和新配对新增。
- 当左右水位分别达到一对事件的时间时，该配对标记为 `final=True`，之后不能再因更正或撤回变化。
- 每侧最多保留 500 个活动事件。事件在本侧时间已关闭且对侧水位达到 `time + 3` 后归档；最终配对仍保留在当前结果中。

## 示例

```python
from event_join import EventJoinService, Side

service = EventJoinService()
service.add(Side.LEFT, "l1", "user", 10, 1)
result = service.add(Side.RIGHT, "r1", "user", 12, 1)

result.added       # 新增配对
result.retracted   # 撤回配对
result.pairs       # 稳定排序后的当前全部配对

service.correct(Side.LEFT, "l1", "user", 11, 2)
service.withdraw(Side.RIGHT, "r1")
service.advance_watermark(Side.LEFT, 11)
```

## 测试

```bash
python3 test_event_join.py -v
```

随机测试会在每个成功操作后，用测试端维护的有效原始事件全量重算配对，再核对服务状态、更正跨键、迟到拒绝和最终结果稳定性。

"""双流事件配对服务。

左、右两路事件按到达顺序写入。同键且时间差绝对值不超过 3 分钟的左右事件
组成一对，配对身份由 ``(左事件 ID, 右事件 ID)`` 确定。
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from typing import Any, Iterable


MATCH_WINDOW_MINUTES = 3
DEFAULT_MAX_ACTIVE_EVENTS = 500


class Side(str, Enum):
    LEFT = "left"
    RIGHT = "right"

    @property
    def opposite(self) -> "Side":
        return Side.RIGHT if self is Side.LEFT else Side.LEFT


class EventJoinError(Exception):
    """所有配对服务错误的基类。"""


class DuplicateEventError(EventJoinError):
    """事件 ID 在同一侧已经出现过（包括已撤回的 ID）。"""


class UnknownEventError(EventJoinError):
    """事件不存在，或已经因水位推进而归档。"""


class StaleRevisionError(EventJoinError):
    """修订号不是严格递增，或与指定的当前修订号不一致。"""


class LateEventError(EventJoinError):
    """事件时间不晚于本路水位，不能新增或修改。"""


class InvalidWatermarkError(EventJoinError):
    """水位不是整数，或试图后退。"""


class CapacityError(EventJoinError):
    """活动事件数已达上限，且当前没有可归档事件。"""


@dataclass(frozen=True)
class Event:
    side: Side
    id: Any
    key: Any
    minute: int
    revision: int
    data: Any = None


@dataclass(frozen=True)
class Pair:
    """一对左右事件。

    ``pair_id`` 是稳定身份；``final`` 为 True 时，该配对本身不能再变化。
    """

    left_id: Any
    right_id: Any
    key: Any
    left_minute: int
    right_minute: int
    left_revision: int
    right_revision: int
    final: bool = False

    @property
    def pair_id(self) -> tuple[Any, Any]:
        return (self.left_id, self.right_id)


@dataclass(frozen=True)
class OperationResult:
    added: tuple[Pair, ...]
    retracted: tuple[Pair, ...]
    pairs: tuple[Pair, ...]



def _sort_term(value: Any) -> tuple[int, Any]:
    """为常见 ID/键类型提供全序，避免整数与字符串无法直接比较。"""

    if value is None:
        return (0, 0)
    if isinstance(value, bool):
        return (1, int(value))
    if isinstance(value, int):
        return (1, value)
    if isinstance(value, str):
        return (2, value)
    return (3, repr(value))


def pair_sort_key(pair: Pair) -> tuple[Any, ...]:
    return (
        _sort_term(pair.key),
        pair.left_minute,
        pair.right_minute,
        _sort_term(pair.left_id),
        _sort_term(pair.right_id),
    )


class EventJoinService:
    def __init__(self, max_active_events: int = DEFAULT_MAX_ACTIVE_EVENTS) -> None:
        if max_active_events <= 0:
            raise ValueError("max_active_events must be positive")
        self._max_active_events = max_active_events
        self._watermarks: dict[Side, int | None] = {
            Side.LEFT: None,
            Side.RIGHT: None,
        }
        self._active: dict[Side, dict[Any, Event]] = {
            Side.LEFT: {},
            Side.RIGHT: {},
        }
        self._known: set[tuple[Side, Any]] = set()
        self._pairs: dict[tuple[Any, Any], Pair] = {}
        # pair_id -> 事件 ID，仅索引仍处于活动状态的配对。
        self._index: dict[Side, dict[Any, set[tuple[Any, Any]]]] = {
            Side.LEFT: {},
            Side.RIGHT: {},
        }
        self._archived_final_pairs: dict[tuple[Any, Any], Pair] = {}

    @property
    def max_active_events(self) -> int:
        return self._max_active_events

    def watermark(self, side: Side) -> int | None:
        return self._watermarks[side]

    def active_count(self, side: Side) -> int:
        return len(self._active[side])

    def current_pairs(self) -> tuple[Pair, ...]:
        return self._sorted_pairs(
            list(self._pairs.values()), list(self._archived_final_pairs.values())
        )

    def final_pairs(self) -> tuple[Pair, ...]:
        return tuple(pair for pair in self.current_pairs() if pair.final)

    def add(
        self,
        side: Side,
        event_id: Any,
        key: Any,
        minute: int,
        revision: int,
        data: Any = None,
    ) -> OperationResult:
        """新增一路事件，并发布它与所有现存对侧事件形成的配对。"""

        self._validate_identity(event_id, key)
        self._validate_minute_revision(minute, revision)
        self._validate_writable_time(side, minute)

        qualified_id = (side, event_id)
        if qualified_id in self._known:
            raise DuplicateEventError(f"{side.value} event {event_id!r} already exists")

        # 仅在容量不足时归档；归档是水位驱动的安全操作，不产生差量。
        if self.active_count(side) >= self._max_active_events:
            self._archive_expired()
            if self.active_count(side) >= self._max_active_events:
                raise CapacityError(
                    f"{side.value} side already has {self._max_active_events} active events"
                )

        event = Event(side, event_id, key, minute, revision, data)
        self._active[side][event_id] = event
        self._known.add(qualified_id)
        added = self._match_event(event)
        return self._result(added, [])

    def correct(
        self,
        side: Side,
        event_id: Any,
        new_key: Any,
        new_minute: int,
        new_revision: int,
        data: Any = None,
    ) -> OperationResult:
        """原子更正事件：先撤回旧配对，再发布新配对。"""

        self._validate_identity(event_id, new_key)
        self._validate_minute_revision(new_minute, new_revision)

        event = self._require_active_event(side, event_id)
        self._validate_writable_time(side, event.minute)
        self._validate_writable_time(side, new_minute)
        if new_revision <= event.revision:
            raise StaleRevisionError(
                f"new revision {new_revision} must be greater than {event.revision}"
            )

        # 至此所有校验已完成。下面的状态替换不在调用方观察到的任何中途点返回，
        # 因而不会暴露“旧配对已撤回、新配对尚未发布”的半套结果。
        old_pair_ids = list(self._index[side].get(event_id, ()))
        old_pairs = [self._pairs[pid] for pid in old_pair_ids if pid in self._pairs]
        for pair in old_pairs:
            self._remove_pair(pair)

        corrected = replace(
            event,
            key=new_key,
            minute=new_minute,
            revision=new_revision,
            data=data,
        )
        self._active[side][event_id] = corrected
        new_pairs = self._match_event(corrected)

        return self._result(new_pairs, old_pairs)

    def withdraw(
        self, side: Side, event_id: Any, expected_revision: int | None = None
    ) -> OperationResult:
        """撤回事件，并原子撤回它参与的所有配对。"""

        event = self._require_active_event(side, event_id)
        self._validate_writable_time(side, event.minute)
        if expected_revision is not None and expected_revision != event.revision:
            raise StaleRevisionError(
                f"expected revision {event.revision}, got {expected_revision}"
            )

        pair_ids = list(self._index[side].get(event_id, ()))
        retracted = [self._pairs[pid] for pid in pair_ids if pid in self._pairs]
        for pair in retracted:
            self._remove_pair(pair)

        self._index[side].pop(event_id, None)
        self._active[side].pop(event_id, None)
        # 合格身份仍保留在 _known 中，因此稳定 ID 不能被重新新增。
        return self._result([], retracted)

    def advance_watermark(self, side: Side, new_watermark: int) -> OperationResult:
        """推进单路水位，标记最终配对并归档不可能再配对的活动事件。"""

        if isinstance(new_watermark, bool) or not isinstance(new_watermark, int):
            raise InvalidWatermarkError("watermark must be an integer minute")

        current = self._watermarks[side]
        if current is not None and new_watermark < current:
            raise InvalidWatermarkError(
                f"watermark cannot move backwards: {current} -> {new_watermark}"
            )

        if current != new_watermark:
            self._watermarks[side] = new_watermark
            self._mark_final_pairs()
            self._archive_expired()

        return self._result([], [])

    def get_pair(self, left_id: Any, right_id: Any) -> Pair | None:
        pair_id = (left_id, right_id)
        pair = self._pairs.get(pair_id)
        return pair if pair is not None else self._archived_final_pairs.get(pair_id)

    @staticmethod
    def _validate_identity(event_id: Any, key: Any) -> None:
        try:
            hash(event_id)
            hash(key)
        except TypeError as exc:
            raise ValueError("event id and key must be hashable") from exc

    @staticmethod
    def _validate_minute_revision(minute: int, revision: int) -> None:
        if isinstance(minute, bool) or not isinstance(minute, int):
            raise ValueError("minute must be an integer")
        if isinstance(revision, bool) or not isinstance(revision, int):
            raise ValueError("revision must be an integer")

    def _validate_writable_time(self, side: Side, minute: int) -> None:
        watermark = self._watermarks[side]
        if watermark is not None and minute <= watermark:
            raise LateEventError(
                f"{side.value} event at minute {minute} is not later than watermark "
                f"{watermark}"
            )

    def _require_active_event(self, side: Side, event_id: Any) -> Event:
        try:
            return self._active[side][event_id]
        except KeyError:
            raise UnknownEventError(
                f"{side.value} event {event_id!r} is not active"
            ) from None

    def _make_pair(self, left: Event, right: Event, final: bool = False) -> Pair:
        return Pair(
            left_id=left.id,
            right_id=right.id,
            key=left.key,
            left_minute=left.minute,
            right_minute=right.minute,
            left_revision=left.revision,
            right_revision=right.revision,
            final=final,
        )

    def _match_event(self, event: Event) -> list[Pair]:
        added: list[Pair] = []
        opposite = event.side.opposite
        for candidate in self._active[opposite].values():
            if candidate.key != event.key:
                continue
            if abs(event.minute - candidate.minute) > MATCH_WINDOW_MINUTES:
                continue

            if event.side is Side.LEFT:
                left, right = event, candidate
            else:
                left, right = candidate, event

            pair = self._make_pair(left, right)
            self._pairs[pair.pair_id] = pair
            self._index[Side.LEFT].setdefault(left.id, set()).add(pair.pair_id)
            self._index[Side.RIGHT].setdefault(right.id, set()).add(pair.pair_id)
            added.append(pair)
        return added

    def _remove_pair(self, pair: Pair) -> None:
        pair_id = pair.pair_id
        self._pairs.pop(pair_id, None)
        self._index[Side.LEFT].get(pair.left_id, set()).discard(pair_id)
        self._index[Side.RIGHT].get(pair.right_id, set()).discard(pair_id)

    def _mark_final_pairs(self) -> None:
        left_wm = self._watermarks[Side.LEFT]
        right_wm = self._watermarks[Side.RIGHT]
        if left_wm is None or right_wm is None:
            return

        for pair in list(self._pairs.values()):
            if pair.final:
                continue
            if pair.left_minute <= left_wm and pair.right_minute <= right_wm:
                finalized = replace(pair, final=True)
                self._pairs[pair.pair_id] = finalized

    def _archive_expired(self) -> None:
        """删除两路水位之后不可能再产生连接的活动事件。"""

        left_wm = self._watermarks[Side.LEFT]
        right_wm = self._watermarks[Side.RIGHT]
        if left_wm is None or right_wm is None:
            return

        for side, side_wm, opposite_wm in (
            (Side.LEFT, left_wm, right_wm),
            (Side.RIGHT, right_wm, left_wm),
        ):
            opposite = side.opposite
            expired = [
                event
                for event in self._active[side].values()
                if event.minute <= side_wm
                and event.minute + MATCH_WINDOW_MINUTES <= opposite_wm
            ]

            for event in expired:
                # 归档第二批事件时，其配对可能已随第一批事件移走。
                if event.id not in self._active[side]:
                    continue

                related = set(self._index[side].pop(event.id, set()))
                for pair_id in related:
                    pair = self._pairs.pop(pair_id, None)
                    if pair is None:
                        continue

                    if side is Side.LEFT:
                        other_id = pair.right_id
                    else:
                        other_id = pair.left_id
                    self._index[opposite].get(other_id, set()).discard(pair_id)

                    finalized = pair
                    if not finalized.final:
                        finalized = replace(finalized, final=True)
                    self._archived_final_pairs[pair_id] = finalized

                self._active[side].pop(event.id, None)

    @staticmethod
    def _sorted_pairs(*groups: Iterable[Pair]) -> tuple[Pair, ...]:
        pairs: list[Pair] = []
        for group in groups:
            pairs.extend(group)
        pairs.sort(key=pair_sort_key)
        return tuple(pairs)

    def _result(
        self, added: Iterable[Pair], retracted: Iterable[Pair]
    ) -> OperationResult:
        return OperationResult(
            added=tuple(sorted(added, key=pair_sort_key)),
            retracted=tuple(sorted(retracted, key=pair_sort_key)),
            pairs=self._sorted_pairs(
                self._pairs.values(), self._archived_final_pairs.values()
            ),
        )

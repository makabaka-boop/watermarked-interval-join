"""Ordered two-sided event pairing service.

The service accepts additions, corrections, retractions and watermark
advances for the left and right sides.  A pair is materialized for every two
same-key events on opposite sides whose integer minutes are at most three
minutes apart.  Pair identity is the stable pair of event IDs, so a correction
that changes key/time can atomically withdraw old identities and add new ones.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set, Tuple

__all__ = [
    "LEFT",
    "RIGHT",
    "MAX_ACTIVE_EVENTS",
    "Event",
    "Pair",
    "OperationResult",
    "PairingService",
]

LEFT = "left"
RIGHT = "right"
MAX_ACTIVE_EVENTS = 500
_TOLERANCE_MINUTES = 3

PairKey = Tuple[object, object]


@dataclass(frozen=True)
class Event:
    """A versioned event on one side of the stream."""

    id: object
    key: object
    minute: int
    revision: int

    def __post_init__(self) -> None:
        if not isinstance(self.minute, int) or isinstance(self.minute, bool):
            raise TypeError("minute must be an integer")
        if not isinstance(self.revision, int) or isinstance(self.revision, bool):
            raise TypeError("revision must be an integer")
        if self.revision < 0:
            raise ValueError("revision must be non-negative")


@dataclass(frozen=True)
class Pair:
    """A left/right event match.

    ``final`` is part of the returned snapshot, not pair identity.  It becomes
    true only when both side watermarks are strictly later than the timestamp
    of the respective event in this pair.
    """

    left: Event
    right: Event
    final: bool = False

    @property
    def key(self) -> object:
        return self.left.key

    @property
    def identity(self) -> PairKey:
        return self.left.id, self.right.id


@dataclass(frozen=True)
class OperationResult:
    """Result of one accepted or rejected operation."""

    accepted: bool
    added: Tuple[Pair, ...] = ()
    retracted: Tuple[Pair, ...] = ()
    pairs: Tuple[Pair, ...] = ()
    error: Optional[str] = None

    @property
    def changed(self) -> bool:
        return bool(self.added or self.retracted)


def pair_sort_key(pair: Pair) -> Tuple[object, int, int, object, object]:
    return (
        pair.key,
        pair.left.minute,
        pair.right.minute,
        pair.left.id,
        pair.right.id,
    )


def _other(side: str) -> str:
    if side == LEFT:
        return RIGHT
    if side == RIGHT:
        return LEFT
    raise ValueError("side must be LEFT or RIGHT")


class PairingService:
    """Process left/right operations in their received order."""

    def __init__(self, max_active_events: int = MAX_ACTIVE_EVENTS) -> None:
        self.max_active_events = max_active_events
        self._active: Dict[str, Dict[object, Event]] = {
            LEFT: {},
            RIGHT: {},
        }
        # Events that can no longer be modified, but are retained so that the
        # complete valid input can be reconstructed.  A tombstone means an ID
        # was explicitly retracted and may never be reused.
        self._sealed: Dict[str, Dict[object, Event]] = {LEFT: {}, RIGHT: {}}
        self._tombstones: Dict[str, Set[object]] = {LEFT: set(), RIGHT: set()}

        self._watermarks: Dict[str, Optional[int]] = {LEFT: None, RIGHT: None}

        self._pairs: Dict[PairKey, Pair] = {}
        self._open_pairs: Set[PairKey] = set()
        self._pairs_by_event: Dict[object, Set[PairKey]] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add(self, side: str, event: Event) -> OperationResult:
        """Add a new event."""
        error = self._validate_side_and_event(side, event)
        if error is None and event.id in self._tombstones[side]:
            error = "EVENT_RETRACTED"
        elif error is None and event.id in self._sealed[side]:
            error = "EVENT_LOCKED"
        elif error is None and event.id in self._active[side]:
            error = "EVENT_EXISTS"
        elif error is None and self._is_locked(side, event.minute):
            error = "EVENT_LATE"
        elif error is None and len(self._active[side]) >= self.max_active_events:
            error = "TOO_MANY_ACTIVE_EVENTS"
        if error is not None:
            return self._rejected(error)

        self._active[side][event.id] = event
        added = self._connect_event(side, event)
        return self._accepted(added=added)

    def correct(self, side: str, event: Event) -> OperationResult:
        """Correct an existing event atomically."""
        error = self._validate_side_and_event(side, event)
        old = self._active[side].get(event.id) if error is None else None
        if error is None and event.id in self._tombstones[side]:
            error = "EVENT_RETRACTED"
        elif error is None and event.id in self._sealed[side]:
            error = "EVENT_LOCKED"
        elif error is None and old is None:
            error = "EVENT_NOT_FOUND"
        elif error is None and self._is_locked(side, old.minute):
            error = "EVENT_LATE"
        elif error is None and self._is_locked(side, event.minute):
            error = "EVENT_LATE"
        elif event.revision <= old.revision:
            error = "OLD_REVISION"
        if error is not None:
            return self._rejected(error)

        added, retracted = self._replace_event(side, old, event)
        return self._accepted(added=added, retracted=retracted)

    def retract(
        self, side: str, event_id: object, revision: Optional[int] = None
    ) -> OperationResult:
        """Retract an active event and every pair containing it."""
        if side not in (LEFT, RIGHT):
            return self._rejected("INVALID_SIDE")
        if revision is not None:
            if not isinstance(revision, int) or isinstance(revision, bool):
                return self._rejected("INVALID_REVISION")
            if revision < 0:
                return self._rejected("INVALID_REVISION")

        if event_id in self._tombstones[side]:
            return self._rejected("EVENT_RETRACTED")
        if event_id in self._sealed[side]:
            return self._rejected("EVENT_LOCKED")

        event = self._active[side].get(event_id)
        if event is None:
            return self._rejected("EVENT_NOT_FOUND")
        if self._is_locked(side, event.minute):
            return self._rejected("EVENT_LATE")
        if revision is not None and revision <= event.revision:
            return self._rejected("OLD_REVISION")

        retracted = self._disconnect_event(event_id)
        del self._active[side][event_id]
        self._tombstones[side].add(event_id)
        return self._accepted(retracted=retracted)

    def advance_watermark(self, side: str, minute: int) -> OperationResult:
        """Advance one side's monotonically increasing watermark."""
        if side not in (LEFT, RIGHT):
            return self._rejected("INVALID_SIDE")
        if not isinstance(minute, int) or isinstance(minute, bool):
            return self._rejected("INVALID_MINUTE")

        current = self._watermarks[side]
        if current is not None and minute <= current:
            return self._rejected("WATERMARK_NOT_ADVANCED")

        self._watermarks[side] = minute
        self._finalize_pairs()
        self._seal_eligible_events(LEFT)
        self._seal_eligible_events(RIGHT)
        return self._accepted()

    def current_pairs(self) -> Tuple[Pair, ...]:
        """Return all current pairs in stable order."""
        return self._sorted_pairs(self._pairs.values())

    def valid_events(self, side: str) -> Tuple[Event, ...]:
        """Return all active and sealed events that have not been retracted."""
        self._require_side(side)
        events = list(self._active[side].values()) + list(
            self._sealed[side].values()
        )
        return tuple(sorted(events, key=lambda event: (event.minute, event.id, event.key)))

    def active_count(self, side: str) -> int:
        self._require_side(side)
        return len(self._active[side])

    def watermark(self, side: str) -> Optional[int]:
        self._require_side(side)
        return self._watermarks[side]

    def recompute_pairs_from_valid_events(self) -> Tuple[Pair, ...]:
        """Reference recomputation from the complete valid raw events."""
        pairs: List[Pair] = []
        for left in self.valid_events(LEFT):
            for right in self.valid_events(RIGHT):
                if left.key == right.key and abs(left.minute - right.minute) <= _TOLERANCE_MINUTES:
                    pairs.append(
                        Pair(
                            left=left,
                            right=right,
                            final=self._is_pair_final(left.minute, right.minute),
                        )
                    )
        return self._sorted_pairs(pairs)

    # ------------------------------------------------------------------
    # Internal pairing
    # ------------------------------------------------------------------

    def _connect_event(self, side: str, event: Event) -> Tuple[Pair, ...]:
        added: List[Pair] = []
        opposite = _other(side)

        # A sealed opposite event cannot still be matchable: an event is
        # sealed only after the other side has passed its latest possible
        # matching minute.  Scanning active events is therefore complete.
        for other in self._active[opposite].values():
            pair = self._make_pair(side, event, other)
            if pair is not None:
                self._add_pair(pair)
                added.append(pair)

        return self._sorted_pairs(added)

    def _replace_event(
        self, side: str, old: Event, new: Event
    ) -> Tuple[Tuple[Pair, ...], Tuple[Pair, ...]]:
        opposite = _other(side)
        old_keys = set(self._pairs_by_event.get(new.id, ()))

        new_by_partner: Dict[object, Pair] = {}
        for other in self._active[opposite].values():
            pair = self._make_pair(side, new, other)
            if pair is not None:
                new_by_partner[other.id] = pair

        new_keys = {pair.identity for pair in new_by_partner.values()}
        added_keys = new_keys - old_keys
        removed_keys = old_keys - new_keys

        retracted = tuple(self._pairs[key] for key in removed_keys)
        added = tuple(new_by_partner[self._partner_id(key, opposite)] for key in added_keys)

        # All state changes happen here, after the replacement has been fully
        # computed.  Retained identities receive their corrected snapshot.
        self._active[side][new.id] = new
        for key in removed_keys:
            self._remove_pair(key)
        for key in added_keys:
            self._add_pair(new_by_partner[self._partner_id(key, opposite)])
        for pair in new_by_partner.values():
            if pair.identity in old_keys:
                self._pairs[pair.identity] = pair

        return self._sorted_pairs(added), self._sorted_pairs(retracted)

    def _disconnect_event(self, event_id: object) -> Tuple[Pair, ...]:
        keys = set(self._pairs_by_event.get(event_id, ()))
        retracted = tuple(self._pairs[key] for key in keys)
        for key in keys:
            self._remove_pair(key)
        self._pairs_by_event.pop(event_id, None)
        return self._sorted_pairs(retracted)

    @staticmethod
    def _make_pair(side: str, event: Event, other: Event) -> Optional[Pair]:
        if event.key != other.key:
            return None
        if abs(event.minute - other.minute) > _TOLERANCE_MINUTES:
            return None
        if side == LEFT:
            return Pair(left=event, right=other)
        return Pair(left=other, right=event)

    def _add_pair(self, pair: Pair) -> None:
        key = pair.identity
        self._pairs[key] = pair
        self._open_pairs.add(key)
        for event_id in (pair.left.id, pair.right.id):
            self._pairs_by_event.setdefault(event_id, set()).add(key)

    def _remove_pair(self, key: PairKey) -> None:
        pair = self._pairs.pop(key)
        self._open_pairs.discard(key)
        for event_id in (pair.left.id, pair.right.id):
            keys = self._pairs_by_event.get(event_id)
            if keys is not None:
                keys.discard(key)
                if not keys:
                    del self._pairs_by_event[event_id]

    @staticmethod
    def _partner_id(key: PairKey, opposite_side: str) -> object:
        left_id, right_id = key
        return right_id if opposite_side == RIGHT else left_id

    # ------------------------------------------------------------------
    # Watermarks, finality and active-event retention
    # ------------------------------------------------------------------

    def _is_locked(self, side: str, minute: int) -> bool:
        watermark = self._watermarks[side]
        return watermark is not None and minute <= watermark

    def _is_pair_final(self, left_minute: int, right_minute: int) -> bool:
        left_wm = self._watermarks[LEFT]
        right_wm = self._watermarks[RIGHT]
        return (
            left_wm is not None
            and right_wm is not None
            and left_wm > left_minute
            and right_wm > right_minute
        )

    def _finalize_pairs(self) -> None:
        for key in list(self._open_pairs):
            pair = self._pairs[key]
            if self._is_pair_final(pair.left.minute, pair.right.minute):
                self._open_pairs.discard(key)
                self._pairs[key] = Pair(
                    left=pair.left,
                    right=pair.right,
                    final=True,
                )

    def _seal_eligible_events(self, side: str) -> None:
        """Move events out of the bounded active set once no match can arrive."""
        opposite = _other(side)
        opposite_wm = self._watermarks[opposite]
        side_wm = self._watermarks[side]
        if side_wm is None or opposite_wm is None:
            return

        for event_id, event in list(self._active[side].items()):
            # This side cannot modify it, and the other side cannot add any
            # event within the three-minute tolerance.
            if event.minute <= side_wm and event.minute + _TOLERANCE_MINUTES <= opposite_wm:
                self._sealed[side][event_id] = event
                del self._active[side][event_id]

    # ------------------------------------------------------------------
    # Result/validation helpers
    # ------------------------------------------------------------------

    def _accepted(
        self,
        added: Iterable[Pair] = (),
        retracted: Iterable[Pair] = (),
    ) -> OperationResult:
        return OperationResult(
            accepted=True,
            added=tuple(added),
            retracted=tuple(retracted),
            pairs=self.current_pairs(),
        )

    def _rejected(self, error: str) -> OperationResult:
        return OperationResult(
            accepted=False,
            pairs=self.current_pairs(),
            error=error,
        )

    @staticmethod
    def _validate_side_and_event(side: str, event: Event) -> Optional[str]:
        if side not in (LEFT, RIGHT):
            return "INVALID_SIDE"
        if not isinstance(event, Event):
            return "INVALID_EVENT"
        return None

    @staticmethod
    def _require_side(side: str) -> None:
        if side not in (LEFT, RIGHT):
            raise ValueError("side must be LEFT or RIGHT")

    def _sorted_pairs(self, pairs: Iterable[Pair]) -> Tuple[Pair, ...]:
        return tuple(
            Pair(
                left=pair.left,
                right=pair.right,
                final=(
                    pair.final
                    if pair.final
                    else self._is_pair_final(pair.left.minute, pair.right.minute)
                ),
            )
            for pair in sorted(pairs, key=pair_sort_key)
        )

"""EventJoinService 单元测试和基于全量重算的随机属性测试。"""

from __future__ import annotations

import random
import unittest
from dataclasses import replace

from event_join import (
    MATCH_WINDOW_MINUTES,
    CapacityError,
    DuplicateEventError,
    Event,
    EventJoinError,
    EventJoinService,
    LateEventError,
    Pair,
    Side,
    StaleRevisionError,
    UnknownEventError,
    pair_sort_key,
)

L, R = Side.LEFT, Side.RIGHT


def pid(pair: Pair) -> tuple[object, object]:
    return pair.pair_id


def recalculate_pairs(events: dict[Side, dict[object, Event]]) -> list[Pair]:
    pairs = []
    for left in events[L].values():
        for right in events[R].values():
            if (
                left.key == right.key
                and abs(left.minute - right.minute) <= MATCH_WINDOW_MINUTES
            ):
                pairs.append(
                    Pair(
                        left_id=left.id,
                        right_id=right.id,
                        key=left.key,
                        left_minute=left.minute,
                        right_minute=right.minute,
                        left_revision=left.revision,
                        right_revision=right.revision,
                    )
                )
    return sorted(pairs, key=pair_sort_key)


class EventJoinServiceTest(unittest.TestCase):
    def test_basic_pair_and_stable_order(self) -> None:
        svc = EventJoinService()
        first = svc.add(R, "r2", "k", 8, 1)
        self.assertEqual(first.added, ())

        second = svc.add(L, "l1", "k", 10, 2)
        self.assertEqual([pid(p) for p in second.added], [("l1", "r2")])

        # 不同键、超过 3 分钟均不配对。
        svc.add(L, "l2", "other", 10, 1)
        result = svc.add(R, "r1", "k", 5, 1)
        self.assertEqual(result.added, ())

        # 一个事件可与多个符合条件的对侧事件配对。
        result = svc.add(R, "r3", "k", 12, 1)
        self.assertEqual([pid(p) for p in result.added], [("l1", "r3")])
        self.assertEqual(
            [pid(p) for p in result.pairs], [("l1", "r2"), ("l1", "r3")]
        )

    def test_correction_is_atomic_and_supports_cross_key(self) -> None:
        svc = EventJoinService()
        svc.add(L, "l1", "old", 10, 1)
        svc.add(R, "r-old", "old", 11, 1)
        svc.add(R, "r-new", "new", 20, 1)

        result = svc.correct(L, "l1", "new", 19, 2)

        self.assertEqual([pid(p) for p in result.retracted], [("l1", "r-old")])
        self.assertEqual([pid(p) for p in result.added], [("l1", "r-new")])
        self.assertEqual([pid(p) for p in result.pairs], [("l1", "r-new")])

        # 返回值是同一个原子结果；两个差量列表都在状态替换完成后生成。
        self.assertEqual(result.retracted[0].key, "old")
        self.assertEqual(result.added[0].key, "new")

    def test_correction_replaces_pair_version_even_when_identity_is_unchanged(
        self,
    ) -> None:
        svc = EventJoinService()
        svc.add(L, "l1", "k", 10, 1)
        svc.add(R, "r1", "k", 10, 1)

        result = svc.correct(L, "l1", "k", 10, 2)

        self.assertEqual(len(result.retracted), 1)
        self.assertEqual(len(result.added), 1)
        self.assertEqual(pid(result.retracted[0]), ("l1", "r1"))
        self.assertEqual(pid(result.added[0]), ("l1", "r1"))
        self.assertEqual(result.retracted[0].left_revision, 1)
        self.assertEqual(result.added[0].left_revision, 2)
        self.assertEqual(len(result.pairs), 1)
        self.assertEqual(result.pairs[0].left_revision, 2)

    def test_stale_revision_and_watermark_rejections(self) -> None:
        svc = EventJoinService()
        svc.add(L, "l1", "k", 10, 1)

        with self.assertRaises(StaleRevisionError):
            svc.correct(L, "l1", "k", 11, 1)

        svc.advance_watermark(L, 10)

        with self.assertRaises(LateEventError):
            svc.add(L, "late", "k", 10, 1)
        with self.assertRaises(LateEventError):
            svc.add(L, "late", "k", 9, 1)
        with self.assertRaises(LateEventError):
            svc.correct(L, "l1", "k", 10, 2)

        # 更严格地说，时间必须严格晚于水位。
        with self.assertRaises(LateEventError):
            svc.add(L, "at-watermark", "k", 10, 1)
        svc.add(L, "future", "k", 11, 1)

    def test_watermarks_are_monotonic_and_finalize_pairs(self) -> None:
        svc = EventJoinService()
        svc.add(L, "l1", "k", 10, 1)
        svc.add(R, "r1", "k", 12, 1)

        svc.advance_watermark(L, 10)
        pair = svc.get_pair("l1", "r1")
        self.assertIsNotNone(pair)
        self.assertFalse(pair.final)

        svc.advance_watermark(R, 12)
        pair = svc.get_pair("l1", "r1")
        self.assertIsNotNone(pair)
        self.assertTrue(pair.final)
        self.assertEqual(svc.final_pairs(), (pair,))

        with self.assertRaises(EventJoinError):
            svc.advance_watermark(R, 11)

        # 最终结果不再因更正、撤回而改变：水位拒绝这些修改。
        with self.assertRaises(LateEventError):
            svc.correct(R, "r1", "other", 20, 2)
        with self.assertRaises(LateEventError):
            svc.withdraw(R, "r1")
        self.assertEqual(svc.get_pair("l1", "r1"), pair)

    def test_withdraw_removes_all_related_pairs(self) -> None:
        svc = EventJoinService()
        svc.add(L, "l1", "k", 10, 1)
        svc.add(R, "r1", "k", 10, 1)
        svc.add(R, "r2", "k", 12, 1)

        result = svc.withdraw(L, "l1", expected_revision=1)
        self.assertEqual(
            [pid(p) for p in result.retracted], [("l1", "r1"), ("l1", "r2")]
        )
        self.assertEqual(result.pairs, ())

        with self.assertRaises(UnknownEventError):
            svc.withdraw(L, "l1")

        with self.assertRaises(DuplicateEventError):
            svc.add(L, "l1", "k", 10, 1)

    def test_watermark_archives_unmatchable_events_and_preserves_final_pairs(
        self,
    ) -> None:
        svc = EventJoinService()
        svc.add(L, "l1", "k", 10, 1)
        svc.add(R, "r1", "k", 12, 1)

        svc.advance_watermark(L, 100)
        self.assertEqual(svc.active_count(L), 1)
        self.assertEqual(svc.active_count(R), 1)

        svc.advance_watermark(R, 100)
        self.assertEqual(svc.active_count(L), 0)
        self.assertEqual(svc.active_count(R), 0)

        pair = svc.get_pair("l1", "r1")
        self.assertIsNotNone(pair)
        self.assertTrue(pair.final)
        self.assertEqual(svc.current_pairs(), (pair,))

        with self.assertRaises(UnknownEventError):
            svc.correct(L, "l1", "k", 11, 2)

    def test_active_event_capacity(self) -> None:
        svc = EventJoinService(max_active_events=3)
        svc.add(L, 1, "k", 1, 1)
        svc.add(L, 2, "k", 2, 1)
        svc.add(L, 3, "k", 3, 1)

        with self.assertRaises(CapacityError):
            svc.add(L, 4, "k", 4, 1)

        # 仅推进对侧水位时，本侧事件还不能归档。
        svc.advance_watermark(R, 4)
        with self.assertRaises(CapacityError):
            svc.add(L, 4, "k", 4, 1)

        # 本侧水位关闭事件 1，且对侧已越过 1+3，事件 1 可安全归档。
        svc.advance_watermark(L, 1)
        svc.add(L, 4, "k", 5, 1)
        self.assertEqual(svc.active_count(L), 3)
        self.assertEqual(EventJoinService().max_active_events, 500)

    def test_invalid_arguments(self) -> None:
        svc = EventJoinService()
        with self.assertRaises(ValueError):
            svc.add(L, [1], "k", 1, 1)
        with self.assertRaises(ValueError):
            svc.add(L, 1, "k", 1.5, 1)
        with self.assertRaises(ValueError):
            EventJoinService(max_active_events=0)
        with self.assertRaises(EventJoinError):
            svc.advance_watermark(L, "10")  # type: ignore[arg-type]

    def test_reference_recalculation_after_every_accepted_operation(self) -> None:
        """随机操作流：测试端保留有效原始事件，每次接受操作后全量重算。"""

        rng = random.Random(73019)
        svc = EventJoinService(max_active_events=80)
        active: dict[Side, dict[object, Event]] = {L: {}, R: {}}
        watermarks = {L: None, R: None}
        known: set[tuple[Side, object]] = set()
        finalized: dict[tuple[object, object], Pair] = {}
        archived_final: dict[tuple[object, object], Pair] = {}
        next_id = 1
        operations = 0

        def active_pair_map() -> dict[tuple[object, object], Pair]:
            return {p.pair_id: p for p in recalculate_pairs(active)}

        def archive_side(side: Side, side_wm: int, opp_wm: int) -> None:
            for event in list(active[side].values()):
                if (
                    event.minute <= side_wm
                    and event.minute + MATCH_WINDOW_MINUTES <= opp_wm
                ):
                    active[side].pop(event.id)

        def map_diff(
            old: dict[tuple[object, object], Pair],
            new: dict[tuple[object, object], Pair],
        ) -> set[tuple[object, object]]:
            changed = set(new) - set(old)
            changed.update(pair_id for pair_id in set(old) & set(new) if old[pair_id] != new[pair_id])
            return changed

        def assert_snapshot() -> None:
            expected_active = active_pair_map()
            expected_current: dict[tuple[object, object], Pair] = {}

            for pair_id, pair in list(finalized.items()):
                left_exists = pair.left_id in active[L]
                right_exists = pair.right_id in active[R]
                if left_exists or right_exists:
                    current = expected_active.get(pair_id)
                    self.assertIsNotNone(current)
                    expected_current[pair_id] = replace(current, final=True)
                else:
                    archived_final[pair_id] = pair

            expected_current.update(archived_final)
            for pair_id, pair in expected_active.items():
                expected_current.setdefault(pair_id, pair)

            actual = {p.pair_id: p for p in svc.current_pairs()}
            self.assertEqual(actual, expected_current)

            for pair in expected_active.values():
                if (
                    watermarks[L] is not None
                    and watermarks[R] is not None
                    and pair.left_minute <= watermarks[L]
                    and pair.right_minute <= watermarks[R]
                ):
                    actual_pair = svc.get_pair(pair.left_id, pair.right_id)
                    self.assertIsNotNone(actual_pair)
                    self.assertTrue(actual_pair.final)

        def mark_expected_final() -> None:
            if watermarks[L] is None or watermarks[R] is None:
                return
            for pair in active_pair_map().values():
                if (
                    pair.left_minute <= watermarks[L]
                    and pair.right_minute <= watermarks[R]
                ):
                    finalized[pair.pair_id] = replace(pair, final=True)

        assert_snapshot()

        while operations < 700:
            side = rng.choice([L, R])
            op = rng.randrange(7)

            if op in (0, 1):
                event_id = next_id
                next_id += 1
                minute = rng.randrange(0, 40)
                key = rng.randrange(1, 7)
                before = active_pair_map()

                try:
                    result = svc.add(side, event_id, key, minute, 1)
                except EventJoinError as exc:
                    self.assertIsInstance(exc, (LateEventError, CapacityError))
                    self.assertNotIn((side, event_id), known)
                else:
                    event = Event(side, event_id, key, minute, 1)
                    active[side][event_id] = event
                    known.add((side, event_id))
                    after = active_pair_map()
                    self.assertEqual(
                        {p.pair_id for p in result.added}, set(after) - set(before)
                    )
                    self.assertEqual(result.retracted, ())
                    assert_snapshot()
                    operations += 1

            elif op in (2, 3) and active[side]:
                event = rng.choice(list(active[side].values()))
                old_revision = event.revision
                new_revision = old_revision + rng.choice([0, 1])
                new_key = rng.choice([event.key, rng.randrange(1, 7)])
                new_minute = rng.randrange(0, 40)
                before = active_pair_map()

                try:
                    result = svc.correct(
                        side, event.id, new_key, new_minute, new_revision
                    )
                except EventJoinError as exc:
                    wm = watermarks[side]
                    if wm is not None and (
                        event.minute <= wm or new_minute <= wm
                    ):
                        self.assertIsInstance(exc, LateEventError)
                    else:
                        self.assertIsInstance(exc, StaleRevisionError)
                        self.assertLessEqual(new_revision, old_revision)
                    self.assertEqual(active[side][event.id], event)
                else:
                    corrected = replace(
                        event,
                        key=new_key,
                        minute=new_minute,
                        revision=new_revision,
                    )
                    active[side][event.id] = corrected
                    after = active_pair_map()

                    retracted_ids = map_diff(after, before)
                    added_ids = map_diff(before, after)
                    self.assertEqual(
                        {p.pair_id for p in result.retracted}, retracted_ids
                    )
                    self.assertEqual({p.pair_id for p in result.added}, added_ids)
                    assert_snapshot()
                    operations += 1

            elif op == 4 and active[side]:
                event = rng.choice(list(active[side].values()))
                before = active_pair_map()

                try:
                    result = svc.withdraw(side, event.id)
                except LateEventError:
                    self.assertIsNotNone(watermarks[side])
                    self.assertLessEqual(event.minute, watermarks[side])
                else:
                    active[side].pop(event.id)
                    after = active_pair_map()
                    self.assertEqual(result.added, ())
                    self.assertEqual(
                        {p.pair_id for p in result.retracted},
                        map_diff(after, before),
                    )
                    assert_snapshot()
                    operations += 1

            else:
                current = watermarks[side]
                new_wm = (
                    rng.randrange(0, 35)
                    if current is None
                    else current + rng.choice([-2, 0, 1, 3])
                )

                try:
                    result = svc.advance_watermark(side, new_wm)
                except EventJoinError:
                    self.assertIsNotNone(current)
                    self.assertLess(new_wm, current)
                else:
                    watermarks[side] = new_wm
                    self.assertEqual(result.added, ())
                    self.assertEqual(result.retracted, ())

                    mark_expected_final()
                    opp = side.opposite
                    opp_wm = watermarks[opp]
                    if opp_wm is not None:
                        archive_side(side, new_wm, opp_wm)
                        archive_side(opp, opp_wm, new_wm)
                    assert_snapshot()
                    operations += 1

        self.assertGreaterEqual(operations, 700)


if __name__ == "__main__":
    unittest.main()

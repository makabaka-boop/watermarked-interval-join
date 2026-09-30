import random
import unittest

from stream_pairing import (
    LEFT,
    RIGHT,
    Event,
    Pair,
    PairingService,
    pair_sort_key,
)


def reference_pairs(
    service: PairingService,
    left_events: list[Event],
    right_events: list[Event],
) -> tuple[Pair, ...]:
    """Calculate pairs from caller-owned raw events."""
    pairs = []
    for left in left_events:
        for right in right_events:
            if left.key != right.key:
                continue
            if abs(left.minute - right.minute) > 3:
                continue
            final = (
                service.watermark(LEFT) is not None
                and service.watermark(RIGHT) is not None
                and service.watermark(LEFT) > left.minute
                and service.watermark(RIGHT) > right.minute
            )
            pairs.append(Pair(left, right, final))
    return tuple(sorted(pairs, key=pair_sort_key))


def expected_pairs(service: PairingService) -> tuple[Pair, ...]:
    """Independent reference calculation from all valid raw events."""
    return reference_pairs(
        service,
        list(service.valid_events(LEFT)),
        list(service.valid_events(RIGHT)),
    )


class PairingServiceTest(unittest.TestCase):
    def assertStateMatchesReference(self, service: PairingService) -> None:
        self.assertEqual(service.current_pairs(), expected_pairs(service))

    def test_pair_is_added_within_tolerance_and_uses_stable_order(self) -> None:
        service = PairingService()
        service.add(LEFT, Event("l1", "b", 10, 0))
        service.add(LEFT, Event("l0", "a", 10, 0))
        service.add(RIGHT, Event("r2", "a", 13, 0))

        result = service.add(RIGHT, Event("r1", "a", 10, 0))
        self.assertTrue(result.accepted)
        self.assertEqual(
            result.added,
            (Pair(Event("l0", "a", 10, 0), Event("r1", "a", 10, 0), False),),
        )
        self.assertEqual([p.key for p in result.pairs], ["a", "a"])
        self.assertStateMatchesReference(service)

    def test_different_key_or_outside_tolerance_does_not_pair(self) -> None:
        service = PairingService()
        service.add(LEFT, Event("l1", "a", 10, 0))

        rejected_time = service.add(RIGHT, Event("r1", "a", 14, 0))
        self.assertTrue(rejected_time.accepted)
        self.assertEqual(rejected_time.added, ())

        rejected_key = service.add(RIGHT, Event("r2", "b", 10, 0))
        self.assertTrue(rejected_key.accepted)
        self.assertEqual(rejected_key.added, ())
        self.assertEqual(service.current_pairs(), ())

    def test_correction_can_move_event_across_keys_atomically(self) -> None:
        service = PairingService()
        left_a = Event("l-a", "a", 10, 0)
        left_b = Event("l-b", "b", 10, 0)
        right_a = Event("r-a", "a", 10, 0)
        right_b = Event("r-b", "b", 10, 0)
        service.add(LEFT, left_a)
        service.add(LEFT, left_b)
        service.add(RIGHT, right_a)
        service.add(RIGHT, right_b)

        result = service.correct(LEFT, Event("l-a", "b", 10, 1))
        self.assertTrue(result.accepted)

        # Retractions and additions are both present in one returned result;
        # no operation ever exposes only the withdrawal or only the new pair.
        self.assertEqual(result.retracted, (Pair(left_a, right_a),))
        self.assertEqual(
            result.added,
            (Pair(Event("l-a", "b", 10, 1), right_b),),
        )
        self.assertEqual(
            {(p.left.id, p.right.id, p.key) for p in result.pairs},
            {
                ("l-a", "r-b", "b"),
                ("l-b", "r-b", "b"),
            },
        )
        self.assertStateMatchesReference(service)

    def test_correction_same_partner_updates_snapshot_without_delta(self) -> None:
        service = PairingService()
        left = Event("l1", "a", 10, 0)
        right = Event("r1", "a", 11, 0)
        service.add(LEFT, left)
        service.add(RIGHT, right)

        corrected = Event("l1", "a", 12, 1)
        result = service.correct(LEFT, corrected)
        self.assertTrue(result.accepted)
        self.assertEqual(result.added, ())
        self.assertEqual(result.retracted, ())
        self.assertEqual(result.pairs, (Pair(corrected, right),))

    def test_revision_must_increase(self) -> None:
        service = PairingService()
        service.add(LEFT, Event("l1", "a", 10, 1))
        result = service.correct(LEFT, Event("l1", "a", 11, 1))
        self.assertFalse(result.accepted)
        self.assertEqual(result.error, "OLD_REVISION")
        self.assertEqual(service.valid_events(LEFT)[0].revision, 1)
        self.assertStateMatchesReference(service)

    def test_retract_removes_event_and_all_related_pairs(self) -> None:
        service = PairingService()
        left = Event("l1", "a", 10, 0)
        rights = [Event(f"r{i}", "a", 10 + i, 0) for i in range(3)]
        service.add(LEFT, left)
        for right in rights:
            service.add(RIGHT, right)

        result = service.retract(LEFT, "l1")
        self.assertTrue(result.accepted)
        self.assertEqual(len(result.retracted), 3)
        self.assertEqual(result.pairs, ())
        self.assertEqual(service.valid_events(LEFT), ())

        reused = service.add(LEFT, Event("l1", "a", 10, 0))
        self.assertFalse(reused.accepted)
        self.assertEqual(reused.error, "EVENT_RETRACTED")

    def test_watermark_rejects_late_addition_and_modification(self) -> None:
        service = PairingService()
        service.add(LEFT, Event("l1", "a", 10, 0))
        service.advance_watermark(LEFT, 10)

        add_at_minute_10 = service.add(LEFT, Event("late", "a", 10, 0))
        add_before_minute_10 = service.add(LEFT, Event("older", "a", 9, 0))
        self.assertEqual(add_at_minute_10.error, "EVENT_LATE")
        self.assertEqual(add_before_minute_10.error, "EVENT_LATE")

        correction = service.correct(LEFT, Event("l1", "a", 11, 1))
        retraction = service.retract(LEFT, "l1")
        self.assertEqual(correction.error, "EVENT_LATE")
        self.assertEqual(retraction.error, "EVENT_LATE")
        self.assertEqual(service.valid_events(LEFT), (Event("l1", "a", 10, 0),))

        future = service.add(LEFT, Event("future", "a", 11, 0))
        self.assertTrue(future.accepted)

    def test_watermarks_only_advance_forward(self) -> None:
        service = PairingService()
        self.assertTrue(service.advance_watermark(LEFT, 5).accepted)
        self.assertEqual(service.advance_watermark(LEFT, 5).error, "WATERMARK_NOT_ADVANCED")
        self.assertEqual(service.advance_watermark(LEFT, 4).error, "WATERMARK_NOT_ADVANCED")
        self.assertEqual(service.watermark(LEFT), 5)

    def test_pair_becomes_final_only_when_both_watermarks_pass_it(self) -> None:
        service = PairingService()
        service.add(LEFT, Event("l1", "a", 10, 0))
        service.add(RIGHT, Event("r1", "a", 12, 0))

        service.advance_watermark(LEFT, 11)
        self.assertFalse(service.current_pairs()[0].final)

        # Equality does not pass the event: watermark must be strictly later.
        service.advance_watermark(RIGHT, 12)
        self.assertFalse(service.current_pairs()[0].final)

        service.advance_watermark(RIGHT, 13)
        final_pair = service.current_pairs()[0]
        self.assertTrue(final_pair.final)
        self.assertStateMatchesReference(service)

        before = service.current_pairs()
        self.assertEqual(service.advance_watermark(LEFT, 100).added, ())
        self.assertEqual(service.advance_watermark(LEFT, 100).retracted, ())
        self.assertEqual(service.current_pairs(), before)
        self.assertTrue(service.current_pairs()[0].final)

    def test_active_events_are_capped_per_side(self) -> None:
        service = PairingService(max_active_events=500)
        for i in range(500):
            self.assertTrue(service.add(LEFT, Event(i, "k", 100 + i, 0)).accepted)

        self.assertEqual(service.active_count(LEFT), 500)
        self.assertEqual(service.add(LEFT, Event(500, "k", 1000, 0)).error, "TOO_MANY_ACTIVE_EVENTS")

        # The other side has its own bounded active set.
        self.assertTrue(service.add(RIGHT, Event("r", "k", 1000, 0)).accepted)
        self.assertEqual(service.active_count(RIGHT), 1)

    def test_watermarks_release_old_events_from_active_capacity(self) -> None:
        service = PairingService(max_active_events=2)
        service.add(LEFT, Event(1, "a", 10, 0))
        service.add(LEFT, Event(2, "a", 11, 0))

        # Advancing only the left watermark still permits possible future
        # right matches at minute 14 for event 1, so nothing can be released.
        service.advance_watermark(LEFT, 100)
        self.assertEqual(service.active_count(LEFT), 2)

        service.advance_watermark(RIGHT, 100)
        self.assertEqual(service.active_count(LEFT), 0)
        self.assertEqual(len(service.valid_events(LEFT)), 2)
        self.assertTrue(service.add(LEFT, Event(3, "a", 200, 0)).accepted)

    def test_many_to_many_matches_all_within_tolerance(self) -> None:
        service = PairingService()
        for minute in (10, 11):
            service.add(LEFT, Event(f"l{minute}", "a", minute, 0))
        for minute in (11, 12):
            service.add(RIGHT, Event(f"r{minute}", "a", minute, 0))

        self.assertEqual(len(service.current_pairs()), 4)
        self.assertStateMatchesReference(service)

    @staticmethod
    def _active_count(service: PairingService, side: str, raw: dict[str, Event]) -> int:
        opposite = RIGHT if side == LEFT else LEFT
        side_wm = service.watermark(side)
        opposite_wm = service.watermark(opposite)
        count = 0
        for event in raw.values():
            if side_wm is None or opposite_wm is None:
                count += 1
            elif not (event.minute <= side_wm and event.minute + 3 <= opposite_wm):
                count += 1
        return count

    def test_random_sequence_matches_full_recompute(self) -> None:
        random.seed(7321)
        service = PairingService(max_active_events=40)
        raw = {LEFT: {}, RIGHT: {}}
        next_revision = {}

        # Keep the synthetic clock finite and biased near existing events so
        # corrections and retractions are often still mutable.
        for step in range(3000):
            side = random.choice((LEFT, RIGHT))
            kind = random.choices(
                ("add", "correct", "retract", "watermark"),
                weights=(45, 25, 15, 15),
            )[0]

            if kind == "add" or not raw[side]:
                event_id = f"e{side}-{len(next_revision)}"
                event = Event(event_id, f"k{random.randrange(8)}", random.randrange(300), 0)
                next_revision[event_id] = 0
                result = service.add(side, event)
                if result.accepted:
                    raw[side][event_id] = event
                else:
                    next_revision.pop(event_id, None)
            elif kind == "correct":
                event_id = random.choice(list(raw[side]))
                old = raw[side][event_id]
                new = Event(
                    event_id,
                    f"k{random.randrange(8)}",
                    max(0, old.minute + random.randrange(-5, 6)),
                    old.revision + random.choice((1, 1, 2)),
                )
                result = service.correct(side, new)
                if result.accepted:
                    raw[side][event_id] = new
            elif kind == "retract":
                event_id = random.choice(list(raw[side]))
                result = service.retract(side, event_id)
                if result.accepted:
                    del raw[side][event_id]
            else:
                current = service.watermark(side)
                target = (current if current is not None else -1) + random.randrange(1, 8)
                result = service.advance_watermark(side, target)

            self.assertEqual(service.active_count(LEFT), self._active_count(service, LEFT, raw[LEFT]))
            self.assertEqual(service.active_count(RIGHT), self._active_count(service, RIGHT, raw[RIGHT]))
            self.assertEqual(
                service.current_pairs(),
                reference_pairs(service, list(raw[LEFT].values()), list(raw[RIGHT].values())),
            )

            for checked_side in (LEFT, RIGHT):
                self.assertLessEqual(
                    service.active_count(checked_side),
                    service.max_active_events,
                )


if __name__ == "__main__":
    unittest.main()

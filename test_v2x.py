import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from botocore.exceptions import ClientError

import v2x_service
import vehicle_simulator
from rsu import Rsu, SeenIds, create_app as create_rsu_app, validate_event
from v2x_service import AlertEngine, V2XService, percentile_95

TOPIC = "arn:aws:sns:us-east-1:000000000000:v2x-alerts"


def make_event(event_type="normal telemetry", source="veh-001", event_time=None, event_id=None):
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "source_id": source,
        "event_type": event_type,
        "event_time": event_time or "2026-11-09T10:00:00.000Z",
        "location": "segment-A12",
        "payload": {"x": 100, "y": 80, "lane": 1, "speed": 50, "seq": 1, "run_id": "test"},
    }


def conditional_failure():
    return ClientError({"Error": {"Code": "ConditionalCheckFailedException", "Message": "x"}}, "op")


class FakeTable:
    def __init__(self):
        self.items = {}

    def put_item(self, Item, ConditionExpression=None):
        if ConditionExpression and Item["pk"] in self.items:
            raise conditional_failure()
        self.items[Item["pk"]] = Item

    def get_item(self, Key):
        item = self.items.get(Key["pk"])
        return {"Item": item} if item else {}

    def update_item(self, Key, UpdateExpression, ConditionExpression, ExpressionAttributeValues):
        current = self.items.get(Key["pk"])
        event_time = ExpressionAttributeValues[":t"]
        if current and current["event_time"] >= event_time:
            raise conditional_failure()
        self.items[Key["pk"]] = {**Key, "event_time": event_time}


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


@pytest.fixture
def harness():
    class Harness:
        sent, published, confirmed = [], [], []
        clock = Clock()

    h = Harness()
    h.sent, h.published, h.confirmed = [], [], []
    h.rsu = Rsu("rsu-1", h.sent.append, lambda t, b: h.published.append((t, json.loads(b))),
                confirm_subscription=h.confirmed.append, topic_arn=TOPIC, clock=h.clock)
    h.client = create_rsu_app(h.rsu).test_client()
    return h


def sns_post(client, message_type, body):
    return client.post("/sns", data=json.dumps(body), content_type="text/plain; charset=UTF-8",
                       headers={"x-amz-sns-message-type": message_type})


# --- Validación y filtrado en la RSU -----------------------------------------

@pytest.mark.parametrize("change", [
    {"event_type": "unknown"},
    {"source_id": "<script>"},
    {"event_time": "ayer"},
    {"payload": "x"},
])
def test_invalid_events_are_rejected(change):
    assert validate_event({**make_event(), **change}) is not None


def test_missing_field_is_rejected():
    event = make_event()
    del event["location"]
    assert validate_event(event) == "missing location"


def test_malformed_json_is_rejected(harness):
    assert harness.rsu.handle_event(b"{no-json") == "rejected"


def test_duplicate_event_is_dropped(harness):
    event = json.dumps(make_event("congestion"))
    assert harness.rsu.handle_event(event) == "accepted"
    assert harness.rsu.handle_event(event) == "duplicate"
    assert harness.rsu.snapshot()["buffered"] == 1


def test_lru_forgets_oldest():
    seen = SeenIds(capacity=2)
    for key in "abc":
        assert not seen.seen(key)
    assert not seen.seen("a")


def test_normal_telemetry_is_rate_limited_per_vehicle(harness):
    assert harness.rsu.handle_event(json.dumps(make_event())) == "accepted"
    assert harness.rsu.handle_event(json.dumps(make_event())) == "filtered"
    assert harness.rsu.handle_event(json.dumps(make_event(source="veh-002"))) == "accepted"
    harness.clock.now = 3.1
    assert harness.rsu.handle_event(json.dumps(make_event())) == "accepted"


def test_critical_events_are_never_rate_limited(harness):
    for _ in range(3):
        assert harness.rsu.handle_event(json.dumps(make_event("road hazard"))) == "accepted"


def test_sudden_braking_alerts_locally_without_cloud(harness):
    harness.rsu.set_link(False)
    harness.rsu.handle_event(json.dumps(make_event("sudden braking")))
    topic, alert = harness.published[0]
    assert topic == "v2x/rsu-1/alerts"
    assert alert["origin"] == "rsu-1"
    assert harness.sent == []


# --- Buffer y reenvío --------------------------------------------------------

def test_buffer_holds_events_while_link_is_down_and_keeps_order(harness):
    harness.rsu.set_link(False)
    ids = []
    for _ in range(3):
        event = make_event("congestion")
        ids.append(event["event_id"])
        harness.rsu.handle_event(json.dumps(event))
    harness.rsu.forward_once()
    assert harness.sent == []
    harness.rsu.set_link(True)
    for _ in range(3):
        harness.rsu.forward_once()
    assert [json.loads(body)["event_id"] for body in harness.sent] == ids


def test_failed_send_is_retried_with_backoff():
    attempts = []

    def failing(body):
        attempts.append(body)
        if len(attempts) < 3:
            raise RuntimeError("sin red")

    rsu = Rsu("rsu-1", failing, lambda t, b: None)
    rsu.handle_event(json.dumps(make_event("congestion")))
    assert [rsu.forward_once() for _ in range(3)] == [1, 2, 0]
    assert rsu.snapshot()["buffered"] == 0
    assert rsu.snapshot()["retries"] == 2
    assert rsu.backoff == 1


def test_full_buffer_discards_normal_telemetry_first():
    rsu = Rsu("rsu-1", lambda b: None, lambda t, b: None, buffer_size=2)
    rsu.handle_event(json.dumps(make_event()))
    rsu.handle_event(json.dumps(make_event("congestion")))
    rsu.handle_event(json.dumps(make_event("road hazard")))
    assert [e["event_type"] for e in rsu.buffer] == ["congestion", "road hazard"]
    assert rsu.snapshot()["dropped_buffer"] == 1


# --- SNS por HTTP ------------------------------------------------------------

def test_sns_subscription_confirmation_uses_token(harness):
    response = sns_post(harness.client, "SubscriptionConfirmation", {"TopicArn": TOPIC, "Token": "abc"})
    assert response.status_code == 200
    assert harness.confirmed == ["abc"]


def notification(alert):
    return {"TopicArn": TOPIC, "Message": json.dumps(alert)}


def test_sns_notification_in_text_plain_is_relayed_by_mqtt(harness):
    response = sns_post(harness.client, "Notification", notification({"alert_id": "a1", "scope": "all"}))
    assert response.status_code == 200
    assert harness.published == [("v2x/rsu-1/alerts", {"alert_id": "a1", "scope": "all"})]


def test_sns_duplicate_alert_is_relayed_once(harness):
    for _ in range(2):
        assert sns_post(harness.client, "Notification",
                        notification({"alert_id": "a1", "scope": "all"})).status_code == 200
    assert len(harness.published) == 1


def test_sns_alert_for_other_rsu_is_ignored(harness):
    sns_post(harness.client, "Notification", notification({"alert_id": "a2", "scope": "rsu-2"}))
    assert harness.published == []


def test_sns_foreign_topic_is_forbidden(harness):
    body = {"TopicArn": "arn:aws:sns:us-east-1:1:other", "Message": "{}"}
    assert sns_post(harness.client, "Notification", body).status_code == 403


def test_sns_returns_503_when_link_is_down_so_sns_retries(harness):
    harness.client.post("/admin/link", json={"up": False})
    body = notification({"alert_id": "a3", "scope": "all"})
    assert sns_post(harness.client, "Notification", body).status_code == 503
    harness.client.post("/admin/link", json={"up": True})
    assert sns_post(harness.client, "Notification", body).status_code == 200
    assert len(harness.published) == 1


# --- V2X-SERVICE -------------------------------------------------------------

@pytest.fixture
def service():
    published = []
    instance = V2XService(FakeTable(), published.append)
    instance.published = published
    return instance


def test_same_event_twice_is_stored_once(service):
    body = json.dumps(make_event())
    assert service.process_body(body) == "processed"
    assert service.process_body(body, receive_count=2) == "duplicate"
    assert len([k for k in service.table.items if k.startswith("event#")]) == 1
    assert service.metrics["duplicates"] == 1
    assert service.metrics["redelivered"] == 1


def test_malformed_message_raises_so_it_reaches_the_dlq(service):
    with pytest.raises(ValueError):
        service.process_body("{no-json")
    with pytest.raises(ValueError):
        service.process_body(json.dumps({"event_id": "x"}))


def test_older_event_does_not_overwrite_vehicle_state(service):
    service.process_body(json.dumps(make_event(event_time="2026-11-09T10:00:05.000Z")))
    service.process_body(json.dumps(make_event(event_time="2026-11-09T10:00:01.000Z")))
    assert service.table.items["vehicle#veh-001"]["event_time"] == "2026-11-09T10:00:05.000Z"
    assert service.vehicles["veh-001"]["event_time"] == "2026-11-09T10:00:05.000Z"
    assert service.metrics["out_of_order"] == 1


def test_road_hazard_generates_one_alert_for_all_rsus(service):
    body = json.dumps(make_event("road hazard"))
    service.process_body(body)
    service.process_body(body, receive_count=2)
    assert len(service.published) == 1
    alert = json.loads(service.published[0])
    assert alert["scope"] == "all"
    assert alert["origin"] == "cloud"


def test_alert_lost_before_publish_is_recovered_on_redelivery():
    published = []

    def failing_once(body):
        if not hasattr(failing_once, "failed"):
            failing_once.failed = True
            raise RuntimeError("SNS caído")
        published.append(body)

    instance = V2XService(FakeTable(), failing_once)
    body = json.dumps(make_event("road hazard"))
    with pytest.raises(RuntimeError):
        instance.process_body(body)
    assert instance.process_body(body, receive_count=2) == "duplicate"
    assert len(published) == 1


def congestion(source, second=0):
    return make_event("congestion", source, f"2026-11-09T10:00:{second:02d}.000Z")


def test_congestion_needs_three_distinct_vehicles_and_fires_once():
    engine = AlertEngine()
    assert engine.evaluate(congestion("veh-001")) == []
    assert engine.evaluate(congestion("veh-001", 1)) == []
    assert engine.evaluate(congestion("veh-002", 2)) == []
    assert len(engine.evaluate(congestion("veh-003", 3))) == 1
    assert engine.evaluate(congestion("veh-004", 4)) == []


def test_congestion_alert_id_is_deterministic():
    first, second = AlertEngine(congestion_threshold=1), AlertEngine(congestion_threshold=1)
    assert first.evaluate(congestion("veh-001"))[0]["alert_id"] == \
        second.evaluate(congestion("veh-002", 5))[0]["alert_id"]


def test_congestion_in_different_windows_does_not_accumulate():
    engine = AlertEngine()
    assert engine.evaluate(make_event("congestion", "veh-001", "2026-11-09T10:00:00.000Z")) == []
    assert engine.evaluate(make_event("congestion", "veh-002", "2026-11-09T10:00:31.000Z")) == []
    assert engine.evaluate(make_event("congestion", "veh-003", "2026-11-09T10:01:01.000Z")) == []


def test_percentile():
    assert percentile_95([]) is None
    assert percentile_95(list(range(1, 101))) == 96


def test_api_endpoints(service):
    service.process_body(json.dumps(make_event("road hazard")))
    client = v2x_service.create_app(service).test_client()
    assert "veh-001" in client.get("/api/vehicles").get_json()
    assert client.get("/api/alerts").get_json()[0]["alert_type"] == "road hazard"
    assert client.get("/api/metrics").get_json()["processed"] == 1
    assert client.get("/").status_code == 200


# --- Simulador ---------------------------------------------------------------

def test_vehicles_are_split_between_rsus():
    assert [vehicle_simulator.assign_rsu(n) for n in range(1, 6)] == ["rsu-1"] * 3 + ["rsu-2"] * 2


def test_simulated_event_is_accepted_by_rsu_schema():
    state = {"x": 100, "lane": 2, "speed": 20}
    first = vehicle_simulator.make_event("veh-001", state, 1, "run", "road hazard")
    second = vehicle_simulator.make_event("veh-001", state, 1, "run", "road hazard")
    assert validate_event(first) is None
    assert first["event_id"] != second["event_id"]
    parsed = datetime.fromisoformat(first["event_time"].replace("Z", "+00:00"))
    assert abs(datetime.now(timezone.utc) - parsed) < timedelta(seconds=5)


class Args:
    delay_ms = 0
    loss = 0
    duplicate = 0
    reorder = 0


def injector_with(**options):
    args = Args()
    for name, value in options.items():
        setattr(args, name, value)
    output = []
    return vehicle_simulator.Injector(lambda t, b: output.append(b), args, vehicle_simulator.Recorder()), output


def test_injector_loss():
    injector, output = injector_with(loss=1)
    injector.send("t", "a")
    assert output == []


def test_injector_duplicate():
    injector, output = injector_with(duplicate=1)
    injector.send("t", "a")
    assert output == ["a", "a"]


def test_injector_reorder_swaps_consecutive_messages():
    injector, output = injector_with(reorder=1)
    injector.send("t", "a")
    injector.send("t", "b")
    assert output == ["b", "a"]


def test_cumulative_counter_survives_restart():
    counter = vehicle_simulator.CumulativeCounter()
    for value in (5, 8, None, 2, 4):
        counter.update(value)
    assert counter.total == 7

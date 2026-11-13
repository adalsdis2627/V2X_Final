import json
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

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


def congestion(source, second=0, rsu="rsu-1"):
    return {**make_event("congestion", source, f"2026-11-09T10:00:{second:02d}.000Z"), "rsu_id": rsu}


def test_cloud_congestion_needs_three_vehicles_from_two_rsus_and_fires_once():
    engine = AlertEngine()
    assert engine.evaluate(congestion("veh-001")) == []
    assert engine.evaluate(congestion("veh-001", 1)) == []
    assert engine.evaluate(congestion("veh-002", 2)) == []
    alerts = engine.evaluate(congestion("veh-004", 3, rsu="rsu-2"))
    assert len(alerts) == 1
    assert alerts[0]["scope"] == "all"
    assert "rsu-1, rsu-2" in alerts[0]["message"]
    assert engine.evaluate(congestion("veh-005", 4, rsu="rsu-2")) == []


def test_cloud_ignores_congestion_confined_to_one_rsu():
    engine = AlertEngine()
    for second, source in enumerate(["veh-001", "veh-002", "veh-003"]):
        assert engine.evaluate(congestion(source, second)) == []


def test_congestion_alert_id_is_deterministic():
    first = AlertEngine(congestion_threshold=1, min_rsus=1)
    second = AlertEngine(congestion_threshold=1, min_rsus=1)
    assert first.evaluate(congestion("veh-001"))[0]["alert_id"] == \
        second.evaluate(congestion("veh-002", 5))[0]["alert_id"]


def test_congestion_in_different_windows_does_not_accumulate():
    engine = AlertEngine()
    for source, rsu, event_time in [("veh-001", "rsu-1", "10:00:00"), ("veh-004", "rsu-2", "10:00:31"),
                                    ("veh-002", "rsu-1", "10:01:01")]:
        event = {**make_event("congestion", source, f"2026-11-09T{event_time}.000Z"), "rsu_id": rsu}
        assert engine.evaluate(event) == []


# --- Congestión local en la RSU (ventana deslizante) -------------------------

def local_congestion_alerts(harness):
    return [alert for _, alert in harness.published if alert["alert_type"] == "congestion"]


def test_rsu_alerts_congestion_in_its_segment_with_two_vehicles(harness):
    harness.rsu.handle_event(json.dumps(make_event("congestion", "veh-001")))
    assert local_congestion_alerts(harness) == []
    harness.clock.now = 10
    harness.rsu.handle_event(json.dumps(make_event("congestion", "veh-002")))
    alerts = local_congestion_alerts(harness)
    assert len(alerts) == 1
    assert alerts[0]["origin"] == "rsu-1"
    assert alerts[0]["scope"] == "rsu-1"
    assert harness.rsu.snapshot()["local_congestion_alerts"] == 1


def test_rsu_congestion_ignores_repeated_reports_from_one_vehicle(harness):
    for second in range(3):
        harness.clock.now = second
        harness.rsu.handle_event(json.dumps(make_event("congestion", "veh-001")))
    assert local_congestion_alerts(harness) == []


def test_rsu_congestion_window_slides(harness):
    harness.rsu.handle_event(json.dumps(make_event("congestion", "veh-001")))
    harness.clock.now = 31  # El primer aviso ya ha salido de la ventana de 30 s.
    harness.rsu.handle_event(json.dumps(make_event("congestion", "veh-002")))
    assert local_congestion_alerts(harness) == []
    harness.clock.now = 40  # veh-002 (t=31) y veh-003 (t=40) caen dentro de la misma ventana.
    harness.rsu.handle_event(json.dumps(make_event("congestion", "veh-003")))
    assert len(local_congestion_alerts(harness)) == 1


def test_rsu_congestion_realerts_at_most_once_per_window(harness):
    for now, source in [(0, "veh-001"), (1, "veh-002"), (5, "veh-003"), (20, "veh-001")]:
        harness.clock.now = now
        harness.rsu.handle_event(json.dumps(make_event("congestion", source)))
    assert len(local_congestion_alerts(harness)) == 1
    harness.clock.now = 32  # Sigue habiendo congestión y ya ha pasado una ventana desde la alerta.
    harness.rsu.handle_event(json.dumps(make_event("congestion", "veh-002")))
    assert len(local_congestion_alerts(harness)) == 2


def test_congestion_events_are_still_forwarded_to_the_cloud(harness):
    for source in ("veh-001", "veh-002"):
        harness.rsu.handle_event(json.dumps(make_event("congestion", source)))
    assert harness.rsu.snapshot()["buffered"] == 2


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

def test_each_rsu_covers_half_of_the_circuit():
    assert [vehicle_simulator.rsu_for_lane(lane) for lane in range(4)] == ["rsu-1", "rsu-1", "rsu-2", "rsu-2"]


def test_simulated_event_is_accepted_by_rsu_schema():
    state = {"x": 100, "lane": 2, "speed": 20}
    first = vehicle_simulator.make_event("veh-001", state, 1, "run", "road hazard")
    second = vehicle_simulator.make_event("veh-001", state, 1, "run", "road hazard")
    assert validate_event(first) is None
    assert first["event_id"] != second["event_id"]
    parsed = datetime.fromisoformat(first["event_time"].replace("Z", "+00:00"))
    assert abs(datetime.now(timezone.utc) - parsed) < timedelta(seconds=5)


def simulator_args(**options):
    defaults = dict(rate=1.5, critical_rate=0, delay_ms=0, loss=0, duplicate=0, reorder=0, burst=0, burst_period=10)
    return SimpleNamespace(**{**defaults, **options})


def injector_with(**options):
    output = []
    injector = vehicle_simulator.Injector(simulator_args(**options), vehicle_simulator.Recorder())
    return injector, output, lambda topic, body: output.append(body)


def test_injector_loss():
    injector, output, publish = injector_with(loss=1)
    injector.send(publish, "t", "a")
    assert output == []


def test_injector_duplicate():
    injector, output, publish = injector_with(duplicate=1)
    injector.send(publish, "t", "a")
    assert output == ["a", "a"]


def test_injector_reorder_swaps_consecutive_messages():
    injector, output, publish = injector_with(reorder=1)
    injector.send(publish, "t", "a")
    injector.send(publish, "t", "b")
    assert output == ["b", "a"]


# --- Cambio de RSU (handover) ------------------------------------------------

class FakeMqttClient:
    def __init__(self, client_id):
        self.client_id = client_id
        self.published, self.subscriptions = [], []
        self.endpoint, self.closed = None, False

    def connect(self, host, port):
        self.endpoint = (host, port)

    def loop_start(self):
        self.on_connect(self, None, None, 0, None)

    def subscribe(self, topic, qos=0):
        self.subscriptions.append(topic)

    def publish(self, topic, body, qos=0):
        self.published.append((topic, json.loads(body)))

    def loop_stop(self):
        pass

    def disconnect(self):
        self.closed = True


ENDPOINTS = {"rsu-1": ("rsu1-host", 1883), "rsu-2": ("rsu2-host", 1883)}


def vehicle_on_lane(lane, fail_rsu=None, **options):
    clients = []

    def factory(client_id):
        if fail_rsu and fail_rsu in client_id:
            raise ConnectionRefusedError("RSU caída")
        clients.append(FakeMqttClient(client_id))
        return clients[-1]

    vehicle = vehicle_simulator.Vehicle("veh-001", ENDPOINTS, simulator_args(**options), "test",
                                        vehicle_simulator.Recorder(), threading.Event(),
                                        lane=lane, client_factory=factory)
    vehicle.handover_grace_s = 0
    return vehicle, clients


def cross_to_next_lane(vehicle):
    vehicle.state.update(x=640, speed=20)  # El siguiente paso supera x=650 y cambia de carril.
    vehicle.step()


def test_vehicle_starts_on_the_rsu_of_its_lane():
    vehicle, clients = vehicle_on_lane(2)
    assert vehicle.rsu_id == "rsu-2"
    assert clients[0].endpoint == ENDPOINTS["rsu-2"]
    assert clients[0].subscriptions == ["v2x/rsu-2/alerts"]


def test_staying_in_the_same_half_does_not_change_rsu():
    vehicle, clients = vehicle_on_lane(0)
    cross_to_next_lane(vehicle)  # Carril 1 -> 2: sigue en la RSU 1.
    assert vehicle.rsu_id == "rsu-1"
    assert len(clients) == 1


def test_handover_connects_to_new_rsu_before_closing_the_old_one():
    vehicle, clients = vehicle_on_lane(1)
    cross_to_next_lane(vehicle)  # Carril 2 -> 3: pasa a la RSU 2.
    old, new = clients
    assert vehicle.rsu_id == "rsu-2"
    assert new.endpoint == ENDPOINTS["rsu-2"] and new.subscriptions == ["v2x/rsu-2/alerts"]
    assert [topic for topic, _ in new.published] == ["v2x/rsu-2/events/veh-001"]
    assert new.published[0][1]["payload"]["lane"] == 3
    assert old.published == []
    time.sleep(0.05)  # Periodo de gracia (0 s en la prueba) antes de cerrar la conexión antigua.
    assert old.closed and not new.closed
    assert vehicle.recorder.counters["handovers"] == 1


def test_circuit_wraps_back_to_the_first_rsu():
    vehicle, clients = vehicle_on_lane(3)
    cross_to_next_lane(vehicle)  # Carril 4 -> 1.
    assert vehicle.rsu_id == "rsu-1"
    assert clients[-1].endpoint == ENDPOINTS["rsu-1"]


def test_held_message_leaves_through_the_old_rsu_on_handover():
    vehicle, clients = vehicle_on_lane(1, reorder=1)
    vehicle.step()  # Evento retenido por la inyección de desorden.
    assert clients[0].published == []
    cross_to_next_lane(vehicle)
    assert [topic for topic, _ in clients[0].published] == ["v2x/rsu-1/events/veh-001"]


def test_failed_handover_keeps_old_rsu_and_retries():
    vehicle, clients = vehicle_on_lane(1, fail_rsu="rsu-2")
    cross_to_next_lane(vehicle)
    assert vehicle.rsu_id == "rsu-1"
    assert [topic for topic, _ in clients[0].published] == ["v2x/rsu-1/events/veh-001"]
    assert vehicle.recorder.counters["handover_failures"] == 1
    vehicle.client_factory = lambda client_id: clients.append(FakeMqttClient(client_id)) or clients[-1]
    vehicle.step()  # Siguiente evento: vuelve a intentarlo y ahora lo consigue.
    assert vehicle.rsu_id == "rsu-2"


def test_vehicle_ignores_repeated_alert_heard_from_both_rsus():
    vehicle, _ = vehicle_on_lane(0)
    alert = json.dumps({"alert_id": "a1", "alert_type": "road hazard", "origin": "cloud",
                        "trigger_event_time": "2026-11-09T10:00:00.000Z"})
    vehicle._on_alert(alert)
    vehicle._on_alert(alert)
    assert len(vehicle.recorder.alerts) == 1


def test_cumulative_counter_survives_restart():
    counter = vehicle_simulator.CumulativeCounter()
    for value in (5, 8, None, 2, 4):
        counter.update(value)
    assert counter.total == 7

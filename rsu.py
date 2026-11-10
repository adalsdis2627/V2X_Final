"""Roadside Unit: valida y filtra eventos MQTT, genera alertas locales,
reenvía a SQS con buffer y retransmite las alertas de SNS (HTTP) a sus vehículos."""
import json
import os
import re
import threading
import time
from collections import OrderedDict, deque
from datetime import datetime, timezone

from flask import Flask, jsonify, request

EVENT_TYPES = {"normal telemetry", "sudden braking", "congestion", "road hazard"}
NORMAL = "normal telemetry"
REQUIRED_FIELDS = ("event_id", "source_id", "event_type", "event_time", "location", "payload")
SOURCE_ID_RE = re.compile(r"^[\w-]{1,32}$")
MAX_BACKOFF_S = 30


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def validate_event(event):
    """Devuelve el motivo del rechazo, o None si el evento es válido."""
    if not isinstance(event, dict):
        return "not an object"
    for field in REQUIRED_FIELDS:
        if field not in event:
            return f"missing {field}"
    if event["event_type"] not in EVENT_TYPES:
        return "unknown event_type"
    if not isinstance(event["source_id"], str) or not SOURCE_ID_RE.match(event["source_id"]):
        return "invalid source_id"
    if not isinstance(event["event_id"], str) or not event["event_id"]:
        return "invalid event_id"
    if not isinstance(event["payload"], dict):
        return "invalid payload"
    try:
        datetime.fromisoformat(str(event["event_time"]).replace("Z", "+00:00"))
    except ValueError:
        return "invalid event_time"
    return None


class SeenIds:
    """Conjunto LRU de identificadores, para detectar duplicados con memoria acotada."""

    def __init__(self, capacity=20000):
        self.capacity = capacity
        self.items = OrderedDict()

    def seen(self, key):
        if key in self.items:
            self.items.move_to_end(key)
            return True
        self.items[key] = True
        if len(self.items) > self.capacity:
            self.items.popitem(last=False)
        return False


class Rsu:
    def __init__(self, rsu_id, send_to_cloud, publish_mqtt, confirm_subscription=None, topic_arn=None,
                 telemetry_period=3.0, buffer_size=5000, clock=time.monotonic):
        self.rsu_id = rsu_id
        self.send_to_cloud = send_to_cloud
        self.publish_mqtt = publish_mqtt
        self.confirm_subscription = confirm_subscription
        self.topic_arn = topic_arn
        self.telemetry_period = telemetry_period
        self.buffer_size = buffer_size
        self.clock = clock
        self.lock = threading.Lock()
        self.buffer = deque()
        self.seen_events = SeenIds()
        self.seen_alerts = SeenIds()
        self.last_telemetry = {}
        self.link_up = True
        self.backoff = 1
        self.metrics = {k: 0 for k in (
            "received", "rejected", "duplicates", "filtered", "forwarded", "dropped_buffer",
            "retries", "local_alerts", "cloud_alerts_relayed", "cloud_alerts_ignored",
            "cloud_alerts_duplicates")}

    @property
    def alerts_topic(self):
        return f"v2x/{self.rsu_id}/alerts"

    # --- Ruta de eventos ---------------------------------------------------

    def handle_event(self, raw):
        """Procesa un mensaje MQTT. Devuelve 'rejected', 'duplicate', 'filtered' o 'accepted'."""
        with self.lock:
            self.metrics["received"] += 1
            try:
                event = json.loads(raw)
            except (TypeError, ValueError):
                self.metrics["rejected"] += 1
                return "rejected"
            reason = validate_event(event)
            if reason:
                self.metrics["rejected"] += 1
                print(f"[{self.rsu_id}] rechazado: {reason}")
                return "rejected"
            if self.seen_events.seen(event["event_id"]):
                self.metrics["duplicates"] += 1
                return "duplicate"
            if event["event_type"] == NORMAL:
                now = self.clock()
                last = self.last_telemetry.get(event["source_id"])
                if last is not None and now - last < self.telemetry_period:
                    self.metrics["filtered"] += 1
                    return "filtered"
                self.last_telemetry[event["source_id"]] = now
            event["rsu_id"] = self.rsu_id
            event["rsu_time"] = utc_now()
            self._enqueue(event)

        if event["event_type"] == "sudden braking":
            self._send_local_alert(event)
        return "accepted"

    def _enqueue(self, event):
        """Mantiene el orden FIFO. Si el buffer está lleno sacrifica primero telemetría normal."""
        if len(self.buffer) >= self.buffer_size:
            oldest_normal = next((e for e in self.buffer if e["event_type"] == NORMAL), None)
            if oldest_normal is not None:
                self.buffer.remove(oldest_normal)
            elif event["event_type"] == NORMAL:
                self.metrics["dropped_buffer"] += 1
                return
            else:
                self.buffer.popleft()
            self.metrics["dropped_buffer"] += 1
        self.buffer.append(event)

    def _send_local_alert(self, event):
        alert = {
            "alert_id": f"local-{event['event_id']}",
            "alert_type": "sudden braking ahead",
            "severity": "high",
            "origin": self.rsu_id,
            "scope": self.rsu_id,
            "location": event["location"],
            "alert_time": utc_now(),
            "trigger_event_id": event["event_id"],
            "trigger_event_time": event["event_time"],
            "message": f"{event['source_id']} ha frenado bruscamente",
        }
        self.publish_mqtt(self.alerts_topic, json.dumps(alert))
        with self.lock:
            self.metrics["local_alerts"] += 1

    def forward_once(self):
        """Intenta reenviar el evento más antiguo. Devuelve los segundos a esperar."""
        with self.lock:
            if not self.link_up:
                return 0.5
            if not self.buffer:
                return 0.05
            event = self.buffer[0]
        try:
            self.send_to_cloud(json.dumps(event))
        except Exception as exc:
            with self.lock:
                self.metrics["retries"] += 1
                wait, self.backoff = self.backoff, min(self.backoff * 2, MAX_BACKOFF_S)
            print(f"[{self.rsu_id}] fallo al enviar a SQS, reintento en {wait}s: {exc}")
            return wait
        with self.lock:
            if self.buffer and self.buffer[0] is event:
                self.buffer.popleft()
            self.metrics["forwarded"] += 1
            self.backoff = 1
        return 0

    def forward_forever(self):
        while True:
            wait = self.forward_once()
            if wait:
                time.sleep(wait)

    # --- Ruta de alertas (SNS por HTTP) ------------------------------------

    def handle_sns(self, message_type, body):
        """Procesa una entrega HTTP de SNS. Devuelve el código de estado de la respuesta."""
        try:
            message = json.loads(body)
        except (TypeError, ValueError):
            return 400
        if message.get("TopicArn") != self.topic_arn:
            return 403
        if message_type == "SubscriptionConfirmation":
            self.confirm_subscription(message["Token"])
            return 200
        if message_type != "Notification":
            return 200
        with self.lock:
            if not self.link_up:
                return 503  # Enlace cortado en el experimento: SNS reintentará.
        try:
            alert = json.loads(message["Message"])
            alert_id = alert["alert_id"]
        except (KeyError, TypeError, ValueError):
            return 400
        with self.lock:
            if self.seen_alerts.seen(alert_id):
                self.metrics["cloud_alerts_duplicates"] += 1
                return 200
            if alert.get("scope") not in ("all", self.rsu_id):
                self.metrics["cloud_alerts_ignored"] += 1
                return 200
            self.metrics["cloud_alerts_relayed"] += 1
        self.publish_mqtt(self.alerts_topic, json.dumps(alert))
        return 200

    def set_link(self, up):
        with self.lock:
            self.link_up = up

    def snapshot(self):
        with self.lock:
            return {**self.metrics, "buffered": len(self.buffer), "link_up": self.link_up}


def create_app(rsu):
    app = Flask(__name__)

    @app.post("/sns")
    def sns():
        # SNS envía JSON con Content-Type text/plain: request.json no serviría.
        status = rsu.handle_sns(request.headers.get("x-amz-sns-message-type"), request.get_data())
        return "", status

    @app.post("/admin/link")
    def admin_link():
        rsu.set_link(bool(request.get_json(force=True).get("up")))
        return jsonify(rsu.snapshot())

    @app.get("/metrics")
    def metrics():
        return jsonify(rsu.snapshot())

    return app


def retry_until_ok(action, description):
    while True:
        try:
            return action()
        except Exception as exc:
            print(f"Esperando {description}: {exc}")
            time.sleep(3)


def main():
    import boto3
    import paho.mqtt.client as mqtt

    rsu_id = os.environ.get("RSU_ID", "rsu-1")
    public_host = os.environ["PUBLIC_HOST"]
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    queue_name = os.environ.get("QUEUE_NAME", "v2x-events")
    topic_name = os.environ.get("TOPIC_NAME", "v2x-alerts")
    http_port = int(os.environ.get("HTTP_PORT", "8080"))

    sqs = boto3.client("sqs", region_name=region)
    sns = boto3.client("sns", region_name=region)
    queue_url = retry_until_ok(lambda: sqs.get_queue_url(QueueName=queue_name)["QueueUrl"], "la cola SQS")
    topic_arn = retry_until_ok(lambda: sns.create_topic(Name=topic_name)["TopicArn"], "el topic SNS")

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"{rsu_id}-core")
    rsu = Rsu(
        rsu_id,
        send_to_cloud=lambda body: sqs.send_message(QueueUrl=queue_url, MessageBody=body),
        publish_mqtt=lambda topic, body: client.publish(topic, body, qos=1),
        confirm_subscription=lambda token: sns.confirm_subscription(TopicArn=topic_arn, Token=token),
        topic_arn=topic_arn,
        telemetry_period=float(os.environ.get("TELEMETRY_PERIOD_S", "3")),
        buffer_size=int(os.environ.get("BUFFER_SIZE", "5000")),
    )

    def on_connect(mqtt_client, _userdata, _flags, _reason, _properties):
        mqtt_client.subscribe(f"v2x/{rsu_id}/events/+", qos=1)

    client.on_connect = on_connect
    client.on_message = lambda _c, _u, msg: rsu.handle_event(msg.payload)
    retry_until_ok(lambda: client.connect("localhost", 1883), "el broker MQTT")
    client.loop_start()

    threading.Thread(target=rsu.forward_forever, daemon=True).start()

    def subscribe_when_listening():
        time.sleep(2)
        endpoint = f"http://{public_host}:{http_port}/sns"
        retry_until_ok(lambda: sns.subscribe(TopicArn=topic_arn, Protocol="http", Endpoint=endpoint),
                       "la suscripción SNS")
        print(f"[{rsu_id}] suscrito a {topic_arn} en {endpoint}")

    threading.Thread(target=subscribe_when_listening, daemon=True).start()
    create_app(rsu).run(host="0.0.0.0", port=http_port, threaded=True)


if __name__ == "__main__":
    main()

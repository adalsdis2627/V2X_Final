"""V2X-SERVICE: consume eventos de SQS, los persiste en DynamoDB de forma idempotente,
detecta condiciones de alerta, las publica en SNS y muestra el estado en un dashboard."""
import hashlib
import json
import os
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone

from botocore.exceptions import ClientError
from flask import Flask, jsonify, render_template_string

REQUIRED_FIELDS = ("event_id", "source_id", "event_type", "event_time")


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_time(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def percentile_95(values):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(0.95 * len(ordered)))]


def build_alert(alert_type, event, key, severity, message):
    """El alert_id es determinista: reprocesar un evento nunca crea una alerta distinta."""
    digest = hashlib.sha1(f"{alert_type}|{event['location']}|{key}".encode()).hexdigest()[:16]
    return {
        "alert_id": f"alrt-{digest}",
        "alert_type": alert_type,
        "severity": severity,
        "origin": "cloud",
        "scope": "all",
        "location": event["location"],
        "alert_time": utc_now(),
        "trigger_event_id": event["event_id"],
        "trigger_event_time": event["event_time"],
        "message": message,
    }


class AlertEngine:
    def __init__(self, congestion_threshold=3, window_s=30):
        self.threshold = congestion_threshold
        self.window_s = window_s
        self.windows = defaultdict(set)
        self.fired = set()

    def evaluate(self, event):
        if event["event_type"] == "road hazard":
            return [build_alert("road hazard", event, event["event_id"], "high",
                                f"Peligro en la vía notificado por {event['source_id']}")]
        if event["event_type"] != "congestion":
            return []

        bucket = int(parse_time(event["event_time"]).timestamp() // self.window_s)
        key = (event["location"], bucket)
        self.windows[key].add(event["source_id"])
        for old in [k for k in self.windows if k[1] < bucket - 2]:
            del self.windows[old]
            self.fired.discard(old)
        if len(self.windows[key]) < self.threshold or key in self.fired:
            return []
        self.fired.add(key)
        return [build_alert("congestion", event, bucket, "medium",
                            f"{len(self.windows[key])} vehículos en congestión en {event['location']}")]


class V2XService:
    def __init__(self, table, publish_alert, engine=None):
        self.table = table
        self.publish_alert = publish_alert
        self.engine = engine or AlertEngine()
        self.lock = threading.Lock()
        self.vehicles = {}
        self.alerts = deque(maxlen=50)
        self.latencies = deque(maxlen=2000)
        self.metrics = {k: 0 for k in (
            "accepted", "processed", "duplicates", "redelivered", "out_of_order",
            "alerts_published", "errors")}

    def _count(self, name):
        with self.lock:
            self.metrics[name] += 1

    def process_body(self, body, receive_count=1):
        """Lanza una excepción si no se puede procesar: el mensaje se reintenta y acaba en la DLQ."""
        event = json.loads(body)
        for field in REQUIRED_FIELDS:
            if field not in event:
                raise ValueError(f"missing {field}")
        event.setdefault("location", "unknown")
        self._count("accepted")
        if receive_count > 1:
            self._count("redelivered")

        is_new = self._store_event(event, body)
        if is_new:
            self._update_vehicle(event)
            self._record_latency(event)
        else:
            self._count("duplicates")
        # También con duplicados: si el proceso cayó antes de publicar, la alerta sale ahora.
        for alert in self.engine.evaluate(event):
            self._emit_alert(alert)
        if is_new:
            self._count("processed")
        return "processed" if is_new else "duplicate"

    def _store_event(self, event, body):
        try:
            self.table.put_item(
                Item={
                    "pk": f"event#{event['event_id']}",
                    "event": body if isinstance(body, str) else body.decode(),
                    "event_type": event["event_type"],
                    "event_time": event["event_time"],
                    "run_id": str(event.get("payload", {}).get("run_id", "")),
                },
                ConditionExpression="attribute_not_exists(pk)",
            )
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise

    def _update_vehicle(self, event):
        payload = event.get("payload", {})
        try:
            self.table.update_item(
                Key={"pk": f"vehicle#{event['source_id']}"},
                UpdateExpression="SET event_time = :t, vehicle_state = :s",
                ConditionExpression="attribute_not_exists(event_time) OR event_time < :t",
                ExpressionAttributeValues={":t": event["event_time"], ":s": json.dumps(event)},
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
            self._count("out_of_order")

        with self.lock:
            current = self.vehicles.get(event["source_id"])
            if current and current["event_time"] >= event["event_time"]:
                return
            now_ms = time.time() * 1000
            self.vehicles[event["source_id"]] = {
                "id": event["source_id"],
                "rsu": event.get("rsu_id", "-"),
                "x": payload.get("x", 50),
                "y": payload.get("y", 80),
                "lane": payload.get("lane", 1),
                "speed": payload.get("speed", 0),
                "events": [{"type": event["event_type"], "timestamp": now_ms}],
                "timestamp": now_ms,
                "event_time": event["event_time"],
                "last_update": time.strftime("%H:%M:%S"),
            }

    def _record_latency(self, event):
        if "rsu_time" in event:
            elapsed = (datetime.now(timezone.utc) - parse_time(event["rsu_time"])).total_seconds() * 1000
            with self.lock:
                self.latencies.append(elapsed)

    def _emit_alert(self, alert):
        key = {"pk": f"alert#{alert['alert_id']}"}
        if "Item" in self.table.get_item(Key=key):
            return
        # Publicar antes de guardar: un fallo entre ambos repite la alerta, y la RSU la deduplica.
        self.publish_alert(json.dumps(alert))
        self.table.put_item(Item={**key, "alert": json.dumps(alert), "alert_time": alert["alert_time"]})
        with self.lock:
            self.alerts.appendleft(alert)
            self.metrics["alerts_published"] += 1
        print(f"ALERTA {alert['alert_type']} en {alert['location']}: {alert['message']}")

    def snapshot(self):
        with self.lock:
            return {**self.metrics, "p95_cloud_latency_ms": percentile_95(list(self.latencies))}


def sqs_listener(service, sqs, queue_url):
    print("Esperando eventos de la cola SQS")
    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20,
                AttributeNames=["ApproximateReceiveCount"],
            )
        except Exception as exc:
            print(f"Error en SQS: {exc}")
            time.sleep(2)
            continue
        for msg in response.get("Messages", []):
            try:
                service.process_body(msg["Body"], int(msg["Attributes"]["ApproximateReceiveCount"]))
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
            except Exception as exc:
                service._count("errors")
                print(f"Error procesando mensaje: {exc}")


HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <style>
        body { font-family: Arial, sans-serif; background-color: #1a1a24; color: white; text-align: center; margin: 0; padding: 20px; }
        #canvas-container { margin: 15px auto; display: inline-block; border: 3px solid #444; border-radius: 8px; background-color: #111118; }
        canvas { display: block; }
        .vehicle-card { display: inline-block; background: #2a2a3c; padding: 10px 15px; margin: 5px; border-radius: 6px; text-align: left; font-size: 12px; width: 170px; vertical-align: top; }
        .status-dot { height: 10px; width: 10px; border-radius: 50%; display: inline-block; margin-right: 5px; }
        .normal { background-color: #00ff88; }
        .danger { background-color: #ff4444; }
        .events-list { margin: 5px 0 0 0; padding-left: 15px; color: #ffdd57; }
        .events-list li { font-size: 11px; }
        #alerts-list { max-width: 900px; margin: 0 auto; text-align: left; font-size: 13px; }
        .alert-row { background: #4a2130; border-left: 4px solid #ff4444; padding: 5px 10px; margin: 3px 0; }
    </style>
</head>
<body>
    <h1> Circuito de 4 Carriles</h1>
    <div id="canvas-container"><canvas id="map" width="700" height="400"></canvas></div>
    <h2>Alertas recientes</h2>
    <div id="alerts-list">Sin alertas</div>
    <h2>Vehículos en línea</h2>
    <div id="vehicles-list">Esperando datos de la SQS</div>

    <script>
        const canvas = document.getElementById('map');
        const ctx = canvas.getContext('2d');

        function drawRoads() {
            ctx.clearRect(0, 0, canvas.width, canvas.height);
            const laneHeight = canvas.height / 5;
            
            for (let i = 1; i <= 4; i++) {
                let yPos = i * laneHeight;
                
                ctx.strokeStyle = "#444455";
                ctx.lineWidth = 25;
                ctx.beginPath();
                ctx.moveTo(50, yPos);
                ctx.lineTo(canvas.width - 50, yPos);
                ctx.stroke();

                ctx.strokeStyle = "#ffffff";
                ctx.lineWidth = 2;
                ctx.setLineDash([10, 10]);
                ctx.beginPath();
                ctx.moveTo(50, yPos);
                ctx.lineTo(canvas.width - 50, yPos);
                ctx.stroke();
                ctx.setLineDash([]);
            }
        }

        async function updateDashboard() {
            try {
                const res = await fetch('/api/vehicles');
                const data = await res.json();
                drawRoads();
                const listDiv = document.getElementById('vehicles-list');
                listDiv.innerHTML = '';

                const now = Date.now();

                const vehiclesArray = Object.values(data).filter(veh => {
                    return (now - veh.timestamp) <= 15000;
                });

                if (vehiclesArray.length === 0) {
                    listDiv.innerHTML = 'Esperando datos de la SQS.';
                    return;
                }

                vehiclesArray.forEach((veh) => {
                    let targetY = veh.y;
                    let mappedX = veh.x;

                    let hasAlert = veh.events && veh.events.some(e => e.type !== "normal telemetry");

                    // Representación de los coches
                    ctx.beginPath();
                    ctx.arc(mappedX, targetY, 12, 0, 2 * Math.PI);
                    ctx.fillStyle = hasAlert ? '#ff4444' : '#00ccff';
                    ctx.fill();
                    ctx.strokeStyle = '#ffffff';
                    ctx.lineWidth = 2;
                    ctx.stroke();

                    ctx.fillStyle = '#ffffff';
                    ctx.font = '11px Arial';
                    ctx.fillText(veh.id, mappedX - 15, targetY - 16);

                    if (hasAlert) {
                        ctx.fillStyle = '#ffff00';
                        ctx.font = '12px Arial';
                        ctx.fillText('⚠️', mappedX - 5, targetY + 4);
                    }

                    let eventsHtml = "";
                    let validEvents = veh.events ? veh.events.filter(e => (now - e.timestamp) < 15000) : [];
                    
                    if (validEvents.length === 0) {
                        eventsHtml = "<li>Ninguna</li>";
                    } else {
                        validEvents.forEach(ev => {
                            eventsHtml += `<li>${ev.type}</li>`;
                        });
                    }

                    const card = document.createElement('div');
                    card.className = `vehicle-card`;
                    card.innerHTML = `
                        <strong><span class="status-dot ${hasAlert ? 'danger' : 'normal'}"></span>${veh.id}</strong><br>
                        Carril: ${veh.lane} | Vel: ${veh.speed} km/h<br>
                        Pos: (${Math.round(veh.x)}, ${Math.round(veh.y)})<br>
                        <hr style="border: 0; border-top: 1px solid #444; margin: 5px 0;">
                        <small><strong>Eventos actuales:</strong></small>
                        <ul class="events-list">
                            ${eventsHtml}
                        </ul>
                        <small style="color: #888;">Hora: ${veh.last_update}</small>
                    `;
                    listDiv.appendChild(card);
                });
            } catch (err) { console.error("Error:", err); }
        }
        async function updateAlerts() {
            try {
                const res = await fetch('/api/alerts');
                const alerts = await res.json();
                const box = document.getElementById('alerts-list');
                box.textContent = alerts.length ? '' : 'Sin alertas';
                alerts.slice(0, 8).forEach(a => {
                    const row = document.createElement('div');
                    row.className = 'alert-row';
                    row.textContent = `${a.alert_time} | ${a.alert_type} | ${a.location} | ${a.message}`;
                    box.appendChild(row);
                });
            } catch (err) { console.error("Error:", err); }
        }
        setInterval(updateDashboard, 1000);
        setInterval(updateAlerts, 1000);
    </script>
</body>
</html>
"""


def create_app(service):
    app = Flask(__name__)

    @app.route("/")
    def index():
        return render_template_string(HTML_TEMPLATE)

    @app.route("/api/vehicles")
    def get_vehicles():
        with service.lock:
            return jsonify(dict(service.vehicles))

    @app.route("/api/alerts")
    def get_alerts():
        with service.lock:
            return jsonify(list(service.alerts))

    @app.route("/api/metrics")
    def get_metrics():
        return jsonify(service.snapshot())

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

    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    queue_name = os.environ.get("QUEUE_NAME", "v2x-events")
    topic_name = os.environ.get("TOPIC_NAME", "v2x-alerts")
    table_name = os.environ.get("TABLE_NAME", "v2x")

    sqs = boto3.client("sqs", region_name=region)
    sns = boto3.client("sns", region_name=region)
    table = boto3.resource("dynamodb", region_name=region).Table(table_name)

    queue_url = retry_until_ok(lambda: sqs.get_queue_url(QueueName=queue_name)["QueueUrl"], "la cola SQS")
    topic_arn = retry_until_ok(lambda: sns.create_topic(Name=topic_name)["TopicArn"], "el topic SNS")
    retry_until_ok(table.load, "la tabla DynamoDB")

    service = V2XService(table, lambda body: sns.publish(TopicArn=topic_arn, Message=body))
    threading.Thread(target=sqs_listener, args=(service, sqs, queue_url), daemon=True).start()
    create_app(service).run(host="0.0.0.0", port=5000)


if __name__ == "__main__":
    main()

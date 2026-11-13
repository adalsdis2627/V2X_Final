"""Simulador de vehículos (OBU) por MQTT, inyección de anomalías y experimentos controlados.

Los vehículos recorren un circuito de 4 carriles: la RSU 1 cubre los carriles 1-2 y la RSU 2
los carriles 3-4. Al pasar de una mitad a la otra, cada vehículo cambia de RSU.

Ejemplos:
  python vehicle_simulator.py --local
  python vehicle_simulator.py --vehicles 5 --rate 1 --loss 0.1 --duration 120
  python vehicle_simulator.py --experiment disconnect --repeat 3 --ssh-key labsuser.pem
"""
import argparse
import csv
import json
import os
import random
import subprocess
import threading
import time
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt

REGION = "us-east-1"
QUEUE_NAME = "v2x-events"
DLQ_NAME = "v2x-events-dlq"
TOPIC_NAME = "v2x-alerts"
TABLE_NAME = "v2x"
LOCALSTACK_URL = "http://localhost:4566"
RESULTS_DIR = Path("results")
LANE_Y_POSITIONS = [80, 160, 240, 320]
CONNECT_TIMEOUT_S = 5
HANDOVER_GRACE_S = 1.5  # Tiempo que se mantiene la conexión antigua tras un cambio de RSU.
CRITICAL_EVENTS = [("sudden braking", 0.35), ("congestion", 0.45), ("road hazard", 0.20)]
SPEED_RANGES = {
    "normal telemetry": (40, 90),
    "sudden braking": (5, 30),
    "congestion": (5, 25),
    "road hazard": (30, 60),
}


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def rsu_for_lane(lane_index):
    """Los carriles 1-2 (índices 0-1) los cubre la RSU 1 y los carriles 3-4 la RSU 2."""
    return "rsu-1" if lane_index < 2 else "rsu-2"


def p95(values):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(0.95 * len(ordered)))] if ordered else ""


def choose_event_type(critical_rate):
    if random.random() >= critical_rate:
        return "normal telemetry"
    names, weights = zip(*CRITICAL_EVENTS)
    return random.choices(names, weights)[0]


def make_event(vehicle_id, state, seq, run_id, event_type):
    low, high = SPEED_RANGES[event_type]
    return {
        "event_id": str(uuid.uuid4()),
        "source_id": vehicle_id,
        "event_type": event_type,
        "event_time": utc_now(),
        "location": "segment-A12",
        "payload": {
            "x": state["x"],
            "y": LANE_Y_POSITIONS[state["lane"]],
            "lane": state["lane"] + 1,
            "speed": random.randint(low, high),
            "seq": seq,
            "run_id": run_id,
        },
    }


class Injector:
    """Aplica retardo, pérdida, duplicación y desorden antes de publicar.

    Cada mensaje viaja con su propia función de publicación: así, un mensaje retrasado o
    retenido sale por la RSU a la que iba dirigido aunque el vehículo haya cambiado de RSU.
    """

    def __init__(self, args, recorder):
        self.delay_s = args.delay_ms / 1000
        self.loss = args.loss
        self.duplicate = args.duplicate
        self.reorder = args.reorder
        self.recorder = recorder
        self.lock = threading.Lock()
        self.held = None

    def _emit(self, publish, topic, body):
        if self.delay_s:
            timer = threading.Timer(self.delay_s, publish, (topic, body))
            timer.daemon = True
            timer.start()
        else:
            publish(topic, body)
        self.recorder.count("published")

    def send(self, publish, topic, body):
        if random.random() < self.loss:
            self.recorder.count("injected_loss")
            return
        copies = 1
        if random.random() < self.duplicate:
            copies = 2
            self.recorder.count("injected_duplicates")
        with self.lock:
            if self.held is None and random.random() < self.reorder:
                self.held = (publish, topic, body, copies)
                self.recorder.count("injected_reorders")
                return
            held, self.held = self.held, None
        for _ in range(copies):
            self._emit(publish, topic, body)
        self._release(held)

    def flush(self):
        with self.lock:
            held, self.held = self.held, None
        self._release(held)

    def _release(self, held):
        if held:
            publish, topic, body, copies = held
            for _ in range(copies):
                self._emit(publish, topic, body)


class Recorder:
    def __init__(self):
        self.lock = threading.Lock()
        self.counters = {}
        self.alerts = []
        self.handover_ms = []

    def count(self, name):
        with self.lock:
            self.counters[name] = self.counters.get(name, 0) + 1

    def alert(self, vehicle_id, alert):
        received = datetime.now(timezone.utc)
        trigger = datetime.fromisoformat(alert["trigger_event_time"].replace("Z", "+00:00"))
        latency_ms = round((received - trigger).total_seconds() * 1000, 1)
        row = {"vehicle": vehicle_id, "alert_id": alert["alert_id"], "alert_type": alert["alert_type"],
               "origin": alert["origin"], "latency_ms": latency_ms}
        with self.lock:
            self.alerts.append(row)
        print(f"  << [{vehicle_id}] ALERTA {alert['alert_type']} ({alert['origin']}) {latency_ms} ms")

    def handover(self, vehicle_id, old_rsu, new_rsu, elapsed_ms):
        with self.lock:
            self.counters["handovers"] = self.counters.get("handovers", 0) + 1
            self.handover_ms.append(round(elapsed_ms, 1))
        print(f"  <> [{vehicle_id}] cambio de {old_rsu} a {new_rsu} ({elapsed_ms:.0f} ms)")

    def p95_latency(self, origin_cloud):
        return p95([a["latency_ms"] for a in self.alerts if (a["origin"] == "cloud") == origin_cloud])

    def summary(self):
        return {**self.counters, "alerts_received": len(self.alerts),
                "p95_local_alert_ms": self.p95_latency(False), "p95_cloud_alert_ms": self.p95_latency(True),
                "p95_handover_ms": p95(self.handover_ms)}

    def write_alerts(self, run_id):
        RESULTS_DIR.mkdir(exist_ok=True)
        with open(RESULTS_DIR / f"{run_id}_alerts.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, ["vehicle", "alert_id", "alert_type", "origin", "latency_ms"])
            writer.writeheader()
            writer.writerows(self.alerts)


def new_mqtt_client(client_id):
    return mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)


class Vehicle(threading.Thread):
    """Vehículo que recorre el circuito y cambia de RSU al pasar de una mitad a la otra.

    El cambio es "make-before-break": primero se conecta a la RSU nueva y solo después
    cambia de cliente y cierra la conexión antigua, así que no se pierden eventos.
    """

    def __init__(self, vehicle_id, endpoints, args, run_id, recorder, stop_event,
                 lane=None, verbose=False, client_factory=new_mqtt_client):
        super().__init__(daemon=True)
        self.vehicle_id = vehicle_id
        self.endpoints = endpoints
        self.args = args
        self.run_id = run_id
        self.recorder = recorder
        self.stop_event = stop_event
        self.verbose = verbose
        self.client_factory = client_factory
        self.handover_grace_s = HANDOVER_GRACE_S
        self.lock = threading.Lock()
        self.seen_alerts = set()
        lane = random.randint(0, 3) if lane is None else lane
        self.state = {"x": random.randint(50, 400), "lane": lane, "speed": random.randint(15, 25)}
        self.seq = 0
        self.rsu_id = rsu_for_lane(lane)
        self.client = self._connect(self.rsu_id)
        self.injector = Injector(args, recorder)

    def _connect(self, rsu_id):
        """Conecta un cliente nuevo a la RSU y espera a que el broker acepte la conexión."""
        # El sufijo aleatorio evita que dos clientes con el mismo identificador se expulsen del broker.
        client = self.client_factory(f"{self.vehicle_id}-{rsu_id}-{uuid.uuid4().hex[:6]}")
        ready = threading.Event()

        def on_connect(mqtt_client, _userdata, _flags, reason_code, _properties):
            if not getattr(reason_code, "is_failure", False):
                mqtt_client.subscribe(f"v2x/{rsu_id}/alerts", qos=1)  # Se repite en cada reconexión.
                ready.set()

        client.on_connect = on_connect
        client.on_message = lambda _client, _userdata, message: self._on_alert(message.payload)
        client.connect(*self.endpoints[rsu_id])
        client.loop_start()
        if not ready.wait(CONNECT_TIMEOUT_S):
            self._close(client)
            raise ConnectionError(f"{rsu_id} no aceptó la conexión en {CONNECT_TIMEOUT_S} s")
        return client

    @staticmethod
    def _close(client):
        client.loop_stop()
        client.disconnect()

    def _on_alert(self, payload):
        alert = json.loads(payload)
        with self.lock:
            # Durante un cambio se escucha a las dos RSU: la misma alerta puede llegar dos veces.
            if alert["alert_id"] in self.seen_alerts:
                return
            self.seen_alerts.add(alert["alert_id"])
        self.recorder.alert(self.vehicle_id, alert)

    def _handover(self, new_rsu):
        started = time.monotonic()
        try:
            new_client = self._connect(new_rsu)
        except Exception as exc:
            self.recorder.count("handover_failures")
            print(f"[{self.vehicle_id}] no se pudo cambiar a {new_rsu}; se reintentará: {exc}")
            return
        self.injector.flush()  # Lo retenido por la inyección de desorden sale por la RSU antigua.
        with self.lock:
            old_client, old_rsu = self.client, self.rsu_id
            self.client, self.rsu_id = new_client, new_rsu
        self.recorder.handover(self.vehicle_id, old_rsu, new_rsu, (time.monotonic() - started) * 1000)
        timer = threading.Timer(self.handover_grace_s, self._close, (old_client,))
        timer.daemon = True
        timer.start()

    def _advance(self):
        self.state["x"] += self.state["speed"]
        if self.state["x"] > 650:
            self.state["x"] = 50
            self.state["lane"] = (self.state["lane"] + 1) % 4
            self.state["speed"] = random.randint(15, 25)

    def _emit(self):
        self.seq += 1
        event_type = choose_event_type(self.args.critical_rate)
        event = make_event(self.vehicle_id, self.state, self.seq, self.run_id, event_type)
        client = self.client  # Fijado ahora: si el mensaje se retrasa, sale por esta RSU.
        self.injector.send(lambda topic, body: client.publish(topic, body, qos=1),
                           f"v2x/{self.rsu_id}/events/{self.vehicle_id}", json.dumps(event))
        self.recorder.count("generated")
        if self.verbose:
            payload = event["payload"]
            print(f"[{self.vehicle_id} -> {self.rsu_id}] carril {payload['lane']} | x={payload['x']} | "
                  f"{payload['speed']} km/h | {event_type}")
        elif event_type != "normal telemetry":
            print(f"[{self.vehicle_id} -> {self.rsu_id}] {event_type}")

    def step(self):
        """Avanza, cambia de RSU si ha entrado en la otra mitad del circuito y emite un evento."""
        self._advance()
        wanted = rsu_for_lane(self.state["lane"])
        if wanted != self.rsu_id:  # Si el cambio falla, se reintenta en el siguiente paso.
            self._handover(wanted)
        self._emit()

    def run(self):
        next_burst = time.monotonic() + self.args.burst_period
        while not self.stop_event.wait(self.args.rate):
            self.step()
            if self.args.burst and time.monotonic() >= next_burst:
                next_burst += self.args.burst_period
                for _ in range(self.args.burst):
                    self._emit()
        self.injector.flush()
        time.sleep(1)  # Da tiempo a que salgan los últimos QoS 1.
        self._close(self.client)


class Fleet:
    def __init__(self, args, targets, run_id):
        self.stop_event = threading.Event()
        self.recorder = Recorder()
        self.vehicles = [
            Vehicle(f"veh-{number:03d}", targets["mqtt"], args, run_id, self.recorder, self.stop_event)
            for number in range(1, args.vehicles + 1)
        ]

    def start(self):
        for vehicle in self.vehicles:
            vehicle.start()

    def stop(self):
        self.stop_event.set()
        for vehicle in self.vehicles:
            vehicle.join(timeout=5)


# --- Descubrimiento de la infraestructura ------------------------------------

def ensure_local_resources():
    """Crea en LocalStack los mismos recursos que el workflow crea en AWS."""
    import boto3

    sqs = boto3.client("sqs", region_name=REGION)
    dlq_url = sqs.create_queue(QueueName=DLQ_NAME)["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    sqs.create_queue(QueueName=QUEUE_NAME, Attributes={
        "VisibilityTimeout": "30",
        "MessageRetentionPeriod": "86400",
        "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": 5}),
    })
    boto3.client("sns", region_name=REGION).create_topic(Name=TOPIC_NAME)
    dynamodb = boto3.client("dynamodb", region_name=REGION)
    if TABLE_NAME not in dynamodb.list_tables()["TableNames"]:
        dynamodb.create_table(
            TableName=TABLE_NAME,
            BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        )
        dynamodb.get_waiter("table_exists").wait(TableName=TABLE_NAME)


def local_targets():
    return {
        "mqtt": {"rsu-1": ("localhost", 1883), "rsu-2": ("localhost", 1884)},
        "rsu_http": {"rsu-1": "http://localhost:8081", "rsu-2": "http://localhost:8082"},
        "service_http": "http://localhost:5000",
        "service_control": lambda action: subprocess.run(["docker", action, "v2x-service"], check=True,
                                                         stdout=subprocess.DEVNULL),
    }


def aws_targets(args):
    import boto3

    ec2 = boto3.client("ec2", region_name=REGION)
    reservations = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": ["v2x-rsu1", "v2x-rsu2", "v2x-service"]},
        {"Name": "instance-state-name", "Values": ["running"]},
    ])["Reservations"]
    hosts = {}
    for reservation in reservations:
        for instance in reservation["Instances"]:
            name = next(t["Value"] for t in instance["Tags"] if t["Key"] == "Name")
            hosts[name] = instance["PublicDnsName"]
    missing = {"v2x-rsu1", "v2x-rsu2", "v2x-service"} - set(hosts)
    if missing:
        raise SystemExit(f"Instancias EC2 no encontradas o paradas: {sorted(missing)}")

    def service_control(action):
        if not args.ssh_key:
            raise SystemExit("El experimento 'failure' en AWS necesita --ssh-key labsuser.pem")
        subprocess.run(["ssh", "-i", args.ssh_key, "-o", "StrictHostKeyChecking=no",
                        f"ec2-user@{hosts['v2x-service']}", f"sudo docker {action} v2x-service"],
                       check=True, stdout=subprocess.DEVNULL)

    return {
        "mqtt": {"rsu-1": (hosts["v2x-rsu1"], 1883), "rsu-2": (hosts["v2x-rsu2"], 1883)},
        "rsu_http": {"rsu-1": f"http://{hosts['v2x-rsu1']}:8080", "rsu-2": f"http://{hosts['v2x-rsu2']}:8080"},
        "service_http": f"http://{hosts['v2x-service']}:5000",
        "service_control": service_control,
    }


# --- Experimentos ------------------------------------------------------------

def http_json(url, data=None):
    body = None if data is None else json.dumps(data).encode()
    request = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"},
                                     method="POST" if body else "GET")
    with urllib.request.urlopen(request, timeout=3) as response:
        return json.load(response)


def try_http_json(url):
    try:
        return http_json(url)
    except Exception:
        return None


class CumulativeCounter:
    """Suma incrementos aunque el contador de origen vuelva a cero al reiniciarse el proceso."""

    def __init__(self):
        self.last = None
        self.total = 0

    def update(self, value):
        if value is None:
            return
        if self.last is not None:
            self.total += value - self.last if value >= self.last else value
        self.last = value


def stored_events(table, run_id):
    """Devuelve los eventos de una ejecución guardados en DynamoDB."""
    from boto3.dynamodb.conditions import Attr

    items, kwargs = [], {"FilterExpression": Attr("run_id").eq(run_id)}
    while True:
        page = table.scan(**kwargs)
        items.extend(page["Items"])
        if "LastEvaluatedKey" not in page:
            return items
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def final_state_ok(table, items):
    latest = {}
    for item in items:
        source = json.loads(item["event"])["source_id"]
        latest[source] = max(latest.get(source, ""), item["event_time"])
    for source, event_time in latest.items():
        state = table.get_item(Key={"pk": f"vehicle#{source}"}, ConsistentRead=True).get("Item", {})
        if state.get("event_time") != event_time:
            return False
    return bool(latest)


def run_experiment_once(args, targets, run_id):
    import boto3

    sqs = boto3.client("sqs", region_name=REGION)
    queue_url = sqs.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
    dlq_url = sqs.get_queue_url(QueueName=DLQ_NAME)["QueueUrl"]
    table = boto3.resource("dynamodb", region_name=REGION).Table(TABLE_NAME)

    def queue_depth(url):
        attrs = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=[
            "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
        return int(attrs["ApproximateNumberOfMessages"]), int(attrs["ApproximateNumberOfMessagesNotVisible"])

    rsus = list(targets["rsu_http"].items())
    counters = {name: CumulativeCounter() for name in (
        "duplicates", "redelivered", "out_of_order", "out_of_order_same_rsu", "out_of_order_cross_rsu", "errors")}
    rsu_totals = {f"{rsu}:{name}": CumulativeCounter() for rsu, _ in rsus
                  for name in ("forwarded", "retries", "dropped_buffer")}
    peaks = {"buffered": 0, "queue": 0}
    timeline, stop_sampling = [], threading.Event()
    marks = {}

    def take_sample():
        visible, in_flight = queue_depth(queue_url)
        buffered = 0
        row = {"t": round(time.monotonic() - marks["start"], 1), "queue_visible": visible, "queue_in_flight": in_flight}
        for rsu, url in rsus:
            metrics = try_http_json(f"{url}/metrics") or {}
            buffered += metrics.get("buffered", 0)
            row[f"{rsu}_buffered"] = metrics.get("buffered", "")
            for name in ("forwarded", "retries", "dropped_buffer"):
                rsu_totals[f"{rsu}:{name}"].update(metrics.get(name))
        service = try_http_json(f"{targets['service_http']}/api/metrics")
        row["service_up"] = service is not None
        row["service_processed"] = (service or {}).get("processed", "")
        for name, counter in counters.items():
            counter.update((service or {}).get(name))
        peaks["buffered"] = max(peaks["buffered"], buffered)
        peaks["queue"] = max(peaks["queue"], visible)
        row["backlog"] = visible + in_flight + buffered
        timeline.append(row)

    def sampler():
        while not stop_sampling.wait(1):
            try:
                take_sample()
            except Exception as exc:
                print(f"Error de muestreo: {exc}")

    marks["start"] = time.monotonic()
    take_sample()  # Referencia de los contadores acumulados antes de generar tráfico.
    fleet = Fleet(args, targets, run_id)
    fleet.start()
    threading.Thread(target=sampler, daemon=True).start()

    print(f"[{run_id}] calentamiento de {args.warmup} s")
    time.sleep(args.warmup)
    rsu_url = targets["rsu_http"]["rsu-1"]
    if args.experiment == "disconnect":
        http_json(f"{rsu_url}/admin/link", {"up": False})
    else:
        targets["service_control"]("stop")
    print(f"[{run_id}] fallo inyectado durante {args.outage} s")
    time.sleep(args.outage)
    if args.experiment == "disconnect":
        http_json(f"{rsu_url}/admin/link", {"up": True})
    else:
        targets["service_control"]("start")
    restored_at = round(time.monotonic() - marks["start"], 1)
    # Referencia para contar solo el desorden posterior a la restauración (precisión de ±1 s de muestreo).
    at_restore = {name: counters[name].total for name in ("out_of_order_same_rsu", "out_of_order_cross_rsu")}
    print(f"[{run_id}] restaurado; tráfico durante {args.cooldown} s más")
    time.sleep(args.cooldown)
    fleet.stop()

    deadline = time.monotonic() + args.drain_timeout
    while time.monotonic() < deadline and not (timeline and timeline[-1]["backlog"] == 0):
        time.sleep(1)
    time.sleep(5)
    stop_sampling.set()
    take_sample()

    recovery = next((r["t"] - restored_at for r in timeline
                     if r["t"] > restored_at and r["backlog"] <= args.recovery_backlog and r["service_up"]), "")
    items = stored_events(table, run_id)
    forwarded = sum(c.total for key, c in rsu_totals.items() if key.endswith(":forwarded"))
    fleet.recorder.write_alerts(run_id)
    RESULTS_DIR.mkdir(exist_ok=True)
    with open(RESULTS_DIR / f"{run_id}_timeline.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, list(timeline[-1]))
        writer.writeheader()
        writer.writerows(timeline)

    return {
        "run_id": run_id,
        "experiment": args.experiment,
        "outage_s": args.outage,
        "generated": fleet.recorder.counters.get("generated", 0),
        "published": fleet.recorder.counters.get("published", 0),
        "rsu_forwarded": forwarded,
        "stored_distinct": len(items),
        "lost": forwarded - len(items),
        "peak_rsu_buffer": peaks["buffered"],
        "peak_queue_visible": peaks["queue"],
        "rsu_retries": sum(c.total for key, c in rsu_totals.items() if key.endswith(":retries")),
        "rsu_dropped": sum(c.total for key, c in rsu_totals.items() if key.endswith(":dropped_buffer")),
        "cloud_duplicates": counters["duplicates"].total,
        "cloud_redelivered": counters["redelivered"].total,
        "cloud_out_of_order": counters["out_of_order"].total,
        "cloud_out_of_order_same_rsu": counters["out_of_order_same_rsu"].total,
        "cloud_out_of_order_cross_rsu": counters["out_of_order_cross_rsu"].total,
        "out_of_order_same_rsu_after_restore":
            counters["out_of_order_same_rsu"].total - at_restore["out_of_order_same_rsu"],
        "out_of_order_cross_rsu_after_restore":
            counters["out_of_order_cross_rsu"].total - at_restore["out_of_order_cross_rsu"],
        "cloud_errors": counters["errors"].total,
        "dlq_depth": sum(queue_depth(dlq_url)),
        "recovery_s": round(recovery, 1) if recovery != "" else "",
        "final_state_ok": final_state_ok(table, items),
        "alerts_received": len(fleet.recorder.alerts),
        "p95_local_alert_ms": fleet.recorder.p95_latency(origin_cloud=False),
        "p95_cloud_alert_ms": fleet.recorder.p95_latency(origin_cloud=True),
        "handovers": fleet.recorder.counters.get("handovers", 0),
        "handover_failures": fleet.recorder.counters.get("handover_failures", 0),
        "p95_handover_ms": p95(fleet.recorder.handover_ms),
    }


def run_experiments(args, targets, base_run_id):
    rows = []
    for index in range(1, args.repeat + 1):
        rows.append(run_experiment_once(args, targets, f"{base_run_id}-{index}"))
        print(json.dumps(rows[-1], indent=2))
    RESULTS_DIR.mkdir(exist_ok=True)
    path = RESULTS_DIR / f"{base_run_id}_summary.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"Resumen guardado en {path}")


def run_fleet(args, targets, run_id):
    fleet = Fleet(args, targets, run_id)
    fleet.start()
    print(f"Ejecución {run_id}: {args.vehicles} vehículos. Ctrl+C para terminar.")
    try:
        if args.duration:
            time.sleep(args.duration)
        else:
            while True:
                time.sleep(1)
    except KeyboardInterrupt:
        pass
    fleet.stop()
    fleet.recorder.write_alerts(run_id)
    print(json.dumps(fleet.recorder.summary(), indent=2))


def parse_args():
    parser = argparse.ArgumentParser(description="Simulador V2X")
    parser.add_argument("--local", action="store_true", help="usa contenedores locales y LocalStack")
    parser.add_argument("--vehicles", type=int, default=5)
    parser.add_argument("--rate", type=float, default=1.5, help="segundos entre eventos de cada vehículo")
    parser.add_argument("--critical-rate", type=float, default=0.15, help="fracción de eventos no rutinarios")
    parser.add_argument("--duration", type=float, default=0, help="segundos de ejecución (0 = indefinido)")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--delay-ms", type=float, default=0)
    parser.add_argument("--loss", type=float, default=0, help="probabilidad de pérdida (0-1)")
    parser.add_argument("--duplicate", type=float, default=0, help="probabilidad de duplicado (0-1)")
    parser.add_argument("--reorder", type=float, default=0, help="probabilidad de desorden (0-1)")
    parser.add_argument("--burst", type=int, default=0, help="eventos extra en cada ráfaga")
    parser.add_argument("--burst-period", type=float, default=10, help="segundos entre ráfagas")
    parser.add_argument("--experiment", choices=["disconnect", "failure"])
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--warmup", type=float, default=20)
    parser.add_argument("--outage", type=float, default=60)
    parser.add_argument("--cooldown", type=float, default=30)
    parser.add_argument("--drain-timeout", type=float, default=180)
    parser.add_argument("--recovery-backlog", type=int, default=10,
                        help="eventos pendientes por debajo de los cuales se considera recuperado")
    parser.add_argument("--ssh-key", help="clave SSH del laboratorio, para el experimento 'failure' en AWS")
    return parser.parse_args()


def main():
    args = parse_args()
    run_id = args.run_id or f"{args.experiment or 'run'}-{time.strftime('%Y%m%d-%H%M%S')}"
    if args.local:
        os.environ["AWS_ENDPOINT_URL"] = LOCALSTACK_URL
        os.environ["AWS_ACCESS_KEY_ID"] = "test"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
        os.environ.pop("AWS_SESSION_TOKEN", None)
        ensure_local_resources()
        targets = local_targets()
    else:
        targets = aws_targets(args)
    if args.experiment:
        run_experiments(args, targets, run_id)
    else:
        run_fleet(args, targets, run_id)


if __name__ == "__main__":
    main()

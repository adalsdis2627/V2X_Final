import time
import json
import threading
from flask import Flask, jsonify, render_template_string
import boto3
 
app = Flask(__name__)

SQS_REGION = 'us-east-1'
QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/933596053553/V2X_Cola'

vehicles_state = {}

def sqs_listener():
    sqs = boto3.client('sqs', region_name=SQS_REGION)
    print("Esperando eventos de la cola SQS")
    
    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=1
            )
            messages = response.get('Messages', [])
            for msg in messages:
                body = json.loads(msg['Body'])
                veh_id = body.get('source_id')
                payload = body.get('payload', {})
                
                active_events_list = body.get('active_events', [body.get('event_type', 'normal telemetry')])
                
                current_time = time.time() * 1000
                formatted_time = time.strftime("%H:%M:%S")

                if veh_id not in vehicles_state:
                    vehicles_state[veh_id] = {
                        "id": veh_id,
                        "x": payload.get("x", 50),
                        "y": payload.get("y", 80),
                        "lane": payload.get("lane", 1),
                        "speed": payload.get("speed", 0),
                        "events": [],
                        "timestamp": current_time,
                        "last_update": formatted_time
                    }

                veh = vehicles_state[veh_id]
                veh["x"] = payload.get("x", veh["x"])
                veh["y"] = payload.get("y", veh["y"])
                veh["lane"] = payload.get("lane", veh["lane"])
                veh["speed"] = payload.get("speed", veh["speed"])
                veh["timestamp"] = current_time
                veh["last_update"] = formatted_time

                formatted_events = []
                for ev_name in active_events_list[:3]:
                    formatted_events.append({
                        "type": ev_name,
                        "timestamp": current_time
                    })
                
                veh["events"] = formatted_events
                
                sqs.delete_message(
                    QueueUrl=QUEUE_URL,
                    ReceiptHandle=msg['ReceiptHandle']
                )
        except Exception as e:
            print(f"Error en SQS: {e}")
        time.sleep(0.2)

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
    </style>
</head>
<body>
    <h1> Circuito de 4 Carriles</h1>
    <div id="canvas-container"><canvas id="map" width="700" height="400"></canvas></div>
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
        setInterval(updateDashboard, 1000);
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/vehicles')
def get_vehicles():
    return jsonify(vehicles_state)

if __name__ == '__main__':
    t = threading.Thread(target=sqs_listener, daemon=True)
    t.start()
    app.run(host='0.0.0.0', port=5000)

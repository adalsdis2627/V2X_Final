# Pruebas en local con LocalStack

Guía para ejecutar el sistema completo en el portátil, sin AWS, y ensayar los dos experimentos antes de lanzarlos en el laboratorio. Usa las mismas imágenes Docker que AWS; solo cambian las variables de entorno.

**Qué no valida.** IAM, IMDS, security groups, DNS públicos ni la política real de reintentos de SNS. Los tiempos medidos aquí tampoco sirven como resultados de la memoria: todo corre en la misma máquina, sin latencia de red real. Los resultados definitivos salen de AWS (ver `EXPERIMENTOS_AWS.md`).

## 1. Requisitos
- Docker Desktop en marcha (`docker ps` responde sin error).
- Python 3.10 o superior, con el entorno virtual `.venv` creado y las dependencias instaladas según el apartado **"Entorno virtual de Python"** del `README.md` (solo la primera vez).
- Puertos libres en el portátil: 4566 (LocalStack), 5000 (dashboard), 1883 y 1884 (MQTT de las RSU), 8081 y 8082 (HTTP de las RSU).

Los comandos son para **Git Bash**, desde la carpeta del proyecto, porque los de Docker usan sintaxis de bash (`ENVS=...`, `\`). Antes de los comandos `python` y `pytest`, activar el entorno en esa terminal:

```bash
source .venv/Scripts/activate
```

Los comandos `python` y `pytest` también funcionan tal cual en PowerShell o CMD con el entorno activado allí (`.venv\Scripts\Activate.ps1` o `.venv\Scripts\activate.bat`).

## 2. Pruebas unitarias
Primero, comprobar que la lógica está bien. No necesitan Docker:

```bash
pytest test_v2x.py
```

Deben pasar las 57. Si alguna falla, no tiene sentido seguir: el sistema completo fallará igual.

## 3. Levantar el sistema

```bash
docker network create v2x
docker run -d --name localstack --network v2x -p 4566:4566 -e SERVICES=sqs,sns,dynamodb localstack/localstack:3.8
docker build --target rsu -t v2x-rsu .
docker build --target service -t v2x-service .

ENVS="-e AWS_ENDPOINT_URL=http://localstack:4566 -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test -e AWS_DEFAULT_REGION=us-east-1"
docker run -d --name v2x-service --network v2x -p 5000:5000 $ENVS v2x-service
docker run -d --name rsu1 --network v2x -p 1883:1883 -p 8081:8080 $ENVS -e RSU_ID=rsu-1 -e PUBLIC_HOST=rsu1 v2x-rsu
docker run -d --name rsu2 --network v2x -p 1884:1883 -p 8082:8080 $ENVS -e RSU_ID=rsu-2 -e PUBLIC_HOST=rsu2 v2x-rsu
```

La cola, el topic y la tabla los crea el simulador al lanzarse con `--local`. Hasta entonces, el servicio y las RSU reintentan solos cada pocos segundos, así que el orden no importa. Si se prefiere crearlos antes:

```bash
AWS_ENDPOINT_URL=http://localhost:4566 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
  python -c "import vehicle_simulator as v; v.ensure_local_resources()"
```

**Comprobar que todo responde:**

```bash
curl http://localhost:5000/api/metrics   # contadores del servicio a 0
curl http://localhost:8081/metrics       # RSU 1, con "link_up": true
curl http://localhost:8082/metrics       # RSU 2
docker logs rsu1                         # debe aparecer "suscrito a ... en http://rsu1:8080/sns"
```

Si la RSU no muestra la línea de suscripción, todavía está esperando a los recursos: lanzar el simulador o el comando anterior y volver a mirar.

## 4. Prueba de humo
Comprueba la ruta completa de eventos y alertas durante 60 s:

```bash
python vehicle_simulator.py --local --duration 60 --run-id local-smoke
```

Abrir `http://localhost:5000` (con `http://`, no `https://`). Debe verse:
- 5 coches moviéndose por los 4 carriles, con el borde del color de su RSU.
- Coches que cambian de color de borde al pasar del carril 2 al 3 o del 4 al 1 (cambio de RSU).
- Alertas en el panel "Alertas recientes".

En la consola del simulador:

| Línea | Significado |
|---|---|
| `[veh-003 -> rsu-1] sudden braking` | Evento publicado por MQTT en esa RSU |
| `<< [veh-002] ALERTA sudden braking (rsu-1) 40 ms` | Alerta local de la RSU: no pasa por la nube, debe tardar decenas de ms |
| `<< [veh-001] ALERTA road hazard (cloud) 110 ms` | Alerta de la nube: RSU → SQS → servicio → SNS → RSU → vehículo. Debe llegar a los vehículos de **ambas** RSU |
| `<> [veh-004] cambio de rsu-1 a rsu-2 (16 ms)` | Cambio de RSU *make-before-break* |

Al terminar imprime un resumen JSON. Comprobar:
- `generated` = `published` (sin inyección de anomalías).
- `handover_failures` = 0.
- `alerts_received` > 0, con alertas de origen `rsu-1`, `rsu-2` y `cloud` en `results/local-smoke_alerts.csv`.

## 5. Inyección de anomalías (opcional)
Requisito funcional del simulador. Se prueban de una en una para ver su efecto:

```bash
python vehicle_simulator.py --local --duration 60 --run-id local-dup --duplicate 0.2
python vehicle_simulator.py --local --duration 60 --run-id local-reorder --reorder 0.2
python vehicle_simulator.py --local --duration 60 --run-id local-loss --loss 0.1
python vehicle_simulator.py --local --duration 60 --run-id local-burst --burst 20 --burst-period 10
python vehicle_simulator.py --local --duration 60 --run-id local-delay --delay-ms 500
```

| Anomalía | Qué debe pasar |
|---|---|
| `--duplicate` | El resumen muestra `injected_duplicates`. La RSU los descarta (`duplicates` en `curl localhost:8081/metrics`) y no llegan a la nube |
| `--reorder` | `injected_reorders` en el resumen. Sube `out_of_order_same_rsu` en `curl localhost:5000/api/metrics`, pero el dashboard sigue mostrando la posición más reciente |
| `--loss` | `injected_loss` en el resumen y `published` < `generated`. El sistema no puede recuperarlos: se perdieron antes de llegar a la RSU |
| `--burst` | La RSU filtra la telemetría sobrante (`filtered` en sus métricas) y no se descartan eventos críticos |
| `--delay-ms` | Las latencias de las alertas suben aproximadamente en ese retardo |

Los contadores de `/metrics` y `/api/metrics` son acumulados desde que arrancó cada contenedor: para comparar, anotar el valor antes y después de cada ejecución.

## 6. Ensayo de los experimentos
Versión corta (unos 2 minutos cada uno) para comprobar que todo funciona antes de AWS:

```bash
python vehicle_simulator.py --local --experiment disconnect --warmup 15 --outage 30 --cooldown 20 --run-id local-disc
python vehicle_simulator.py --local --experiment failure --warmup 15 --outage 30 --cooldown 20 --run-id local-fail
```

Con los valores por defecto (`--warmup 20 --outage 60 --cooldown 30`) se reproduce exactamente lo que se lanzará en AWS. Cada uno se puede repetir con `--repeat 3`.

Para repetir el ensayo, usar otro `--run-id` (por ejemplo, `local-disc-2`) o reiniciar LocalStack: si DynamoDB ya tiene eventos con ese `run_id`, el simulador se detiene antes de empezar. Antes de cada repetición, el simulador también restaura un enlace o un servicio que hayan quedado cortados y espera a que se vacíen los buffers y la cola. El corte se deshace aunque el experimento se interrumpa con Ctrl+C.

| Experimento | Fallo que inyecta |
|---|---|
| `disconnect` | Corta el enlace de la RSU 1 con la nube (`POST /admin/link`). La RSU sigue recibiendo eventos y alertando localmente, pero guarda en su buffer lo que debería ir a SQS y responde 503 a SNS |
| `failure` | `docker stop v2x-service`. Las RSU siguen enviando a SQS, que acumula los mensajes hasta que el servicio vuelve (`docker start`) |

Durante el experimento, el dashboard deja de actualizar la RSU 1 (en `disconnect`) o todo (en `failure`) y se pone al día tras restaurar.

## 7. Interpretar los resultados
Cada repetición genera en `results/`:
- `<run_id>-N_timeline.csv`: una muestra por segundo.
- `<run_id>-N_alerts.csv`: una fila por alerta recibida por cada vehículo.
- `<run_id>_summary.csv`: una fila por repetición (lo mismo que el JSON de la consola).

### Columnas del resumen y valores esperados

| Columna | Qué mide | `disconnect` | `failure` |
|---|---|---|---|
| `generated` / `published` | Eventos creados por los vehículos / publicados por MQTT | Iguales | Iguales |
| `rsu_forwarded` | Eventos que las RSU enviaron a SQS | Menor que `published`: la RSU filtra telemetría | Igual |
| `stored_distinct` | Eventos distintos de esta ejecución en DynamoDB | = `rsu_forwarded` | = `rsu_forwarded` |
| `lost` | `rsu_forwarded − stored_distinct` | **0** | **0** |
| `peak_rsu_buffer` | Máximo de eventos en el buffer de las RSU | Crece durante el corte (decenas) | Casi 0: la RSU sigue enviando |
| `peak_queue_visible` | Máximo de mensajes esperando en SQS | Bajo, con un pico al vaciarse el buffer | Crece durante el corte |
| `rsu_retries` / `rsu_dropped` | Reintentos de envío / eventos descartados por buffer lleno | 0 / 0 (el corte no provoca errores, solo retiene) | 0 / 0 |
| `cloud_duplicates` / `cloud_redelivered` | Eventos repetidos detectados por el servicio / mensajes reentregados por SQS | 0 / 0 | 0-2: mensajes que el servicio tenía en proceso al pararse y que SQS reentrega tras 30 s |
| `cloud_out_of_order` | Eventos que llegaron después de otro más reciente del mismo vehículo | Algunos | 0 o pocos |
| `cloud_out_of_order_same_rsu` | ... y el más reciente vino por la misma RSU (causa: SQS estándar) | ~0 en local; decenas en AWS | ~0 en local; decenas en AWS |
| `cloud_out_of_order_cross_rsu` | ... y el más reciente vino por la otra RSU (causa: cambio de RSU) | ≈ total | ≈ total |
| `out_of_order_*_after_restore` | Lo mismo, contando solo desde la restauración (±1 s) | Same ~0; cross ≈ total | 0 |
| `cloud_errors` / `dlq_depth` | Errores del servicio / mensajes en la DLQ | 0 / 0 | 0 / 0 |
| `recovery_s` | Segundos desde la restauración hasta que quedan ≤ 10 eventos pendientes | Pocos segundos | Pocos segundos |
| `final_state_ok` | El estado guardado de cada vehículo es su evento más reciente | **true** | **true** |
| `p95_local_alert_ms` | p95 de latencia de alertas locales | Decenas de ms: no dependen de la nube | Decenas de ms |
| `p95_cloud_alert_ms` | p95 de latencia de alertas de la nube | Cercano a `--outage`: alertas de eventos retenidos | Cercano a `--outage` |
| `handovers` / `handover_failures` / `p95_handover_ms` | Cambios de RSU, fallidos y su duración | Varios / 0 / decenas de ms | Varios / 0 / decenas de ms |

Ejemplo real de un ensayo corto (30 s de corte): `disconnect` dio 129 reenviados = 129 guardados, 12 desordenados todos de distinta RSU y recuperación en 1,5 s; `failure` dio 143 = 143, 1 duplicado reentregado por SQS y recuperación en 3,6 s. Ambos con `final_state_ok` a true.

### Cómo leer el desorden
`cloud_out_of_order` no es un error: el servicio no sobrescribe el estado con un evento más antiguo, y por eso `final_state_ok` sigue a true. Lo interesante es la causa:
- **Distinta RSU.** Un vehículo cambia a la RSU 2 durante el corte. Sus eventos antiguos se quedan en el buffer de la RSU 1 y llegan a la nube después que los nuevos enviados por la RSU 2. Es el efecto esperado del corte.
- **Misma RSU.** El buffer de la RSU es FIFO, así que dentro de una RSU solo desordena SQS estándar, que no garantiza el orden. Debe ser 0 o muy pocos. Si saliera alto sin `--reorder`, habría un problema en el buffer.

### Cómo leer la línea temporal
`_timeline.csv` tiene las columnas `t` (s desde el inicio), `queue_visible`, `queue_in_flight`, `rsu-1_buffered`, `rsu-2_buffered`, `service_up`, `service_processed` y `backlog` (cola + en vuelo + buffers). Para las gráficas:
- **`disconnect`:** `rsu-1_buffered` sube de forma lineal durante el corte y cae a 0 en pocos segundos al restaurar; `queue_visible` tiene un pico justo después.
- **`failure`:** `service_up` pasa a `False`, `queue_visible` sube durante el corte y baja al volver el servicio, mientras `service_processed` vuelve a empezar desde 0 (contador del contenedor reiniciado) y crece rápido.

### Cómo leer las alertas
En `_alerts.csv`, filtrar por `origin`:
- `rsu-1` / `rsu-2`: alertas locales (frenazo y congestión en el tramo). Siguen llegando durante el corte.
- `cloud`: `road hazard` y congestión entre RSU. Las de eventos retenidos durante el corte llegan con latencia cercana a la duración del corte: la caducidad de alertas no está implementada, así que es lo esperado.

## 8. Limpieza

```bash
docker rm -f rsu1 rsu2 v2x-service localstack
docker network rm v2x
```

LocalStack no guarda nada entre arranques: la próxima vez se parte de cero. Los CSV de `results/` sí se conservan; borrar los de ensayo antes de los experimentos definitivos para no mezclarlos.

## Problemas frecuentes

| Síntoma | Causa y solución |
|---|---|
| `ModuleNotFoundError: No module named 'paho'` (o `boto3`, `pytest`) | El entorno virtual no está activado en esa terminal. Activarlo o usar `.venv/Scripts/python.exe` |
| `port is already allocated` | Quedan contenedores de una prueba anterior. Ejecutar la limpieza |
| `ConnectionRefusedError` al lanzar el simulador | Las RSU no han arrancado aún o no se publicaron los puertos 1883/1884. Ver `docker ps` y `docker logs rsu1` |
| No llegan alertas `cloud` | La RSU no se ha suscrito a SNS. Ver `docker logs rsu1`; si no hay línea de suscripción, reiniciar con `docker restart rsu1 rsu2` |
| El dashboard da `ERR_SSL_PROTOCOL_ERROR` | El navegador fuerza HTTPS. Escribir `http://localhost:5000` |
| `DynamoDB ya tiene N eventos con run_id ...` | Ese `--run-id` ya se usó en un ensayo anterior. Usar otro o reiniciar LocalStack |
| `failure` falla con `docker: command not found` | El simulador llama a `docker` directamente: lanzarlo desde una terminal donde funcione `docker ps` |

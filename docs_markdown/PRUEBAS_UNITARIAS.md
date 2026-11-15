# Pruebas unitarias (`test_v2x.py`)

Descripción de cada prueba de `test_v2x.py`: qué comprueba y por qué importa. Son **53 funciones de prueba y 57 casos**, porque dos funciones están parametrizadas. Se ejecutan en menos de un segundo y no necesitan AWS, MQTT, Docker ni red.

## Cómo ejecutarlas

Con el entorno virtual activo (ver "Entorno virtual de Python" en el `README.md`):

```bash
pip install -r requirements.txt pytest
pytest test_v2x.py          # resumen
pytest test_v2x.py -v       # una línea por caso
pytest test_v2x.py -k sns   # solo las pruebas cuyo nombre contiene "sns"
```

El flujo de despliegue (`.github/workflows/deploy.yml`) las ejecuta antes de construir las imágenes. Si alguna falla, no se despliega nada.

## Cómo se aíslan las dependencias externas

La lógica de la RSU (`Rsu`) y la del servicio (`V2XService`) reciben como funciones sus operaciones de entrada y salida. Las pruebas les pasan sustitutos en memoria:

| Sustituto | Sustituye a | Comportamiento |
|---|---|---|
| `FakeTable` | Tabla de DynamoDB | Reproduce las dos escrituras condicionales del servicio: `put_item` falla si la clave ya existe, y `update_item` falla si el `event_time` guardado no es anterior. Lanza el mismo `ConditionalCheckFailedException` que boto3 |
| `harness` (fixture) | RSU con sus conexiones | Una `Rsu("rsu-1")` cuyos envíos a SQS, publicaciones MQTT y confirmaciones de SNS se guardan en listas (`sent`, `published`, `confirmed`). Incluye el cliente de pruebas de Flask para llamar a `/sns` y `/admin/link` |
| `Clock` | Reloj de la RSU | Reloj controlable (`clock.now = 31`) para probar ventanas de tiempo sin esperar |
| `service` (fixture) | V2X-SERVICE | Un `V2XService` con `FakeTable` y una lista que recoge lo publicado en SNS |
| `FakeMqttClient` | Cliente paho-mqtt | Registra conexión, suscripciones, publicaciones y cierre. Al arrancar llama a `on_connect` como lo haría el broker |
| `fake_http` (fixture) | Llamadas HTTP del simulador | Sustituye `http_json` y `try_http_json`: devuelve métricas configurables de las RSU y del servicio, y anota cada orden enviada |

Funciones auxiliares:
- `make_event`: evento válido con los seis campos del esquema.
- `via_rsu`: el mismo evento con un `rsu_id` y un segundo concreto.
- `congestion`: evento de congestión de un vehículo y una RSU dados.
- `notification`: mensaje de SNS que envuelve una alerta.
- `sns_post`: envía a `/sns` con `Content-Type: text/plain`, como hace SNS.

## 1. Validación y filtrado en la RSU

| Prueba | Qué comprueba |
|---|---|
| `test_invalid_events_are_rejected` (4 casos) | `validate_event` rechaza un tipo de evento desconocido, un `source_id` con caracteres no permitidos (`<script>`), una fecha que no es ISO 8601 y un `payload` que no es un objeto |
| `test_missing_field_is_rejected` | Un evento sin `location` se rechaza con el motivo `missing location` |
| `test_malformed_json_is_rejected` | Un mensaje MQTT que no es JSON se rechaza sin romper la RSU |
| `test_duplicate_event_is_dropped` | El mismo `event_id` recibido dos veces solo se acepta una vez y solo entra una vez en el buffer. Es lo que neutraliza los reenvíos de MQTT con QoS 1 |
| `test_lru_forgets_oldest` | El conjunto `SeenIds` tiene capacidad acotada y olvida los identificadores más antiguos, así que la memoria de la RSU no crece sin límite |
| `test_normal_telemetry_is_rate_limited_per_vehicle` | La telemetría normal se limita a un evento cada 3 s **por vehículo**: el segundo evento de `veh-001` se filtra, el de `veh-002` no, y pasados 3,1 s se vuelve a aceptar |
| `test_critical_events_are_never_rate_limited` | Tres `road hazard` seguidos se aceptan todos: el filtrado nunca descarta eventos críticos |
| `test_sudden_braking_alerts_locally_without_cloud` | Con el enlace a AWS cortado, un frenazo genera una alerta local en `v2x/rsu-1/alerts` con origen `rsu-1`, sin enviar nada a SQS. Es el requisito de alerta local sin depender de la nube |

## 2. Buffer y reenvío

| Prueba | Qué comprueba |
|---|---|
| `test_buffer_holds_events_while_link_is_down_and_keeps_order` | Con el enlace cortado no se envía nada a SQS. Al restaurarlo, los eventos salen en el mismo orden en que llegaron (FIFO) |
| `test_failed_send_is_retried_with_backoff` | Si SQS falla, el evento sigue en el buffer y se reintenta con un retardo que se duplica (1 s, 2 s). Tras el primer envío correcto, el buffer queda vacío, se cuentan 2 reintentos y el retardo vuelve a 1 s |
| `test_full_buffer_discards_normal_telemetry_first` | Con el buffer lleno (capacidad 2 en la prueba), un evento crítico nuevo desplaza a la telemetría normal y no a otro evento crítico. Se cuenta un descarte |

## 3. SNS por HTTP (`/sns` de la RSU)

| Prueba | Qué comprueba |
|---|---|
| `test_sns_subscription_confirmation_uses_token` | Un `SubscriptionConfirmation` se confirma con su `Token` y responde 200 |
| `test_sns_notification_in_text_plain_is_relayed_by_mqtt` | Una notificación con `Content-Type: text/plain`, el que usa SNS, se interpreta bien y la alerta se publica en `v2x/rsu-1/alerts` |
| `test_sns_duplicate_alert_is_relayed_once` | Si SNS entrega dos veces la misma alerta (reintento), la RSU responde 200 a ambas pero solo la retransmite una vez |
| `test_sns_alert_for_other_rsu_is_ignored` | Una alerta con `scope: rsu-2` no se retransmite en la RSU 1 |
| `test_sns_foreign_topic_is_forbidden` | Un mensaje de un topic distinto de `v2x-alerts` se rechaza con 403 |
| `test_sns_returns_503_when_link_is_down_so_sns_retries` | Con el enlace cortado (`/admin/link {"up": false}`) la RSU responde 503 para que SNS reintente. Al restaurarlo, el reintento se acepta y la alerta se publica una sola vez |

## 4. V2X-SERVICE

| Prueba | Qué comprueba |
|---|---|
| `test_same_event_twice_is_stored_once` | Un evento reentregado por SQS (`receive_count=2`) se guarda una sola vez en DynamoDB, cuenta como duplicado y como reentrega |
| `test_malformed_message_raises_so_it_reaches_the_dlq` | Un mensaje que no es JSON, o al que le faltan campos imprescindibles, lanza una excepción. Así no se borra de SQS y, tras 5 recepciones, acaba en la DLQ |
| `test_older_event_does_not_overwrite_vehicle_state` | Si llega un evento más antiguo después de otro más reciente, el estado del vehículo (en DynamoDB y en memoria) conserva el más reciente y se cuenta un evento desordenado |

## 5. Clasificación del desorden

| Prueba | Qué comprueba |
|---|---|
| `test_out_of_order_within_the_same_rsu_is_classified` | Un evento adelantado por otro más reciente que llegó por la misma RSU se cuenta como desorden de misma RSU (causa: SQS estándar) |
| `test_out_of_order_after_rsu_change_is_classified_as_cross_rsu` | Si el evento más reciente llegó por la RSU 2 y el antiguo por la RSU 1, como ocurre tras un cambio de RSU durante un corte, se cuenta como desorden de distinta RSU |
| `test_duplicate_is_not_counted_as_out_of_order` | Un duplicado de un evento antiguo cuenta como duplicado, no como desorden |
| `test_out_of_order_total_equals_same_plus_cross` | En una secuencia mixta de seis eventos, el total de desorden (3) es la suma de misma RSU (2) y distinta RSU (1) |

## 6. Reglas de alerta de la nube

| Prueba | Qué comprueba |
|---|---|
| `test_road_hazard_generates_one_alert_for_all_rsus` | Un `road hazard` genera una alerta con origen `cloud` y ámbito `all`. Reprocesar el mismo evento no la publica otra vez |
| `test_alert_lost_before_publish_is_recovered_on_redelivery` | Si SNS falla al publicar, el procesado lanza una excepción. Cuando SQS reentrega el evento, ya guardado y por tanto duplicado, la alerta se publica. Ninguna alerta se pierde por un fallo entre guardar y publicar |
| `test_cloud_congestion_needs_three_vehicles_from_two_rsus_and_fires_once` | La congestión de nube exige 3 vehículos distintos de al menos 2 RSU en la misma ventana. Los avisos repetidos de un vehículo no cuentan, y la alerta se dispara una sola vez por ventana |
| `test_cloud_ignores_congestion_confined_to_one_rsu` | Tres vehículos de una sola RSU no generan alerta de nube: esa congestión la alerta la propia RSU |
| `test_congestion_alert_id_is_deterministic` | Dos motores independientes generan el mismo `alert_id` para la misma ventana, aunque los eventos sean distintos. Por eso un reprocesado nunca crea una alerta nueva |
| `test_congestion_in_different_windows_does_not_accumulate` | Avisos separados por más de 30 s caen en ventanas distintas y no se suman |

## 7. Congestión local en la RSU (ventana deslizante)

| Prueba | Qué comprueba |
|---|---|
| `test_rsu_alerts_congestion_in_its_segment_with_two_vehicles` | Dos vehículos distintos en 30 s generan una alerta local con origen y ámbito `rsu-1`, y se cuenta en las métricas |
| `test_rsu_congestion_ignores_repeated_reports_from_one_vehicle` | Tres avisos del mismo vehículo no generan alerta |
| `test_rsu_congestion_window_slides` | Un aviso de hace más de 30 s sale de la ventana, y dos avisos dentro de la misma ventana sí alertan |
| `test_rsu_congestion_realerts_at_most_once_per_window` | Mientras la congestión persiste, la RSU vuelve a alertar como mucho una vez cada 30 s |
| `test_congestion_events_are_still_forwarded_to_the_cloud` | Los eventos de congestión se encolan para la nube aunque generen alerta local, porque la nube los necesita para la congestión entre RSU |

## 8. API y utilidades del servicio

| Prueba | Qué comprueba |
|---|---|
| `test_percentile` | `percentile_95` devuelve `None` sin datos y el valor esperado con 100 muestras |
| `test_api_endpoints` | `/api/vehicles`, `/api/alerts`, `/api/metrics` y `/` responden con los datos del evento procesado. `/` comprueba además que la plantilla del dashboard se encuentra |

## 9. Simulador

| Prueba | Qué comprueba |
|---|---|
| `test_each_rsu_covers_half_of_the_circuit` | Los carriles 1-2 corresponden a la RSU 1 y los 3-4 a la RSU 2 |
| `test_simulated_event_is_accepted_by_rsu_schema` | Los eventos del simulador pasan la validación de la RSU, tienen un `event_id` distinto cada vez y un `event_time` en UTC actual. Detecta si el simulador y la RSU dejan de estar de acuerdo en el esquema |
| `test_injector_loss` | Con pérdida 1, el inyector no publica nada |
| `test_injector_duplicate` | Con duplicación 1, publica dos copias idénticas |
| `test_injector_reorder_swaps_consecutive_messages` | Con desorden 1, retiene un mensaje y lo publica después del siguiente |

## 10. Cambio de RSU (*handover*)

| Prueba | Qué comprueba |
|---|---|
| `test_vehicle_starts_on_the_rsu_of_its_lane` | Un vehículo en el carril 3 se conecta a la RSU 2 y se suscribe a sus alertas |
| `test_staying_in_the_same_half_does_not_change_rsu` | Pasar del carril 1 al 2 no cambia de RSU ni abre conexiones nuevas |
| `test_handover_connects_to_new_rsu_before_closing_the_old_one` | Al pasar del carril 2 al 3, el vehículo se conecta y suscribe a la RSU 2, publica por ella y solo después cierra la conexión con la RSU 1 (*make-before-break*). El cambio queda registrado |
| `test_circuit_wraps_back_to_the_first_rsu` | Del carril 4 se vuelve al 1 y, por tanto, a la RSU 1 |
| `test_held_message_leaves_through_the_old_rsu_on_handover` | Un mensaje retenido por la inyección de desorden sale por la RSU a la que iba dirigido cuando el vehículo cambia de RSU |
| `test_failed_handover_keeps_old_rsu_and_retries` | Si la RSU nueva no responde, el vehículo sigue publicando por la antigua, cuenta el fallo y lo reintenta con éxito en el siguiente paso |
| `test_vehicle_ignores_repeated_alert_heard_from_both_rsus` | Una alerta recibida por las dos conexiones durante un cambio se registra una sola vez |

## 11. Control de experimentos

| Prueba | Qué comprueba |
|---|---|
| `test_cumulative_counter_survives_restart` | `CumulativeCounter` sigue sumando aunque el contador de origen vuelva a cero, como ocurre al reiniciar el servicio en `failure`, e ignora las muestras sin dato |
| `test_fault_is_undone_even_if_the_experiment_is_interrupted` (2 casos) | Si se pulsa Ctrl+C durante el corte, el fallo se deshace igualmente. En `disconnect` se manda `{"up": false}` y después `{"up": true}`; en `failure`, `stop` y después `start` |
| `test_leftover_fault_from_an_interrupted_experiment_is_restored` | Antes de empezar, si la RSU 1 sigue con el enlace cortado y el servicio parado, el simulador restaura los dos |
| `test_clean_system_is_left_untouched` | Si todo está en buen estado, las comprobaciones previas no envían ninguna orden |

## Qué no cubren

Son pruebas de lógica. No comprueban la integración con los servicios reales, que se verifica en local con LocalStack (`PRUEBAS_LOCAL.md`) y en AWS (`EXPERIMENTOS_AWS.md`):

- conexión real a MQTT, SQS, SNS y DynamoDB;
- imágenes Docker, red, IAM, IMDS y grupos de seguridad;
- tiempos y latencias.

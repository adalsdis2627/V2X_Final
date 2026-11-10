# V2X: monitorización y alertas distribuidas en AWS

Los vehículos simulados publican eventos por MQTT en su RSU. Cada RSU filtra los eventos, genera alertas locales y reenvía a la cola SQS `v2x-events` los que interesan a la nube. V2X-SERVICE los consume, los guarda en DynamoDB, detecta condiciones de alerta y las publica en el topic SNS `v2x-alerts`. SNS las entrega por HTTP a ambas RSU, que las retransmiten a sus vehículos.

| Fichero | Contenido |
|---|---|
| `vehicle_simulator.py` | Simulador de vehículos, inyección de anomalías y experimentos (se ejecuta en el portátil) |
| `rsu.py` | RSU: broker MQTT, validación, filtrado, buffer, alertas locales y endpoint HTTP para SNS |
| `v2x_service.py` | Servicio cloud: SQS, DynamoDB, reglas de alerta, SNS y dashboard |
| `Dockerfile` | Dos imágenes desde un mismo fichero: `--target rsu` y `--target service` |
| `test_v2x.py` | Pruebas unitarias (pytest) |
| `.github/workflows/deploy.yml` | Pipeline de pruebas, imágenes, infraestructura y despliegue |
| `INSTRUCTIONS_AWS.md` | Los mismos pasos que el pipeline, para hacerlos a mano |

## 1. Qué despliega `deploy.yml`

El workflow se ejecuta en cada push a `main` y también a mano desde **Actions → Run workflow**.

| Recurso | Nombre exacto | Configuración |
|---|---|---|
| Imágenes ECR | `v2x-rsu`, `v2x-service` | Repositorios privados; cada imagen lleva como etiqueta el SHA del commit |
| Cola SQS (DLQ) | `v2x-events-dlq` | Estándar, valores por defecto |
| Cola SQS | `v2x-events` | Estándar, visibility timeout 30 s, retención 86400 s, redrive a la DLQ con `maxReceiveCount=5` |
| Topic SNS | `v2x-alerts` | Estándar, política de entrega HTTP con 10 reintentos lineales (de 1 a 20 s) |
| Tabla DynamoDB | `v2x` | Clave de partición `pk` (String), capacidad bajo demanda |
| Security group | `v2x-sg` | Entrada TCP 22 (SSH), 1883 (MQTT), 8080 (SNS HTTP) y 5000 (dashboard) desde `0.0.0.0/0` |
| EC2 | `v2x-rsu1`, `v2x-rsu2`, `v2x-service` (etiqueta `Name`) | `t3.small`, Amazon Linux 2023 x86_64, key pair `vockey`, perfil `LabInstanceProfile`, hop limit de metadatos 2, Docker instalado por user-data |
| Contenedores | `v2x-service` (puerto 5000); `rsu1` y `rsu2` (puertos 1883 y 8080) | Uno por instancia, con `--restart unless-stopped` |
| Suscripciones SNS | Una HTTP por RSU: `http://<DNS público>:8080/sns` | No las crea el workflow: cada RSU se suscribe y confirma sola al arrancar |

El workflow necesita estos secretos en **Settings → Secrets and variables → Actions**:

| Secreto | Valor |
|---|---|
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | Credenciales de **AWS Details** en el Learner Lab. Caducan con cada sesión |
| `LAB_SSH_KEY` | Contenido completo de `labsuser.pem`, incluidas las líneas `BEGIN` y `END` |

## 2. Orden de ejecución y errores posibles

Cada paso solo se ejecuta si el anterior termina bien. Los pasos son idempotentes: crean un recurso solo si no existe. Si un paso falla, los recursos ya creados se quedan como están, y basta con corregir la causa y relanzar el workflow.

### Paso 1. Pruebas
Prepara Python 3.10, instala `requirements.txt` y ejecuta `pytest test_v2x.py`.

| Error | Causa y solución |
|---|---|
| Falla alguna prueba | Un cambio ha roto la lógica. Reprodúcelo en local con `pytest test_v2x.py` |
| Falla `pip install` | Versión fijada en `requirements.txt` inexistente o caída temporal de PyPI. Relanza o corrige la versión |

### Paso 2. Credenciales e inicio de sesión en ECR
Configura las credenciales de AWS con los secretos e inicia sesión en el registro ECR de la cuenta.

| Error | Causa y solución |
|---|---|
| `ExpiredToken`, `InvalidClientTokenId` o `The security token included in the request is invalid` | Credenciales caducadas o mal copiadas. Actualiza los 3 secretos `AWS_*` |
| `Credentials could not be loaded` | Falta algún secreto o tiene otro nombre |

### Paso 3. Repositorios e imágenes (ECR)
Crea `v2x-rsu` y `v2x-service` si no existen, y para cada uno construye su *target* del `Dockerfile` y publica la imagen con el SHA del commit.

| Error | Causa y solución |
|---|---|
| Falla `docker build` en `apt-get` o `pip` | Problema de red o de un paquete. Relanza; si persiste, revisa el `Dockerfile` |
| `AccessDenied` en `create-repository` o en `docker push` | Credenciales caducadas a mitad de ejecución. Actualiza los secretos y relanza |

### Paso 4. Mensajería y persistencia (SQS, SNS, DynamoDB)
En este orden: crea la DLQ, obtiene su ARN, crea `v2x-events` enlazada a la DLQ, crea el topic, le aplica la política de entrega HTTP y crea la tabla si no existe. El ARN del topic se guarda para el paso 6.

| Error | Causa y solución |
|---|---|
| `QueueAlreadyExists` | `v2x-events` existe con otros atributos, por ejemplo si se creó en la consola con valores por defecto. Borra la cola o iguala sus atributos a los de la tabla de la sección 1 |
| `QueueDeletedRecently` | Se borró una cola con ese nombre hace menos de 60 s. Espera un minuto y relanza |
| `InvalidParameter` en `set-topic-attributes` | La política de entrega se ha editado con valores no válidos. Restaura la del workflow |
| El workflow pasa pero el servicio falla con `ValidationException` | La tabla `v2x` ya existía con otra clave, y el workflow no la recrea. Bórrala y relanza |

### Paso 5. Red e instancias EC2
Crea el security group si no existe y abre sus puertos, ignorando las reglas que ya existan. Busca la AMI más reciente de Amazon Linux 2023. Para cada instancia:
- Si no hay ninguna con esa etiqueta, la crea.
- Si está parada, la arranca.
- Si ya está en marcha, la reutiliza.

Después espera a que esté *running* y guarda su DNS público para el paso 6.

| Error | Causa y solución |
|---|---|
| `InvalidKeyPair.NotFound` | No existe el key pair `vockey` en la región. Comprueba que trabajas en `us-east-1` |
| `InvalidParameterValue` o `AccessDenied` con `LabInstanceProfile` | El perfil no existe o el laboratorio no lo permite. Comprueba en IAM que existe `LabInstanceProfile` |
| `VPCIdNotSpecified` o fallo al crear el security group | La cuenta no tiene VPC por defecto. Créala desde **VPC → Actions → Create default VPC** |
| Límite de instancias o de tipo | El laboratorio limita el número de instancias y sus tipos. Termina instancias sobrantes |
| El DNS público sale vacío | La subred no asigna IP pública. Activa *Auto-assign public IPv4* en la subred por defecto |
| Contenedores sin credenciales (se ve en el paso 6 o después) | Se reutilizó una instancia creada a mano sin `LabInstanceProfile`, sin hop limit 2 o sin Docker, y el workflow no corrige instancias existentes. Termínala y relanza, o corrígela como indica `INSTRUCTIONS_AWS.md` |
| Aparece una instancia de más | Había varias instancias con la misma etiqueta y el workflow usa solo la primera. Termina las sobrantes |

### Paso 6. Despliegue de contenedores
1. Guarda `LAB_SSH_KEY` en un fichero temporal.
2. Borra las suscripciones SNS HTTP confirmadas, que apuntan a DNS de sesiones anteriores.
3. Para `v2x-service`, `rsu1` y `rsu2`, en ese orden: espera por SSH hasta 400 s a que Docker responda, inicia sesión en ECR desde la instancia, descarga la imagen, borra el contenedor anterior y arranca el nuevo. Las RSU reciben `RSU_ID` y `PUBLIC_HOST`.
4. Escribe en el resumen del job las URL del dashboard y de las RSU.

| Error | Causa y solución |
|---|---|
| `Docker no disponible en <host>` | En 400 s no se pudo ejecutar `docker info` por SSH. Las causas son `LAB_SSH_KEY` incorrecta o incompleta, puerto 22 cerrado, o una instancia sin Docker (creada a mano sin user-data). Prueba a mano `ssh -i labsuser.pem ec2-user@<host>` |
| `no basic auth credentials` o `denied` en `docker pull` | La instancia no tiene `LabInstanceProfile`, así que no puede autenticarse en ECR |
| `port is already allocated` | Hay otro contenedor usando el puerto en la instancia. Bórralo con `sudo docker rm -f <nombre>` |

**El workflow termina en verde cuando los contenedores han arrancado, no cuando el sistema funciona.** No comprueba que las RSU se hayan suscrito a SNS ni que los servicios accedan a AWS. Esas comprobaciones están en la sección 3.

## 3. Acciones que siguen siendo manuales

### En cada sesión del Learner Lab
1. Pulsa **Start Lab** y espera al indicador verde.
2. Copia las credenciales nuevas de **AWS Details** en los 3 secretos `AWS_*` de GitHub y en `C:\Users\<usuario>\.aws\credentials`. Las del portátil las usa el simulador para encontrar las instancias y leer métricas.
3. Lanza el workflow desde **Actions → Run workflow**, aunque no haya cambios. Al reiniciar el laboratorio las instancias cambian de DNS, y el workflow vuelve a suscribir las RSU con la dirección nueva.
4. Comprueba el despliegue:
   - Las 2 suscripciones del topic `v2x-alerts` tienen un ARN completo y no `PendingConfirmation`. Se ve en la consola de SNS o con `aws sns list-subscriptions-by-topic`.
   - `http://<DNS de cada RSU>:8080/metrics` responde con `"link_up": true`.
   - El dashboard responde en `http://<DNS de v2x-service>:5000`.

`LAB_SSH_KEY` solo se configura una vez, salvo que el laboratorio genere una clave nueva.

### Simulador de vehículos
Se ejecuta en el portátil, con Python 3.10 o superior:

```bash
pip install -r requirements.txt
python vehicle_simulator.py --duration 120
```

Encuentra las RSU solo, por las etiquetas de las instancias. Por defecto simula 5 vehículos: 1 a 3 en `rsu-1` y 4 y 5 en `rsu-2`. Muestra las alertas recibidas con su latencia y las guarda en `results/<run_id>_alerts.csv`.

| Opción | Uso |
|---|---|
| `--vehicles`, `--rate`, `--critical-rate`, `--duration` | Número de vehículos, segundos entre eventos, fracción de eventos no rutinarios y duración (0 = hasta Ctrl+C) |
| `--delay-ms`, `--loss`, `--duplicate`, `--reorder` | Retardo, pérdida, duplicación y desorden inyectados en origen (probabilidades de 0 a 1) |
| `--burst`, `--burst-period` | Eventos extra por ráfaga y segundos entre ráfagas |
| `--local` | Usa los contenedores locales y LocalStack en lugar de AWS |

### Experimentos
```bash
python vehicle_simulator.py --experiment disconnect --repeat 3
python vehicle_simulator.py --experiment failure --repeat 3 --ssh-key labsuser.pem
```

- **`disconnect`:** corta el enlace de `rsu-1` con AWS durante 60 s.
- **`failure`:** detiene `v2x-service` durante 60 s por SSH.

Los tiempos se ajustan con `--warmup`, `--outage` y `--cooldown`. Por cada repetición se generan `results/<run_id>_timeline.csv` y `_alerts.csv`, y al final un `_summary.csv` con todas.

En Windows, `ssh` rechaza la clave si otros usuarios pueden leerla. Si aparece `UNPROTECTED PRIVATE KEY FILE`, ejecuta en PowerShell:

```powershell
icacls labsuser.pem /inheritance:r /grant:r "$($env:USERNAME):(R)"
```

### Pruebas en local (opcional, sin AWS)
Requiere Docker Desktop. En Git Bash, desde la carpeta del proyecto:

```bash
docker network create v2x
docker run -d --name localstack --network v2x -p 4566:4566 -e SERVICES=sqs,sns,dynamodb localstack/localstack:3.8
docker build --target rsu -t v2x-rsu .
docker build --target service -t v2x-service .
ENVS="-e AWS_ENDPOINT_URL=http://localstack:4566 -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test -e AWS_DEFAULT_REGION=us-east-1"
docker run -d --name v2x-service --network v2x -p 5000:5000 $ENVS v2x-service
docker run -d --name rsu1 --network v2x -p 1883:1883 -p 8081:8080 $ENVS -e RSU_ID=rsu-1 -e PUBLIC_HOST=rsu1 v2x-rsu
docker run -d --name rsu2 --network v2x -p 1884:1883 -p 8082:8080 $ENVS -e RSU_ID=rsu-2 -e PUBLIC_HOST=rsu2 v2x-rsu
python vehicle_simulator.py --local --duration 60
```

Con `--local`, el simulador crea en LocalStack la cola, el topic y la tabla. El dashboard queda en `http://localhost:5000`. Para limpiar: `docker rm -f rsu1 rsu2 v2x-service localstack && docker network rm v2x`.

### Otras tareas
<!-- - **Memoria:** documentar esquemas de eventos y alertas, configuración de la mensajería, uso de HTTP en lugar de HTTPS entre SNS y las RSU, procedimiento de despliegue y resultados de los experimentos. -->
- **Al terminar cada sesión:** pulsar **End Lab**, que detiene las instancias. Los recursos se conservan.
- **Después de la defensa:** borrar los recursos a mano, porque el workflow no lo hace. Hay que terminar las 3 instancias y borrar el security group, las colas, el topic, la tabla y los repositorios ECR.

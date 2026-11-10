FROM python:3.10-slim AS base
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# docker build --target rsu -t v2x-rsu .
FROM base AS rsu
RUN apt-get update \
    && apt-get install -y --no-install-recommends mosquitto \
    && rm -rf /var/lib/apt/lists/* \
    && printf 'listener 1883\nallow_anonymous true\n' > /etc/mosquitto/v2x.conf
COPY rsu.py .
EXPOSE 1883 8080
CMD ["sh", "-c", "mosquitto -d -c /etc/mosquitto/v2x.conf && exec python -u rsu.py"]

# docker build --target service -t v2x-service .
FROM base AS service
COPY v2x_service.py .
EXPOSE 5000
CMD ["python", "-u", "v2x_service.py"]

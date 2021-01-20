# RI Basis Receiver

Empfängt RI Basis Daten via AMQP (RabbitMQ) und speichert sie als JSON-Dateien, eine Datei pro Meldung.

Optional kann nach dem Empfang jeder Nachricht eine `/ris/read`-Anfrage an eine MOTIS-Instanz geschickt werden (nicht für hohes Nachrichtenaufkommen geeignet).

## Konfiguration

Über Umgebungsvariablen (werden auch aus einer `.env`-Datei geladen falls vorhanden):

```shell
CA_FILE=".../ca.crt"
CLIENT_CERT_FILE=".../client.pem"
HOST="..."
VIRTUAL_HOST="ribasis"
USERNAME="..."
PASSWORD="..."
PORT=5671
QUEUE="..."

DATA_DIR="/root/ribasis/data"
ARCHIVE_DIR="/root/ribasis/archive"
```

Beispiel-Konfiguration für lokale RabbitMQ-Instanz:

```shell
HOST="localhost"
PORT=5672
VIRTUAL_HOST="/"
USERNAME="guest"
PASSWORD="guest"
QUEUE="ribasis"

DATA_DIR="/root/ribasis/data"
ARCHIVE_DIR="/root/ribasis/archive"

MOTIS_NOTIFY_URL="http://localhost:8080/ris/read"
```

## Ausgabedateien

In `${DATA_DIR}/YYYYmmdd` wird pro Meldung eine JSON-Datei angelegt. Die Dateinamen haben die Form `<received_at>_<sequence>_<uuid>.json`, wobei `<received_at>` der lokale Unix Timestamp in µs-Präzision (Sekunden * 1_000_000) ist.

Täglich werden alle JSON-Dateien des Vortages als `${ARCHIVE_DIR}/YYYYmmdd.tar.zst` komprimiert.

## Setup

* Getestet mit Ubuntu 16.04 (Python 3.5)
* RI Basis Receiver läuft als systemd service `rib-receiver`
* Tägliche Archivierung läuft als cronjob `/etc/cron.daily/ribasis-archive`

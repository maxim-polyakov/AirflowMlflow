# Airflow + MLflow + Neon + S3

Один Docker Compose поднимает Airflow и MLflow. Метаданные обоих сервисов
хранятся в Neon, исходные данные и артефакты моделей — в S3-совместимом
бакете. На Vast.ai модель запускается прямо из Jupyter Terminal, без
вложенного Docker.

## 1. Подготовка

1. В Neon создайте две базы: `airflow` и `mlflow`.
2. Создайте S3-бакет для данных и бакет (или отдельный prefix) для MLflow.
3. Создайте DNS `A`-записи `airflow.baxic.ru` и `mlflow.baxic.ru`,
   указывающие на публичный IP сервера. Откройте входящие TCP-порты 80/443.
4. Скопируйте `.env.example` в `.env` и заполните значения.
5. Сгенерируйте секреты:

```powershell
docker run --rm apache/airflow:2.10.5-python3.11 python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
docker run --rm python:3.11-slim python -c "import secrets; print(secrets.token_hex(32))"
```

Первое значение запишите в `AIRFLOW_FERNET_KEY`, второе — в
`AIRFLOW_WEBSERVER_SECRET_KEY`. Пароли со спецсимволами в Neon URL должны
быть URL-encoded. Для Neon обязателен `sslmode=require`.

## 2. Запуск

```powershell
Copy-Item .env.example .env
# заполнить .env
docker compose build
docker compose up airflow-init
docker compose up -d
docker compose ps
```

Airflow и MLflow доступны только локально на сервере на портах 8080 и 5000.
Сервис `stormmodel` опубликован на внешнем порту 8000. Создайте DNS
`A`-запись `model.baxic.ru` на тот же IP сервера.

Установите и настройте системный Nginx:

```bash
sudo apt update
sudo apt install -y nginx apache2-utils certbot python3-certbot-nginx
sudo cp deploy/nginx/*.conf /etc/nginx/sites-available/
sudo ln -s /etc/nginx/sites-available/airflow.baxic.ru.conf /etc/nginx/sites-enabled/
sudo ln -s /etc/nginx/sites-available/mlflow.baxic.ru.conf /etc/nginx/sites-enabled/
sudo ln -s /etc/nginx/sites-available/model.baxic.ru.conf /etc/nginx/sites-enabled/
sudo htpasswd -c /etc/nginx/.htpasswd-mlflow mlflow
sudo htpasswd -c /etc/nginx/.htpasswd-model model-api
sudo nginx -t
sudo systemctl reload nginx
sudo certbot --nginx \
  -d airflow.baxic.ru \
  -d mlflow.baxic.ru \
  -d model.baxic.ru
```

- Airflow: `https://airflow.baxic.ru`
- MLflow: `https://mlflow.baxic.ru`
- Model API: `https://model.baxic.ru`

В Airflow включите и запустите DAG `download_dataset_from_s3`. Он скачает
`s3://DATA_BUCKET/DATA_OBJECT_KEY` в общий каталог `data/input`.

## 3. Логирование модели из Vast notebook

MLflow должен быть доступен с Vast по публичному HTTPS URL, через VPN либо
SSH-туннель. Загрузка артефактов идет через MLflow, поэтому S3-ключи на
Vast не нужны. Перед запуском notebook задайте данные Basic Auth:

```bash
export MLFLOW_TRACKING_URI="https://mlflow.baxic.ru"
export MLFLOW_TRACKING_USERNAME="mlflow"
export MLFLOW_TRACKING_PASSWORD="<ваш пароль>"
```

Пример в notebook:

```python
import mlflow
import mlflow.sklearn
from mlflow import MlflowClient
from sklearn.datasets import load_iris
from sklearn.ensemble import RandomForestClassifier

mlflow.set_tracking_uri("https://mlflow.baxic.ru")
mlflow.set_experiment("vast-training")

X, y = load_iris(return_X_y=True)
model = RandomForestClassifier(n_estimators=100, random_state=42).fit(X, y)

with mlflow.start_run():
    info = mlflow.sklearn.log_model(
        model,
        artifact_path="model",
        registered_model_name="stormmodel",
        input_example=X[:2],
    )
```

При первой регистрации alias еще не существует. Назначьте его созданной
версии в UI MLflow или кодом:

```python
client = MlflowClient()
versions = client.search_model_versions("name = 'stormmodel'")
latest = max(versions, key=lambda item: int(item.version))
client.set_registered_model_alias("stormmodel", "production", latest.version)
```

Сервис `stormmodel` проверяет alias `production` каждую минуту. После
назначения alias новой версии он автоматически перезапускает стандартный
MLflow inference server. Проверка API:

```bash
curl -u model-api:<пароль> https://model.baxic.ru/ping
```

## 4. Запуск модели на Vast без Docker

При создании Vast-инстанса откройте TCP-порт `8000`. В Jupyter Terminal:

```bash
export MLFLOW_TRACKING_URI="https://mlflow.baxic.ru"
export MODEL_URI="models:/stormmodel@production"
bash scripts/start_model_on_vast.sh
```

Если репозитория на Vast нет, те же команды можно выполнить напрямую:

```bash
pip install "mlflow==2.18.0" scikit-learn
nohup mlflow models serve \
  -m "models:/stormmodel@production" \
  -h 0.0.0.0 -p 8000 --no-conda \
  >/workspace/model-server.log 2>&1 &
```

Проверка внутри Vast:

```bash
curl http://127.0.0.1:8000/ping
```

Inference выполняется запросом `POST /invocations` на внешний IP и
проброшенный Vast-порт. Скрипт сначала скачивает модель, затем устанавливает
ее `requirements.txt`. Флаг `--no-conda` запускает модель в текущем
Python-окружении инстанса.

## 5. Деплой через GitHub Actions

В `Settings → Secrets and variables → Actions` создайте Secrets:

- `DEPLOY_SSH_KEY` — полный приватный SSH-ключ;
- `DEPLOY_ENV` — полное содержимое production-файла `.env`.
- `DEPLOY_HOST=89.124.86.173`;
- `DEPLOY_USER=baxic`;
- `DEPLOY_PORT=22`;
- `DEPLOY_PATH=/home/baxic/Documents/GitHub/AirflowMlflow`.

Workflow `.github/workflows/deploy.yml` проверяет Compose и DAG, копирует
проект по SSH, выполняет миграцию Airflow и запускает все сервисы.


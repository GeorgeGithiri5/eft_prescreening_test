# Airflow Setup Instructions

### 1. Clone the repository
```bash
git https://github.com/GeorgeGithiri5/eft_prescreening_test
cd eft_prescreening_test/airflow
```

### 2. Build the Docker images
```bash
docker compose build --no-cache
```

### 3. Start MySQL
```bash
docker compose up -d mysql
```

### 4. Initialize Airflow (one-time setup)
```bash
docker compose run --rm airflow-init
```

### 5. Start Airflow (webserver + scheduler)
```bash
docker compose up -d
```

### 6. Access Services
```bash
    Airflow UI: http://127.0.0.1:8080
    Username: admin
    Password: admin

    MySQL: 127.0.0.1:3306
    User: airflow
    Password: airflow
    Database: airflow
```



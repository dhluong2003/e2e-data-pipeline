# Giới thiệu
1 data pipeline đơn giản, chủ yếu dùng cho demo. Dùng Airflow, Spark, Kafka, Starrock và Grafana


## Luồng dữ liệu:
- ecom_log_gen_kafka.py để tạo dữ liệu -> gửi lên kafka -> dùng spark/flink để preprocess dữ liệu -> load data vào minio, dùng Starrock để query từ minio
- airflow dùng làm orchestrator chạy pipeline từ đầu tới cuối, 
- dùng Grafana+Prometheus tạo dashboard để lấy insight về các metrics trong minio
- Jenkins cho CI/CD (test và deploy các DAG trong airflow hoặc các spark jobs)

# Cách chạy

Yêu cầu cần có:
* Docker (trên Linux) hoặc Docker Desktop (trên Windows)
* Git

1. Clone repo này về máy: `git clone https://github.com/dhluong2003/e2e-data-pipeline.git`

1. Tạo 1 file .env (dùng để tạo container cho Airflow); có thể tham khảo config mẫu dưới đây:
```
AIRFLOW_IMAGE_NAME=apache/airflow:2.10.2-python3.10 # có thể đổi version của Airflow

AIRFLOW_UID=1000
AIRFLOW_GID=1000
_AIRFLOW_WWW_USER_USERNAME=testingaccount # đổi user+password
_AIRFLOW_WWW_USER_PASSWORD=testingaccount
```

2. Kiểm tra xem Docker có đang chạy chưa:
- Trên Linux, dùng lệnh `systemctl status docker`, nếu status = active thì Docker đang chạy
- Trên Windows, kiểm tra trên taskbar xem có biểu tượng của Docker Desktop không, như trong hình sau

![image](https://private-user-images.githubusercontent.com/61196361/518161663-606c5d79-6b0e-40c9-8574-cee0cf4ec073.png?jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NjM5OTU3NDcsIm5iZiI6MTc2Mzk5NTQ0NywicGF0aCI6Ii82MTE5NjM2MS81MTgxNjE2NjMtNjA2YzVkNzktNmIwZS00MGM5LTg1NzQtY2VlMGNmNGVjMDczLnBuZz9YLUFtei1BbGdvcml0aG09QVdTNC1ITUFDLVNIQTI1NiZYLUFtei1DcmVkZW50aWFsPUFLSUFWQ09EWUxTQTUzUFFLNFpBJTJGMjAyNTExMjQlMkZ1cy1lYXN0LTElMkZzMyUyRmF3czRfcmVxdWVzdCZYLUFtei1EYXRlPTIwMjUxMTI0VDE0NDQwN1omWC1BbXotRXhwaXJlcz0zMDAmWC1BbXotU2lnbmF0dXJlPWM4ZDUyMDY1NzE0NmRiNmE0MDRmZGVlYzIxNTdiM2NhM2M1MDJmMGZkY2UwYTIwMDA1ODQ5ZDZlY2I5ZTA2NTImWC1BbXotU2lnbmVkSGVhZGVycz1ob3N0In0.RiSK3GoNu4ftvML3RLFf3iUI6_VfjWzrLvUyUXCotIo)

Nếu có, thì Docker đang chạy.

3. Chuẩn bị sẵn 1 file Dockerfile và requirements.txt (dùng để cài các python module). Có thể tham khảo file Dockerfile mẫu.

4. Mở command line (CMD hoặc PowerShell trên Windows, Bash trên Linux), `cd` đến thư mục chứa repo đã clone về từ bước 1, và chạy lệnh
```
docker compose up -d
```


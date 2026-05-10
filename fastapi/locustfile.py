"""
Locust-файл для нагрузочного тестирования Data Quality Service (Эксперимент Э4).

Запуск:
    locust -f locustfile.py --host=http://localhost:8000

Затем открыть браузер: http://localhost:8089
"""
import os
from locust import HttpUser, task, between


class DQServiceUser(HttpUser):
    """
    Пользователь сервиса Data Quality.
    Загружает Excel-файл и проверяет ответ.
    """
    # Пауза между задачами (фиксированная 1 секунда для контролируемого RPS)
    wait_time = between(1, 1)

    def on_start(self):
        """Подготовка: путь к тестовому файлу."""
        self.filepath = "datasets/DS-1000-clean.xlsx"
        if not os.path.exists(self.filepath):
            raise FileNotFoundError(f"Файл не найден: {self.filepath}")

    @task
    def upload_excel(self):
        """Основная задача: загрузка Excel-файла."""
        with open(self.filepath, "rb") as f:
            # Отправляем файл как multipart/form-data
            files = {"file": (
            os.path.basename(self.filepath), f, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
            with self.client.post("/upload/excel/", files=files, catch_response=True) as response:
                if response.status_code == 200:
                    response.success()
                else:
                    response.failure(f"HTTP {response.status_code}: {response.text[:200]}")

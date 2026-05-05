from __future__ import annotations

from locust import HttpUser, task, between


class LockUser(HttpUser):
    wait_time = between(0.1, 0.5)
    host = "http://localhost:8001"

    def on_start(self) -> None:
        self.headers = {"X-API-Key": "devkey"}

    @task(3)
    def acquire_lock(self) -> None:
        self.client.post(
            "/lock/acquire",
            json={"resource": "bench", "owner": "locust", "ttl_ms": 2000, "mode": "exclusive"},
            headers=self.headers,
        )

    @task(1)
    def queue_roundtrip(self) -> None:
        enqueue = self.client.post(
            "/queue/enqueue",
            json={"queue": "jobs", "payload": {"task": "locust"}},
            headers=self.headers,
        )
        if enqueue.ok:
            dequeue = self.client.post(
                "/queue/dequeue",
                json={"queue": "jobs", "consumer": "locust"},
                headers=self.headers,
            )
            if dequeue.ok and dequeue.json().get("receipt_id"):
                self.client.post(
                    "/queue/ack",
                    json={"queue": "jobs", "receipt_id": dequeue.json().get("receipt_id")},
                    headers=self.headers,
                )

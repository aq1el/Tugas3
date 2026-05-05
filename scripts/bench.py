from __future__ import annotations

import argparse
import asyncio
import time
from collections import Counter
from statistics import mean
from typing import Any, Dict, List

import httpx


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8001")
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--api-key", default="devkey")
    return parser.parse_args()


async def worker(
    client: httpx.AsyncClient,
    url: str,
    api_key: str,
    count: int,
    times: List[float],
    prefix: str,
    errors: List[Dict[str, Any]],
) -> None:
    for i in range(count):
        for attempt in range(3):
            start = time.perf_counter()
            try:
                response = await client.post(
                    f"{url}/lock/acquire",
                    json={
                        "resource": f"{prefix}-{i}",
                        "owner": "bench",
                        "ttl_ms": 2000,
                        "mode": "exclusive",
                    },
                    headers={"X-API-Key": api_key},
                )
            except httpx.HTTPError as exc:
                if attempt < 2:
                    await asyncio.sleep(0.05)
                    continue
                errors.append({"status": "transport", "detail": str(exc)})
                break

            if response.is_success:
                times.append((time.perf_counter() - start) * 1000)
                break

            try:
                detail = response.json()
            except Exception:
                detail = response.text
            errors.append({"status": response.status_code, "detail": detail})
            break


async def main() -> None:
    args = parse_args()
    per_worker = args.requests // args.concurrency
    remainder = args.requests % args.concurrency
    times: List[float] = []
    errors: List[Dict[str, Any]] = []
    run_id = time.time_ns()
    async with httpx.AsyncClient(timeout=10) as client:
        tasks = [
            worker(
                client,
                args.url,
                args.api_key,
                per_worker + (1 if idx < remainder else 0),
                times,
                f"bench-{run_id}-{idx}",
                errors,
            )
            for idx in range(args.concurrency)
        ]
        await asyncio.gather(*tasks)

    result = {"requests_ok": len(times), "requests_total": args.requests}
    if times:
        result.update({
            "avg_ms": mean(times),
            "min_ms": min(times),
            "max_ms": max(times),
        })
    if errors:
        by_status = Counter(err["status"] for err in errors)
        result["errors"] = {
            "count": len(errors),
            "by_status": dict(by_status),
            "sample": errors[0],
        }
    print(result)


if __name__ == "__main__":
    asyncio.run(main())

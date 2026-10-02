"""Run inside the API container with development controls explicitly enabled."""
import json
import os
import time
import urllib.error
import urllib.request

BASE = "http://localhost:8000"
TOKEN = os.environ["CHAOS_TOKEN"]


def request(path, body=None, token=TOKEN):
    req = urllib.request.Request(BASE + path, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.load(response)


def reset(**faults):
    return request("/dev/faults", {"inventory_unavailable": False, "redis_unavailable": False, "consumer_paused": False, "latency_ms": 0, **faults})


def wait_until(check, timeout=20):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        value = check()
        if value:
            return value
        time.sleep(0.5)
    raise AssertionError("Recovery timed out")


try:
    try:
        request("/dev/faults", token="invalid")
        raise AssertionError("Unauthenticated controls accepted")
    except urllib.error.HTTPError as error:
        assert error.code == 403
    reset()
    healthy = wait_until(lambda: (snapshot if not (snapshot := request("/inventory"))["metadata"]["stale"] else None))
    assert len(healthy["products"]) == 500
    reset(inventory_unavailable=True)
    for _ in range(3):
        cached = request("/inventory")
        assert cached["metadata"]["stale"] and cached["metadata"]["source"] == "redis"
        assert len(cached["products"]) == len(healthy["products"])
    assert cached["metadata"]["breaker"]["state"] == "OPEN"
    assert cached["metadata"]["age_seconds"] >= 0
    reset()
    recovered = wait_until(lambda: (snapshot if not (snapshot := request("/inventory"))["metadata"]["stale"] else None))
    assert recovered["metadata"]["breaker"]["state"] == "CLOSED"
    assert {event["to"] for event in recovered["metadata"]["breaker"]["transitions"]} >= {"OPEN", "HALF_OPEN", "CLOSED"}
    reset(consumer_paused=True)
    paused = request("/inventory")
    time.sleep(1)
    older = request("/inventory")
    assert paused["metadata"]["stale"] and older["metadata"]["age_seconds"] > paused["metadata"]["age_seconds"]
    reset(redis_unavailable=True)
    before = request("/metrics")["dlq"]
    assert request("/inventory")["metadata"]["stale"]
    time.sleep(3)
    assert request("/metrics")["dlq"] == before, "Valid updates must not be dead-lettered during Redis failure"
    reset()
    wait_until(lambda: not request("/inventory")["metadata"]["stale"])
    request("/dev/faults/invalid-event", {})
    wait_until(lambda: request("/metrics")["dlq"] > before)
    print("Verified: protected controls, Redis fallback, OPEN/HALF_OPEN/CLOSED recovery, paused freshness, Redis outage replay safety, invalid-event DLQ.")
finally:
    reset()

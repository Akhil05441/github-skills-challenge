import json
import runpy

from src.anomaly_detector import AnomalyDetector
from src.aiops_pipeline import load_data, run_pipeline
from src.event_consumer import EventConsumer
from src.event_producer import EventProducer
from src.event_topic import EventTopic


def test_normal_record_is_not_anomaly():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:00:00",
        "service": "payment-service",
        "response_time_ms": 120,
        "cpu_percent": 42,
        "memory_percent": 51,
        "log_level": "INFO",
        "message": "Payment request processed successfully"
    }

    assert detector.detect(record) is None


def test_anomalous_record_is_detected():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:05:00",
        "service": "payment-service",
        "response_time_ms": 610,
        "cpu_percent": 75,
        "memory_percent": 70,
        "log_level": "ERROR",
        "message": "Payment service timeout"
    }

    event = detector.detect(record)

    assert event is not None
    assert event["type"] == "ANOMALY"


def test_producer_publishes_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)

    event = {
        "type": "ANOMALY",
        "service": "payment-service"
    }

    assert producer.publish(event)
    assert len(topic.get_messages()) == 1


def test_consumer_receives_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)
    consumer = EventConsumer(topic)

    event = {
        "type": "ANOMALY",
        "service": "payment-service"
    }

    producer.publish(event)

    messages = consumer.consume()

    assert len(messages) == 1


def test_warning_record_is_detected_as_anomaly():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:00:00",
        "service": "database-service",
        "response_time_ms": 120,
        "cpu_percent": 50,
        "memory_percent": 60,
        "log_level": "WARNING",
        "message": "Database slow response"
    }

    event = detector.detect(record)

    assert event is not None
    assert event["type"] == "ANOMALY"
    assert "Error log detected" in event["reasons"]


def test_topic_clear_removes_existing_messages():
    topic = EventTopic("anomaly-events")
    topic.publish({"type": "ANOMALY", "service": "x"})

    topic.clear()

    assert topic.get_messages() == []


def test_producer_rejects_empty_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)

    assert producer.publish({}) is False
    assert topic.get_messages() == []


def test_load_data_reads_json_file(tmp_path):
    payload = [{"service": "api", "response_time_ms": 200}]
    file_path = tmp_path / "sample.json"
    file_path.write_text(json.dumps(payload), encoding="utf-8")

    assert load_data(str(file_path)) == payload


def test_run_pipeline_processes_service_data():
    result = run_pipeline("data/service_data.json")

    assert result["records_processed"] == 10
    assert len(result["anomalies_detected"]) == 2
    assert len(result["events_consumed"]) == 2
    assert all(event["type"] == "ANOMALY" for event in result["events_consumed"])


def test_pipeline_main_block_runs_and_prints_summary(tmp_path, monkeypatch, capsys):
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    payload = [
        {
            "timestamp": "2026-09-20T10:00:00",
            "service": "svc-a",
            "response_time_ms": 120,
            "cpu_percent": 42,
            "memory_percent": 51,
            "log_level": "INFO",
            "message": "ok"
        },
        {
            "timestamp": "2026-09-20T10:01:00",
            "service": "svc-a",
            "response_time_ms": 600,
            "cpu_percent": 90,
            "memory_percent": 92,
            "log_level": "ERROR",
            "message": "fail"
        }
    ]
    (data_dir / "service_data.json").write_text(json.dumps(payload), encoding="utf-8")

    monkeypatch.chdir(tmp_path)

    runpy.run_module("src.aiops_pipeline", run_name="__main__")

    captured = capsys.readouterr()
    assert "AIOps Pipeline Result" in captured.out
    assert "Records processed: 2" in captured.out
    assert "Anomalies detected: 1" in captured.out
    assert "Events consumed: 1" in captured.out
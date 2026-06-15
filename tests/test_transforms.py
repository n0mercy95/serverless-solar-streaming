"""
tests/test_transforms.py — Pruebas unitarias para las transformaciones de Apache Beam.

Usa TestPipeline para validar localmente el comportamiento
del ParseSensorReadingDoFn y AggregateTelemetryCombineFn,
incluyendo el desvío correcto a la DLQ.
"""

import json
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from application.transforms.parsing import DLQ_TAG, ParseSensorReadingDoFn
from application.transforms.aggregation import AggregateTelemetryCombineFn
from domain.models import SensorReading, SensorType


def test_parse_sensor_reading_success():
    """Un JSON válido debe producir un SensorReading en el main output."""
    valid_json = json.dumps({
        "event_id": "123",
        "plant_id": "plant-1",
        "sensor_id": "sensor-1",
        "sensor_type": "inverter",
        "timestamp": "2023-01-01T12:00:00Z",
        "lmd_ghi": 1000.0,
        "power_mw": 5.0,
        "module_temp_c": 40.0,
        "ambient_temp_c": 30.0,
        "dc_voltage": 600.0,
        "dc_current": 8.0,
    }).encode("utf-8")

    with TestPipeline() as p:
        result = (
            p
            | "CreateData" >> beam.Create([valid_json])
            | "Parse" >> beam.ParDo(ParseSensorReadingDoFn()).with_outputs(DLQ_TAG, main="valid")
        )

        def check_reading(readings):
            assert len(readings) == 1
            r = readings[0]
            assert r.plant_id == "plant-1"
            assert r.power_mw == 5.0
            assert r.sensor_type == SensorType.INVERTER

        assert_that(result.valid, check_reading, label="CheckValid")


def test_parse_sensor_reading_dlq():
    """Un JSON inválido debe enviarse al side output DLQ y no fallar."""
    invalid_json = b'{"bad_json": "missing_fields"}'

    with TestPipeline() as p:
        result = (
            p
            | "CreateData" >> beam.Create([invalid_json])
            | "Parse" >> beam.ParDo(ParseSensorReadingDoFn()).with_outputs(DLQ_TAG, main="valid")
        )

        def check_dlq(records):
            assert len(records) == 1
            r = records[0]
            assert "bad_json" in r.original_payload
            assert r.failed_step == "ParseSensorReadingDoFn"

        assert_that(result.valid, equal_to([]), label="CheckEmptyValid")
        assert_that(result[DLQ_TAG], check_dlq, label="CheckDLQ")


def test_aggregate_telemetry_combine_fn():
    """Verifica que la agregación calcule correctamente AVG, MIN, MAX."""
    # Simular lecturas en una misma ventana para la misma planta
    ts = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    
    readings = [
        SensorReading(
            event_id="1", plant_id="plant-1", sensor_id="s1", sensor_type=SensorType.INVERTER,
            timestamp=ts, lmd_ghi=1000.0, power_mw=4.0, module_temp_c=40.0, ambient_temp_c=30.0, dc_voltage=600.0, dc_current=8.0
        ),
        SensorReading(
            event_id="2", plant_id="plant-1", sensor_id="s2", sensor_type=SensorType.INVERTER,
            timestamp=ts, lmd_ghi=800.0, power_mw=2.0, module_temp_c=30.0, ambient_temp_c=25.0, dc_voltage=500.0, dc_current=6.0
        ),
    ]

    with TestPipeline() as p:
        # GroupByKey espera tuplas (key, value)
        grouped = (
            p
            | "CreateData" >> beam.Create([("plant-1", r) for r in readings])
            | "Aggregate" >> beam.CombinePerKey(AggregateTelemetryCombineFn())
        )

        def check_aggregation(metrics_kvs):
            assert len(metrics_kvs) == 1
            plant_id, metrics = metrics_kvs[0]
            assert plant_id == "plant-1"
            assert metrics.reading_count == 2
            assert metrics.avg_ghi == 900.0
            assert metrics.avg_power_mw == 3.0
            assert metrics.max_power_mw == 4.0
            assert metrics.min_power_mw == 2.0
            assert metrics.avg_module_temp_c == 35.0

        assert_that(grouped, check_aggregation, label="CheckMetrics")

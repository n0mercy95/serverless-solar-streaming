"""
application/transforms/inference.py — Integración del Modelo ML y Patrón Strategy

Contiene el DoFn que enriquece las métricas agregadas con
las predicciones del modelo Transformer Bi-LSTM y ejecuta
las estrategias de detección de anomalías.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from typing import Iterator

import apache_beam as beam

from domain.models import AggregatedMetrics, AnomalyType
from domain.ports import AnomalyDetectionPort, ModelInferencePort
from domain.strategies import (
    InverterFailureStrategy,
    IrradianceDropStrategy,
    ThermalAnomalyStrategy,
)

logger = logging.getLogger(__name__)


class MockBiLSTMModelPort(ModelInferencePort):
    """
    Simulación del modelo Transformer Bi-LSTM.
    
    Usa una fórmula heurística básica basada en GHI y temperatura
    como placeholder hasta que el modelo real sea inyectado mediante
    RunInference.
    """
    
    def predict_power(self, metrics: AggregatedMetrics) -> float:
        # Fórmula simplificada: 1000 W/m2 GHI a 25C da ~5MW (depende del factor de planta)
        # Factor base: 0.005 MW por W/m2
        base_power = metrics.avg_ghi * 0.005
        
        # Coeficiente de temperatura: pierde 0.4% por cada grado sobre 25C
        temp_diff = max(metrics.avg_module_temp_c - 25.0, 0)
        efficiency_loss = temp_diff * 0.004
        
        predicted = base_power * (1.0 - efficiency_loss)
        return max(predicted, 0.0)


class AnomalyDetectionDoFn(beam.DoFn):
    """
    Ejecuta el Patrón Strategy sobre las métricas agregadas.
    
    1. Predice la potencia ideal usando el modelo ML.
    2. Itera sobre las estrategias (Hotspot, Inversor, GHI).
    3. Asigna el tipo de anomalía y score de mayor severidad.
    """
    
    def __init__(self, model_port: ModelInferencePort = None, strategies: list[AnomalyDetectionPort] = None):
        self.model = model_port or MockBiLSTMModelPort()
        # Instanciar estrategias por defecto si no se inyectan
        self.strategies = strategies or [
            ThermalAnomalyStrategy(),
            IrradianceDropStrategy(),
            InverterFailureStrategy(),
        ]

    def process(self, metrics: AggregatedMetrics) -> Iterator[AggregatedMetrics]:
        # 1. Predecir potencia
        predicted_power = self.model.predict_power(metrics)
        
        # Generar nueva instancia temporal con la predicción (para que la estrategia de inversor la vea)
        temp_metrics = replace(metrics, predicted_power_mw=predicted_power)
        
        highest_score = 0.0
        final_anomaly = AnomalyType.NONE
        
        # 2. Evaluar todas las estrategias (Patrón Strategy)
        for strategy in self.strategies:
            is_anomaly, score, desc = strategy.detect(temp_metrics)
            
            if is_anomaly:
                logger.warning(
                    "Anomalía detectada en %s: %s (score=%.2f)",
                    metrics.plant_id, desc, score
                )
                if score > highest_score:
                    highest_score = score
                    # Mapear clase de estrategia a AnomalyType
                    if isinstance(strategy, ThermalAnomalyStrategy):
                        final_anomaly = AnomalyType.THERMAL
                    elif isinstance(strategy, IrradianceDropStrategy):
                        final_anomaly = AnomalyType.IRRADIANCE_DROP
                    elif isinstance(strategy, InverterFailureStrategy):
                        final_anomaly = AnomalyType.INVERTER_FAILURE
        
        # 3. Emitir las métricas enriquecidas
        enriched_metrics = replace(
            temp_metrics,
            anomaly_type=final_anomaly,
            anomaly_score=highest_score,
        )
        
        yield enriched_metrics

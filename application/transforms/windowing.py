"""
application/transforms/windowing.py — Lógica de Ventanas Temporales

Configura el enventanamiento (windowing) del pipeline de streaming:
- FixedWindows de 15 minutos para la agregación continua
- Watermarks basados en el event time del sensor
- Allowed lateness de 5 minutos para datos tardíos
- Triggers con early firings cada 60 segundos

Referencia PRD:
  "Configuración de ventanas temporales (fijas y deslizantes de 15 minutos).
   Implementación de watermarks, definición de la latencia permitida
   (allowed lateness) y políticas de disparadores (triggers)."
"""

from __future__ import annotations

import apache_beam as beam
from apache_beam import window
from apache_beam.transforms.trigger import (
    AccumulationMode,
    AfterProcessingTime,
    AfterWatermark,
)

# Constantes de configuración de ventanas
WINDOW_SIZE_SECONDS = 15 * 60      # 15 minutos
ALLOWED_LATENESS_SECONDS = 5 * 60  # 5 minutos
EARLY_FIRING_SECONDS = 60          # Resultados parciales cada 60s


class ApplyWindowing(beam.PTransform):
    """
    PTransform que aplica ventanas temporales fijas de 15 minutos.

    Configuración:
    - FixedWindows(15 min): cada ventana cubre exactamente 15 minutos
    - AfterWatermark trigger: dispara cuando la ventana está "completa"
      según el watermark, con early firings cada 60 segundos
    - Allowed lateness: 5 minutos — datos que llegan tarde se incorporan
      a la ventana correcta hasta 5 min después del cierre
    - AccumulationMode.DISCARDING: cada trigger emite solo los nuevos
      datos desde el último disparo (no re-emite toda la ventana)

    ¿Por qué FixedWindows y no SlidingWindows?
    Para la Fase 2, usamos FixedWindows porque la agregación por
    ventana de 15 min es más simple y alineada con la grilla temporal
    del dataset PVOD original. En la Fase 3, se podrá cambiar a
    SlidingWindows si el modelo ML lo requiere.
    """

    def __init__(
        self,
        window_size: int = WINDOW_SIZE_SECONDS,
        allowed_lateness: int = ALLOWED_LATENESS_SECONDS,
        early_firing: int = EARLY_FIRING_SECONDS,
    ):
        super().__init__()
        self._window_size = window_size
        self._allowed_lateness = allowed_lateness
        self._early_firing = early_firing

    def expand(self, pcoll):
        return pcoll | "ApplyFixedWindows" >> beam.WindowInto(
            window.FixedWindows(self._window_size),
            trigger=AfterWatermark(
                early=AfterProcessingTime(self._early_firing),
                late=AfterProcessingTime(self._early_firing),
            ),
            accumulation_mode=AccumulationMode.DISCARDING,
            allowed_lateness=self._allowed_lateness,
        )

# -*- coding: utf-8 -*-
"""Capacidad de Acabado y Tintorería desde el papel de planta (1.51).

Los dos números venían de estimaciones que nadie podía contrastar. La
planta mandó sus dos formatos (F-IT-P-P01-10-06 rev 02, abril 2026:
tiempos de rama, tiempos de tintorería, capacidad de cargas y horario),
así que ahora salen de velocidades y ciclos medidos.

ACABADO — 915,733 → 1,175,313 m/mes (+28%)
    UNITECH 29.0779 m/min y BRUCKNER 28.3478 m/min promedio, menos el
    descuento de planta de cada una (10% y 15%): 1,570.2 + 1,445.7 =
    3,015.9 m/h entre las dos ramas. Por 389.7 h/mes (90 h/semana de los
    dos turnos × 4.33 semanas) da 1,175,313 m/mes. La planta declara
    1,158,124 en su hoja porque calcula el mes con 384 h; se usa la
    convención del módulo (`weeks_per_month`), 1.5% arriba de su cifra.

    Esto es lo que el check «Capacidad normal vs producción real» venía
    pidiendo: con 915,733 capturados, enero–mayo de 2026 produjeron más
    que la capacidad y el costeo tuvo que caer a producción real. Con la
    capacidad honesta, la producción de 2026 (692K–945K m/mes) cabe, y
    el costo fijo por metro deja de llevar dentro la ociosidad.

TINTORERÍA — 195,000 → 216,089 kg/mes (+11%)
    El 195,000 era el producto de tres supuestos: 5 tinas, 625 h/mes y
    un ciclo de 9 h. Los tres estaban mal y casi se cancelaron entre sí.
    Reales: 4 tinas trabajando (la HTJ-5 THEN de 1,200 kg sigue en
    pruebas), 389.7 h/mes, y el ciclo NO es uno solo — va de 2:20 en
    naturales a 10:20 en obscuros. Ponderado por la mezcla real de las
    OPs (82% natural, 13% blanco, 2.7% obscuro, 2.3% medio) el ciclo
    efectivo es de 3.1 h, no 9. Las cuatro tinas dan 554.5 kg/h.

    Respuesta al pendiente que dejó abierto la nota del centro: con el
    ciclo real, las tinas NO son el cuello. Corren al 43% de su
    capacidad (92,810 kg/mes reales contra 216,089).

Además quedan capturados los turnos (90 h/semana, 2 ramas y 4 tinas) y
el throughput nominal de cada centro, que es lo que le da al panel
contra qué validar la capacidad capturada — hasta hoy `capacidad_normal`
ganaba en silencio sobre el cálculo de turnos y por eso Acabado pudo
vivir dos años con un número que sus propias máquinas contradecían.

Recálculo: SOLO 2026. Los períodos de 2024 y 2025 se quedan como están
por decisión de dirección (se cierran con su visto bueno); ninguno está
marcado `cerrado` todavía, así que la restricción se aplica aquí, no en
el guard.
"""
import logging
from datetime import date

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

# Centro → (capacidad/mes, throughput nominal por máquina-hora,
#           nombre del turno, horas/semana, nº de máquinas, nota)
CAPACIDADES = {
    'ACABADO': (
        1175313.0, 1508.0, 'Dos turnos (90 h/semana)', 90.0, 2,
        'Ramas UNITECH (29.08 m/min, −10%) y BRUCKNER (28.35 m/min, '
        '−15%) = 3,015.9 m/h. La ICOMATEX (RAMA 3) sigue en montaje: al '
        'arrancar, subir máquinas a 3 y recapturar la capacidad. '
        'Fuente: T RAMA, F-IT-P-P01-10-06 rev 02 abr-2026.'),
    'TINTORERIA': (
        216089.0, 138.62, 'Dos turnos (90 h/semana)', 90.0, 4,
        'Cuatro tinas trabajando (HTJ-1 SCHOLL, HTJ-2 y HTJ-5 THEN, '
        'HTJ-3 y HTJ-4 SCLAVOS; la HTJ-5 de 1,200 kg en pruebas). '
        '554.5 kg/h ponderando el ciclo real de cada tina por la mezcla '
        'de color de las OPs: 2:20 h en naturales, 10:20 h en obscuros, '
        '3.1 h efectivas. Con ese ciclo las tinas NO son el cuello. '
        'Fuente: TIEMPOS TINTORERIA + CAP DE CARGAS, F-IT-P-P01-10-06 '
        'rev 02 abr-2026.'),
}


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    Centro = env['qb.costeo.centro']
    Turno = env['qb.turno.config']

    for code, datos in CAPACIDADES.items():
        cap, throughput, turno_name, horas, maquinas, nota = datos
        centro = Centro.search([('code', '=', code)], limit=1)
        if not centro:
            _logger.warning('qb_capacidad_costeo 1.51: no existe el centro '
                            '%s — capacidad sin actualizar.', code)
            continue
        anterior = centro.capacidad_normal
        centro.write({'capacidad_normal': cap,
                      'std_output_per_hour': throughput,
                      'notes': nota})
        if not Turno.search_count([('centro_id', '=', centro.id)]):
            Turno.create({'centro_id': centro.id, 'name': turno_name,
                          'hours_per_week': horas, 'machine_count': maquinas})
        _logger.info('qb_capacidad_costeo 1.51: %s capacidad %s → %s',
                     code, anterior, cap)

    # Solo 2026: la capacidad nueva mueve el factor de fabricación por
    # metro, y los períodos viejos se congelan con el visto bueno de
    # dirección, no con una migración.
    corte = date(2026, 1, 1)
    periodos = sorted(p for p in set(
        env['qb.costo.factores'].search([]).mapped('period')) if p >= corte)
    for period in periodos:
        env['qb.costo.producto'].action_recompute_period(period)

    _logger.info('qb_capacidad_costeo 1.51: capacidad de planta capturada; '
                 '%s períodos de 2026 recalculados (2024-2025 intactos).',
                 len(periodos))

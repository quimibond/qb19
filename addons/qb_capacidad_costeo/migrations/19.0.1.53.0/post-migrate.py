# -*- coding: utf-8 -*-
"""Capacidad de tejido desde el formato de planta (1.53).

Último de los tres centros fabriles cuya capacidad era una estimación sin
fuente. Con «informacion_de_carga_produccion» (CapacidadesProducto +
Turnos) sale de velocidades medidas, igual que acabado y tintorería.

TEJIDO — 180,000 → 197,529 kg/mes (+10%)
    27 circulares tejiendo, cada una a su velocidad documentada (11.73
    kg/h promedio; rango 9.1 en las VANGUARD galga 24 a 28.2 en la
    CIRCULAR 26 con XJ130), por 623.5 h/mes — las 144 h/semana del
    horario real: doce turnos de 12 h, con la planta parada de viernes
    19:00 a sábado 19:00.

    Las 37 instaladas darían 269,174 kg/mes. Se cuentan 27 porque son las
    que tejieron en el mes: mismo criterio que en acabado, donde se
    contaron las dos ramas que corren y no la ICOMATEX en montaje. Las
    otras diez no están fuera de servicio —todas tejieron algo en doce
    meses— pero no se dotan; son ociosidad instalada, no capacidad normal.

    De paso valida el throughput que el módulo traía a ojo: 11 kg/h
    capturados contra 11.73 medidos, 6% de diferencia.

Lo que la medición dejó claro, y vale más que el número: las circulares
corren a velocidad NOMINAL. En agosto registraron 8,660 horas-máquina y
produjeron ~93,000 kg, o sea 10.7 kg/h contra los 11.73 del papel. El
problema de tejido no es que las máquinas vayan lentas — es que de las
17,458 horas-máquina programadas de las 27 circulares solo se usaron la
mitad. La ociosidad es de horas, no de kilos por hora.

Recálculo: SOLO 2026, y tejido es el denominador de kg, así que mueve el
factor de fabricación por peso de todos los productos costeados por kilo.
2024 y 2025 se congelan con el visto bueno de dirección.
"""
import logging
from datetime import date

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

CAPACIDAD = 197529.0
THROUGHPUT = 11.73
HORAS_SEMANA = 144.0
MAQUINAS = 27
NOTAS = (
    '37 circulares instaladas, 27 tejiendo en el mes. Capacidad = cada '
    'una a su velocidad del formato de planta (11.73 kg/h promedio, rango '
    '9.1-28.2 según galga y artículo) por 623.5 h/mes (144 h/semana: 12 '
    'turnos de 12 h, para de viernes 19:00 a sábado 19:00). Las 10 '
    'restantes existen y tejieron algo en 12 meses, pero no se dotan: son '
    'ociosidad instalada, no capacidad normal. Producción medida por orden '
    '(TL/OP-TE), no por workorder: la cantidad por workorder está mal '
    'registrada (abril colapsa a ~1/5); la DURACIÓN sí sirve y confirma '
    'que las máquinas corren a velocidad nominal — lo que falta son horas, '
    'no kilos por hora. Fuente: CapacidadesProducto + Turnos, '
    'informacion_de_carga_produccion.'
)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    centro = env['qb.costeo.centro'].search([('code', '=', 'TEJIDO')], limit=1)
    if centro:
        anterior = centro.capacidad_normal
        centro.write({'capacidad_normal': CAPACIDAD,
                      'std_output_per_hour': THROUGHPUT,
                      'notes': NOTAS})
        Turno = env['qb.turno.config']
        if not Turno.search_count([('centro_id', '=', centro.id)]):
            Turno.create({
                'centro_id': centro.id,
                'name': 'Tres turnos (144 h/semana)',
                'hours_per_week': HORAS_SEMANA,
                'machine_count': MAQUINAS,
                'notes': '12 turnos de 12 h: L-V día, M-J noche, sábado y '
                         'domingo noche, domingo día. Máquinas = las que '
                         'tejieron en el mes, no las 37 instaladas.'})
        _logger.info('qb_capacidad_costeo 1.53: TEJIDO capacidad %s → %s',
                     anterior, CAPACIDAD)
    else:
        _logger.warning('qb_capacidad_costeo 1.53: no existe el centro '
                        'TEJIDO — capacidad sin actualizar.')

    # Que la capacidad esté en la BASE antes de que el motor la lea por la
    # vista de ociosidad, que es SQL crudo (el tropiezo de la 1.51).
    env.flush_all()

    corte = date(2026, 1, 1)
    periodos = sorted(p for p in set(
        env['qb.costo.factores'].search([]).mapped('period')) if p >= corte)
    for period in periodos:
        env['qb.costo.producto'].action_recompute_period(period)

    _logger.info('qb_capacidad_costeo 1.53: %s períodos de 2026 recalculados '
                 '(2024-2025 intactos).', len(periodos))

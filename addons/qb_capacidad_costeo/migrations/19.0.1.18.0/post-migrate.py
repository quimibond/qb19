# -*- coding: utf-8 -*-
"""Períodos cerrables + régimen híbrido capa/absorción, con el corte de TEJIDO.

Dos cambios estructurales.

**1. Los períodos se pueden cerrar.** Hasta ahora cualquier recálculo
reescribía meses ya reportados: el número que viste el mes pasado cambiaba
solo la próxima vez que alguien corría el cron, y no había forma de
defenderlo. Ahora un período cerrado congela sus factores y sus costos por
producto — ni el cron ni un write suelto los tocan — y reabrirlo exige motivo
y queda en el historial. Los períodos existentes arrancan en borrador para no
congelar nada sin que alguien lo decida.

**2. Régimen híbrido por centro.** El 1-sep-2026 los 37 workcenters CIRCULAR
empiezan a capitalizar horas × tarifa contra 504.01.0099 COSTOS FABRILES
APLICADOS A PRODUCCIÓN. Desde ese día el costo de tejido viaja dentro del AVCO
del producto y la venta lo libera solo, así que el pool del módulo NO puede
seguir repartiéndolo: sería el mismo peso cobrado dos veces.

Esta migración:

- marca TEJIDO como `absorcion_odoo` con fecha 2026-09-01 (los meses
  anteriores conservan el régimen de capa, así que el histórico no se mueve);
- clasifica la cuenta de costos fabriles aplicados en el bucket nuevo
  `absorcion_odoo`, si está sin clasificar.

Lo que se resta del pool NO es un parámetro que haya que mantener al día: es
el saldo acreedor real de esa cuenta. Si la tarifa por hora absorbe de más o
de menos, el pool se ajusta solo.

Al quedarse TEJIDO fuera, el denominador de kilos se queda sin centro y el
motor manda todo el share al lado de metros. Eso es correcto —no hay a qué
absorber por peso— pero significa que `fab_weight_share` (0.67, calibrado con
tejido dentro) deja de tener efecto: revísalo cuando el siguiente centro
migre. El panel avisa de las dos mitades del doble conteo.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

# Corte confirmado con dirección: los workcenters ya traen la cuenta 504.01.0099
# y ese día se les escribe la tarifa por hora.
TEJIDO_FECHA_ABSORCION = '2026-09-01'
CUENTA_APLICADOS = '504.01.0099'


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})

    centro = env['qb.costeo.centro'].search([('code', '=', 'TEJIDO')], limit=1)
    if centro and centro.modo_costeo != 'absorcion_odoo':
        centro.write({'modo_costeo': 'absorcion_odoo',
                      'fecha_absorcion': TEJIDO_FECHA_ABSORCION})
        _logger.info('qb_capacidad_costeo: TEJIDO pasa a absorción por '
                     'workcenter desde %s — su gasto sale del pool a partir '
                     'de ese período', TEJIDO_FECHA_ABSORCION)
    elif not centro:
        _logger.warning(
            'qb_capacidad_costeo: no se encontró el centro TEJIDO. Márcalo a '
            'mano como «Absorción por workcenter» con fecha %s antes del '
            'primer cierre de septiembre, o su costo se contará dos veces.',
            TEJIDO_FECHA_ABSORCION)

    Clase = env['qb.costeo.cuenta.class']
    cuentas = env['account.account'].search(
        [('code', '=like', CUENTA_APLICADOS + '%')])
    ya = Clase.search([('account_ids', 'in', cuentas.ids)])
    if cuentas and not ya:
        Clase.create([{'account_id': c.id, 'bucket': 'absorcion_odoo'}
                      for c in cuentas])
        _logger.info('qb_capacidad_costeo: %s clasificada como «Absorbido por '
                     'Odoo» — su saldo acreedor se resta del pool',
                     ', '.join(cuentas.mapped('code')))
    elif ya:
        fuera = ya.filtered(lambda c: c.bucket != 'absorcion_odoo')
        if fuera:
            _logger.warning(
                'qb_capacidad_costeo: la cuenta de costos fabriles aplicados '
                'ya está clasificada como %s. Muévela al bucket «Absorbido '
                'por Odoo» o el pool no restará lo que Odoo capitaliza.',
                ', '.join(fuera.mapped('bucket')))
    else:
        _logger.warning(
            'qb_capacidad_costeo: no existe la cuenta %s. Cuando se cree, '
            'clasifícala en el bucket «Absorbido por Odoo».', CUENTA_APLICADOS)

    # El recálculo de los períodos NO va aquí, a propósito. Las migraciones
    # corren en orden de versión y cada una pisa el resultado de la anterior,
    # así que recalcular en las trece dejaba ~130,600 recálculos de producto
    # (13 migraciones x 8 períodos x ~1,256 productos) para un build donde
    # bastan los ~10,000 de la última pasada. Las doce primeras se tiraban a
    # la basura y el build de Odoo.sh las pagaba enteras.
    #
    # Lo hace la migración MÁS NUEVA, una sola vez, con todos los cambios de
    # datos de esta cadena ya aplicados. Si alguna vez se despliega una
    # versión intermedia suelta —hoy no pasa: el módulo se instala en la
    # versión del manifest— queda el botón «Recalcular» del Panel.

    _logger.info('qb_capacidad_costeo: TEJIDO marcado para absorción. Los '
                 'períodos quedan todos en BORRADOR a propósito: congelar un '
                 'mes es decisión de alguien, no de una migración.')

# -*- coding: utf-8 -*-
"""Corregir el reparto de la aduana: el pedimento del hilo lo carga el hilo.

19.0.1.11.0 mandó los gastos e impuestos de importación al bucket
`importacion` y los repartió sobre el valor de compra de la familia de
REVENTA (`Producto en Proceso / Importación`). Estaba mal, y los datos lo
dicen: medido sep 2025 – ago 2026, el valor importado se reparte ~83%
materia prima (hilo, fibra, resina), ~9% reventa y ~6% activo fijo. Tomar
como base solo el 9% multiplicaba el factor por once — el recorte de
cordura lo dejaba en 100%, o sea el doble del costo de cada importado, y
avisaba en el log; pero el número seguía siendo falso.

Dos cambios:

1. **El default deja de prorratear.** El driver `importacion_driver` arranca
   en "landed": el pedimento ya sabe a qué embarque pertenece, así que se
   captura con el landed cost de Odoo sobre la recepción y se capitaliza a
   los productos que lo causaron — la máquina carga su pedimento y el hilo el
   suyo. El módulo se limita a medir cuánta aduana se quedó en resultados
   (columnas nuevas en la conciliación, y un chequeo en el panel que compara
   el pool contra lo capitalizado).

2. **Si se elige prorratear** (`importacion_driver` = "compras"), la base es
   ahora TODO lo comprado a proveedor extranjero, no solo la reventa, y el
   recargo se aplica en la hoja comprada. Así el hilo importado carga su
   aduana en su propio costo y la receta la arrastra a la tela, sin tratar a
   la tela como importada. El país del proveedor es el discriminante, no la
   moneda: comprarle en dólares a ALPEK POLYESTER MEXICO no es importar.

Se recalculan los períodos existentes para deshacer el reparto anterior.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    periodos = env['qb.costo.factores'].search([]).mapped('period')
    for period in sorted(set(periodos)):
        env['qb.costo.producto'].action_recompute_period(period)

    ultimo = env['qb.costo.factores'].search([], order='period DESC', limit=1)
    if ultimo and ultimo.importacion_pool_month:
        _logger.info(
            'qb_capacidad_costeo: %s períodos recalculados. Aduana en '
            'resultados: %.2f/mes. Con el driver «landed» no se prorratea — '
            'captúrala con landed costs en las recepciones y la conciliación '
            'la verá bajar.', len(set(periodos)),
            ultimo.importacion_pool_month)

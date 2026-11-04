# -*- coding: utf-8 -*-
import csv
import io
import logging
import unicodedata

from odoo import fields, models, _
from odoo.tools import file_open

_logger = logging.getLogger(__name__)


def _tokens(nombre):
    """Nombre sin acentos ni mayúsculas, como conjunto de palabras (el orden apellidos/nombre varía)."""
    texto = unicodedata.normalize('NFD', nombre or '')
    texto = ''.join(c for c in texto if unicodedata.category(c) != 'Mn')
    return frozenset(texto.upper().split())


class HrEmployee(models.Model):
    _inherit = 'hr.employee'

    siglas = fields.Char(
        'Siglas', copy=False,
        help='Siglas del operador de Inspección que se imprimen en las etiquetas finales de tela acabada.')

    def action_cargar_siglas_iniciales(self):
        """Carga las siglas de MAQUINA Y OPERADOR INSPECCION.pdf. Empata por nombre sin importar el orden
        de las palabras ni los acentos. Sin @api.model: el botón del header manda ids (ver mrp.tara.producto)."""
        self = self.browse()
        self.check_access('write')
        with file_open('quimibond_rollos_bitacora/data/siglas_operadores.csv', 'rb') as f:
            filas = list(csv.DictReader(io.StringIO(f.read().decode('utf-8-sig'))))
        por_nombre = {}
        for emp in self.with_context(active_test=False).search([]):
            por_nombre.setdefault(_tokens(emp.name), emp)
        cargadas, sin_empleado = 0, []
        for fila in filas:
            emp = por_nombre.get(_tokens(fila['nombre']))
            if not emp:
                sin_empleado.append(fila['nombre'])
                continue
            if emp.siglas != fila['siglas']:
                emp.siglas = fila['siglas']
            cargadas += 1
        if sin_empleado:
            _logger.info('Siglas sin empleado en la base: %s', '; '.join(sin_empleado))
        mensaje = _('%(c)s empleados con siglas, %(s)s sin empleado en esta base.', c=cargadas, s=len(sin_empleado))
        if sin_empleado:
            mensaje += '\n' + ', '.join(sin_empleado)
        return {
            'type': 'ir.actions.client', 'tag': 'display_notification',
            'params': {'title': _('Siglas de operadores'), 'message': mensaje, 'type': 'success', 'sticky': True,
                       'next': {'type': 'ir.actions.client', 'tag': 'soft_reload'}},
        }

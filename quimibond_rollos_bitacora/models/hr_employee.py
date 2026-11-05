# -*- coding: utf-8 -*-
import csv
import io
import logging
import re
import unicodedata

from odoo import api, fields, models, _
from odoo.tools import file_open

_logger = logging.getLogger(__name__)


# 3.5.1 (Dirección, 2026-11-05): el número de trabajador se repite entre nóminas (S-85 Lorena, Q-85 Franco); la
# «Referencia de empleado» con prefijo es única. La planta es nómina semanal: el número solo se busca como S-<número>.
_REFERENCIA = re.compile(r'^([SQC])\s*-?\s*0*(\d+)$')
CAMPO_NUMERO = 'x_studio_nmero_de_trabajador'
CAMPO_REFERENCIA = 'registration_number'  # «Referencia de empleado» (nómina; no existe en Community sin nómina)


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

    @api.model
    def _qb_operador(self, numero):
        """Empleado que teclea su número en una estación (3.5.1). «85» se busca como la referencia S-85 (nómina semanal,
        toda la planta); una referencia completa («Q-7», «q7», «C-26») se busca tal cual. Si no hay referencia, cae al
        número de trabajador solo cuando un único empleado activo lo tiene: con número repetido entre nóminas no adivina."""
        texto = (numero or '').strip().upper()
        if not texto:
            return self.browse()
        # Con sudo: un operador sin permisos de RH busca en hr.employee.public, que no tiene la referencia ni el número
        # de trabajador (prueba 28). Solo se lee el nombre y las siglas.
        self = self.sudo()
        if texto.isdigit():
            referencia = 'S-%d' % int(texto)
        else:
            m = _REFERENCIA.match(texto)
            referencia = '%s-%d' % (m.group(1), int(m.group(2))) if m else texto
        if CAMPO_REFERENCIA in self._fields:
            emp = self.search([(CAMPO_REFERENCIA, '=', referencia)], limit=2)
            if len(emp) == 1:
                return emp
        if texto.isdigit() and CAMPO_NUMERO in self._fields:
            emp = self.search([(CAMPO_NUMERO, '=', texto)], limit=2)
            if len(emp) == 1:
                return emp
        return self.browse()

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

# -*- coding: utf-8 -*-
import csv
import io
import logging

from odoo import api, fields, models, _
from odoo.exceptions import UserError
from odoo.tools import file_open

_logger = logging.getLogger(__name__)

OPERACIONES = [
    ('rama', 'Rama'),
    ('clasificacion_final', 'Clasificación final'),
]

# Archivo de datos -> operación (diseño §9.2). «INSPECCION TARAS» se carga como tara de
# Clasificación final porque Inspección ya no pesa (decidido 2-nov-2026).
ARCHIVOS_INICIALES = [
    ('rama', 'taras_rama.csv'),
    ('clasificacion_final', 'taras_clasificacion_final.csv'),
]


class MrpTaraProducto(models.Model):
    """Tara por producto y operación (diseño §9.2).

    Rama y Clasificación final tienen taras distintas. Sin tara capturada se bloquea el pesaje:
    no se asume 0. Es independiente de ``mrp.tara`` de pesaje_rollos_tejido (tejido).
    """
    _name = 'mrp.tara.producto'
    _description = 'Tara por producto y operación'
    _order = 'default_code, operacion'

    product_tmpl_id = fields.Many2one(
        'product.template', 'Producto', required=True, index=True, ondelete='cascade')
    default_code = fields.Char(related='product_tmpl_id.default_code', string='Referencia', store=True)
    operacion = fields.Selection(OPERACIONES, 'Operación', required=True, index=True)
    tara_kg = fields.Float('Tara (kg)', digits=(12, 3), required=True)
    active = fields.Boolean(default=True)

    _unique_producto_operacion = models.Constraint(
        'unique(product_tmpl_id, operacion)',
        'Ya existe una tara para ese producto y esa operación.')
    _check_tara_positiva = models.Constraint(
        'CHECK(tara_kg > 0)', 'La tara debe ser mayor a cero.')

    @api.model
    def _codigo_alterno(self, codigo):
        """El código del otro producto del par kg/metros: con «I» al frente es el de kg (DAT P-D02-01)."""
        if not codigo:
            return False
        return codigo[1:] if codigo.startswith('I') else 'I' + codigo

    @api.model
    def _buscar_tara(self, product, operacion):
        """Registro de tara del producto para la operación, o vacío.

        Las taras vienen por artículo; se busca el producto mismo y, si no está, su par kg/metros
        (el mismo código con o sin la «I» al frente).
        """
        tmpl = product.product_tmpl_id
        registro = self.search([('product_tmpl_id', '=', tmpl.id), ('operacion', '=', operacion)], limit=1)
        if registro:
            return registro
        alterno = self._codigo_alterno(tmpl.default_code)
        if not alterno:
            return self.browse()
        tmpl_alterno = self.env['product.template'].search([('default_code', '=', alterno)], limit=1)
        if not tmpl_alterno:
            return self.browse()
        return self.search([('product_tmpl_id', '=', tmpl_alterno.id), ('operacion', '=', operacion)], limit=1)

    def action_cargar_iniciales(self):
        """Carga/actualiza las taras de los archivos de Quimibond. Empata por referencia interna:
        primero el «I…» (kg) y, si no existe, el código tal cual. Lo que no empata se reporta.

        Sin @api.model a propósito: el botón del <header> de la lista manda los ids seleccionados
        como argumento y un método api.model reventaba con «takes 1 positional argument». La
        selección se ignora; trabaja siempre sobre todo el modelo."""
        self = self.browse()
        self.check_access('create')
        lineas = []
        for operacion, archivo in ARCHIVOS_INICIALES:
            with file_open('quimibond_rollos_bitacora/data/' + archivo, 'rb') as f:
                filas = list(csv.DictReader(io.StringIO(f.read().decode('utf-8-sig'))))
            codigos = {fila['codigo'].strip() for fila in filas}
            buscar = codigos | {'I' + c for c in codigos}
            tmpl_por_codigo = {
                t.default_code: t for t in self.env['product.template'].search([('default_code', 'in', list(buscar))])
            }
            existentes = {
                t.product_tmpl_id.id: t for t in self.with_context(active_test=False).search([('operacion', '=', operacion)])
            }
            creadas = actualizadas = 0
            sin_producto = []
            for fila in filas:
                codigo = fila['codigo'].strip()
                tara = float(fila['tara_kg'])
                tmpl = tmpl_por_codigo.get('I' + codigo) or tmpl_por_codigo.get(codigo)
                if not tmpl:
                    sin_producto.append(codigo)
                    continue
                registro = existentes.get(tmpl.id)
                if registro:
                    if registro.tara_kg != tara or not registro.active:
                        registro.write({'tara_kg': tara, 'active': True})
                        actualizadas += 1
                else:
                    existentes[tmpl.id] = self.create(
                        {'product_tmpl_id': tmpl.id, 'operacion': operacion, 'tara_kg': tara})
                    creadas += 1
            etiqueta = dict(OPERACIONES)[operacion]
            lineas.append(_('%(op)s: %(c)s nuevas, %(a)s actualizadas, %(s)s sin producto en esta base.',
                            op=etiqueta, c=creadas, a=actualizadas, s=len(sin_producto)))
            if sin_producto:
                _logger.info('Taras de %s sin producto en la base: %s', etiqueta, ', '.join(sin_producto))
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': _('Taras iniciales'),
                'message': '\n'.join(lineas) + '\n' + _('Los códigos sin producto quedan en el log del servidor.'),
                'type': 'success',
                'sticky': True,
                'next': {'type': 'ir.actions.client', 'tag': 'soft_reload'},
            },
        }

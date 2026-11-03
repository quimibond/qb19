# -*- coding: utf-8 -*-
from odoo import _, api, fields, models
from odoo.exceptions import UserError


def _lista(texto):
    return [parte.strip().upper() for parte in (texto or '').replace(';', ',').split(',') if parte.strip()]


class MrpInspeccionRestriccion(models.Model):
    """Restricciones de Inspección por centro de trabajo (diseño §3.2), sobre el código del artículo
    SIN el prefijo «I» (el «I…» de kg es el mismo código que el de metros).

    * prefijos_excluidos: el centro no revisa artículos cuyo código empieza así (centro 9: «WD»).
    * codigos_exclusivos: esos artículos se revisan SOLO en este centro (centro 9: WN075Q66JBL205 y
      WJ070Q66JBL205); los demás centros los rechazan.
    """
    _name = 'mrp.inspeccion.restriccion'
    _description = 'Restricción de Inspección por centro de trabajo'
    _rec_name = 'workcenter_id'

    workcenter_id = fields.Many2one('mrp.workcenter', 'Centro de Inspección', required=True, ondelete='cascade')
    prefijos_excluidos = fields.Char('Prefijos de código excluidos', help='Separados por coma. Ej.: WD')
    codigos_exclusivos = fields.Char(
        'Artículos exclusivos de este centro', help='Separados por coma; solo se revisan aquí.')
    active = fields.Boolean(default=True)

    _unique_centro = models.Constraint(
        'unique(workcenter_id)', 'Ese centro de trabajo ya tiene una restricción.')

    @api.model
    def _codigo_base(self, product):
        codigo = (product.default_code or '').strip().upper()
        return codigo[1:] if codigo.startswith('I') else codigo

    @api.model
    def _validar_centro(self, workcenter, product):
        """Lanza UserError si el artículo no se puede revisar en ese centro de Inspección."""
        codigo = self._codigo_base(product)
        if not codigo:
            return
        reglas = self.search([])
        for regla in reglas:
            if regla.workcenter_id != workcenter and codigo in _lista(regla.codigos_exclusivos):
                raise UserError(_(
                    'El artículo %(codigo)s solo se revisa en %(centro)s, no en %(actual)s.',
                    codigo=codigo, centro=regla.workcenter_id.name, actual=workcenter.name))
        regla = reglas.filtered(lambda r: r.workcenter_id == workcenter)[:1]
        for prefijo in _lista(regla.prefijos_excluidos):
            if codigo.startswith(prefijo):
                raise UserError(_(
                    '%(centro)s no revisa artículos que empiezan con %(prefijo)s (artículo %(codigo)s).',
                    centro=workcenter.name, prefijo=prefijo, codigo=codigo))

    def action_cargar_iniciales(self):
        """Carga las reglas del archivo «centros de trabajo inspección» (centro 9). Plain method:
        el botón de la lista manda ids."""
        self = self.browse()
        centro = self.env['mrp.workcenter'].search([('code', '=', 'INSP9')], limit=1) or \
            self.env['mrp.workcenter'].search([('name', '=', 'INSPECCION 9')], limit=1)
        if not centro:
            raise UserError(_('No existe el centro de trabajo INSPECCION 9 en esta base.'))
        valores = {'prefijos_excluidos': 'WD', 'codigos_exclusivos': 'WN075Q66JBL205,WJ070Q66JBL205'}
        regla = self.with_context(active_test=False).search([('workcenter_id', '=', centro.id)], limit=1)
        if regla:
            regla.write(dict(valores, active=True))
        else:
            self.create(dict(valores, workcenter_id=centro.id))
        return {
            'type': 'ir.actions.client', 'tag': 'display_notification',
            'params': {'type': 'success', 'sticky': False,
                       'message': _('Restricciones de %s cargadas.', centro.name)},
        }

# -*- coding: utf-8 -*-
"""Tramo que Inspección le quita a un rollo con defectos (Junta con el cliente, 2026-11-13).

La revisadora corta lo defectuoso de un rollo y lo aparta: se pesa con la tara de Inspección, se miden sus metros y se
anotan sus defectos. Al final del baño todos los tramos se juntan en un rollo nuevo de FE o Segunda (``mrp.rollo.final``
con ``es_tramos``), que lleva su lote, su etiqueta y la unión de los defectos, como parte de la orden en kg. El tramo no
mueve inventario: lo que se le quita al rollo de Rama sale de su rollo final (que se pesa ya sin el tramo) y entra al
rollo de tramos.
"""
from odoo import api, fields, models, _
from odoo.exceptions import UserError


class MrpRolloTramo(models.Model):
    _name = 'mrp.rollo.tramo'
    _description = 'Tramo quitado a un rollo (Inspección)'
    _order = 'production_id desc, id'

    production_id = fields.Many2one('mrp.production', 'Orden (kg)', required=True, readonly=True, index=True,
                                    ondelete='cascade')
    bitacora_id = fields.Many2one('mrp.rollo.bitacora', 'Rollo de Rama', required=True, readonly=True, index=True,
                                  ondelete='cascade', help='El rollo al que se le quitó el tramo.')
    company_id = fields.Many2one(related='production_id.company_id', store=True)
    kg_bruto = fields.Float('Peso bruto (kg)', digits=(12, 4), readonly=True)
    tara = fields.Float('Tara de Inspección (kg)', digits=(12, 4), readonly=True)
    kg_neto = fields.Float('Kg netos', digits=(12, 4), readonly=True)
    captura_manual = fields.Boolean('Captura manual', readonly=True)
    motivo_manual = fields.Char('Motivo de captura manual', readonly=True)
    metros = fields.Float('Metros', digits=(12, 2), readonly=True)
    defecto_ids = fields.Many2many('calidad.defecto.tela', 'mrp_rollo_tramo_defecto_rel', 'tramo_id', 'defecto_id',
                                   'Defectos', readonly=True)
    siglas_defectos = fields.Char('Siglas de defectos', compute='_compute_siglas')
    workcenter_id = fields.Many2one('mrp.workcenter', 'Revisadora', readonly=True)
    operador = fields.Char('Operador de Inspección', readonly=True)
    fecha = fields.Datetime('Quitado el', readonly=True, default=fields.Datetime.now)
    rollo_final_id = fields.Many2one('mrp.rollo.final', 'Rollo de tramos', readonly=True, copy=False, index=True,
                                     ondelete='set null',
                                     help='El rollo de FE o Segunda en el que se juntó. Vacío mientras el tramo esté pendiente.')

    _check_kg = models.Constraint('CHECK(kg_neto > 0)', 'Los kg netos del tramo deben ser mayores a cero.')
    _check_metros = models.Constraint('CHECK(metros > 0)', 'Los metros del tramo deben ser mayores a cero.')

    @api.depends('defecto_ids.clave')
    def _compute_siglas(self):
        for tramo in self:
            tramo.siglas_defectos = ' '.join(tramo.defecto_ids.sorted('clave').mapped('clave'))

    def unlink(self):
        """Un tramo ya juntado en un rollo de tramos no se borra (primero se anula ese rollo)."""
        if self.filtered('rollo_final_id'):
            raise UserError(_('El tramo ya se juntó en el rollo %s: anula primero ese rollo de tramos.',
                              ', '.join(self.rollo_final_id.mapped('name'))))
        if not self.env.user.has_group('mrp.group_mrp_manager'):
            raise UserError(_('Solo el responsable de Manufactura borra un tramo.'))
        for tramo in self:
            tramo.production_id.message_post(body=_(
                'Tramo de %(kg).2f kg y %(m).2f m del rollo %(r)s borrado por %(u)s.', kg=tramo.kg_neto,
                m=tramo.metros, r=tramo.bitacora_id.name, u=self.env.user.name))
        return super().unlink()

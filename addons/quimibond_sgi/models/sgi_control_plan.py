# -*- coding: utf-8 -*-
import base64

from odoo import models, fields, api
from odoo.exceptions import UserError


class QualityPoint(models.Model):
    _inherit = 'quality.point'

    sgi_control_plan_id = fields.Many2one('sgi.control.plan', string="Plan de control",
                                          ondelete='set null', index=True)
    # Inversa de la liga de migración: qué formatos controlados sustituye
    # este worksheet (para navegar en ambos sentidos).
    sgi_replaced_document_ids = fields.One2many(
        'documents.document', 'sgi_migration_point_id',
        string="Formatos que sustituye")
    sgi_replaced_document_count = fields.Integer(
        compute='_compute_sgi_replaced_document_count',
        string="# Formatos sustituidos")

    def _compute_sgi_replaced_document_count(self):
        for point in self:
            point.sgi_replaced_document_count = len(point.sgi_replaced_document_ids)

    def action_sgi_open_replaced_documents(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Formatos que sustituye",
            'res_model': 'documents.document',
            'view_mode': 'list,form',
            'domain': [('id', 'in', self.sgi_replaced_document_ids.ids)],
        }
    sgi_characteristic = fields.Char(string="Característica",
                                     help="Característica de la Master Spec del cliente.")
    sgi_criticality = fields.Selection([
        ('f', "F - Función"),
        ('r', "R - Regulación"),
        ('s', "S - Seguridad"),
    ], string="Criticidad (F/R/S)",
        help="Esquema de criticidad tipo Continental: F=Función, R=Regulación, S=Seguridad.")
    sgi_in_coa = fields.Boolean(string="Aparece en el Certificado de Calidad",
                                help="Si está marcado, esta característica se imprime en el "
                                     "Certificado de Conformidad (CoA) del lote.")
    sgi_cpk_target = fields.Float(string="Cpk objetivo", digits=(4, 2),
                                  help="Cpk objetivo sugerido: 1.33 para F, 1.67 para R/S.")
    sgi_reaction_plan = fields.Text(string="Plan de reacción")
    sgi_equipment_id = fields.Many2one(
        'maintenance.equipment', string="Equipo de medición",
        domain=[('sgi_is_measuring', '=', True)],
        help="Instrumento con el que se mide esta característica. Su calibración "
             "se verifica al registrar la inspección (IATF 7.1.5.2.1).")


class SgiControlPlan(models.Model):
    _name = 'sgi.control.plan'
    _description = "Plan de control (P-C11)"
    _inherit = ['sgi.base.mixin']
    _order = 'folio desc'
    _sgi_sequence_code = 'sgi.control.plan'

    _folio_uniq = models.Constraint(
        'unique(folio)',
        "Ya existe un plan de control con ese folio.",
    )

    name = fields.Char(string="Nombre", required=True, tracking=True)
    partner_id = fields.Many2one('res.partner', string="Cliente",
                                 domain="[('is_company', '=', True)]")
    product_tmpl_ids = fields.Many2many('product.template', string="Productos")
    phase = fields.Selection([
        ('prototipo', "Prototipo"),
        ('prelanzamiento', "Prelanzamiento"),
        ('produccion', "Producción"),
    ], string="Fase", default='produccion', required=True, tracking=True)
    revision = fields.Char(string="Revisión", default="00", tracking=True)
    state = fields.Selection([
        ('borrador', "Borrador"),
        ('vigente', "Vigente"),
        ('obsoleto', "Obsoleto"),
    ], string="Estado", default='borrador', required=True, tracking=True)
    point_ids = fields.One2many('quality.point', 'sgi_control_plan_id',
                                string="Puntos de control")
    point_count = fields.Integer(string="N° de puntos", compute='_compute_point_count')
    fmea_ids = fields.One2many('sgi.fmea', 'control_plan_id', string="AMEF ligados")
    fmea_count = fields.Integer(string="# AMEF", compute='_compute_fmea_count')
    document_id = fields.Many2one('documents.document', string="Especificación del cliente")
    notes = fields.Text(string="Notas")


    @api.depends('point_ids')
    def _compute_point_count(self):
        for plan in self:
            plan.point_count = len(plan.point_ids)

    @api.depends('fmea_ids')
    def _compute_fmea_count(self):
        for plan in self:
            plan.fmea_count = len(plan.fmea_ids)

    def action_view_fmeas(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "AMEF — %s" % (self.folio or self.name),
            'res_model': 'sgi.fmea',
            'view_mode': 'list,form',
            'domain': [('control_plan_id', '=', self.id)],
            'context': {'default_control_plan_id': self.id},
        }

    def action_set_vigente(self):
        for plan in self:
            if not plan.point_ids:
                raise UserError(
                    "El plan de control %s no puede pasar a Vigente sin al menos "
                    "un punto de control." % (plan.folio or plan.name))
            plan.state = 'vigente'
        return True

    def action_set_borrador(self):
        self.write({'state': 'borrador'})
        return True

    def action_set_obsoleto(self):
        Cron = self.env['sgi.cron']
        manager_id = Cron._sgi_manager_user_id()
        for plan in self:
            plan.state = 'obsoleto'
            # No se desactivan los quality.point automáticamente (pueden vivir en
            # otro plan): se agenda una revisión al Jefe MAST.
            if plan.point_ids and manager_id:
                Cron._sgi_schedule(
                    plan,
                    "Revisar puntos del plan obsoleto %s" % (plan.folio or plan.name),
                    "El plan de control pasó a obsoleto. Revise si sus %d punto(s) de "
                    "control deben desactivarse o reasignarse a otro plan vigente." % len(plan.point_ids),
                    manager_id)
        return True

    def action_view_points(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Puntos de control",
            'res_model': 'quality.point',
            'view_mode': 'list,form',
            'domain': [('sgi_control_plan_id', '=', self.id)],
            'context': {'default_sgi_control_plan_id': self.id},
        }

    def action_open_orphan_points(self):
        """Puntos de calidad reales del piso que no pertenecen a ningún plan.

        En producción la retro-vinculación por nombre de equipo puede no
        encontrar nada (los equipos se renombran): este botón abre los puntos
        sueltos con el plan actual como default para ligarlos en un paso —
        sin un punto ligado no hay CoA ni cadena IATF."""
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Puntos sin plan — ligar a %s" % (self.folio or self.name),
            'res_model': 'quality.point',
            'view_mode': 'list,form',
            'domain': [('sgi_control_plan_id', '=', False)],
            'context': {'default_sgi_control_plan_id': self.id},
        }


class StockLot(models.Model):
    _inherit = 'stock.lot'

    def _sgi_coa_checks(self):
        """Devuelve los quality.check del lote ligados a puntos con sgi_in_coa."""
        self.ensure_one()
        return self.env['quality.check'].search([
            ('lot_ids', 'in', self.ids),
            ('point_id.sgi_in_coa', '=', True),
        ])

    def action_sgi_print_coa(self):
        self.ensure_one()
        if not self._sgi_coa_checks():
            raise UserError(
                "El lote %s no tiene inspecciones de calidad ligadas a puntos "
                "marcados para el Certificado de Calidad. Capture los quality.check "
                "correspondientes antes de emitir el CoA." % self.name)
        return self.env.ref('quimibond_sgi.action_report_coa').report_action(self)

    def _sgi_delivery_pickings(self):
        """Entregas (salidas) en las que participó este lote."""
        self.ensure_one()
        move_lines = self.env['stock.move.line'].search([
            ('lot_id', '=', self.id),
            ('picking_id.picking_type_id.code', '=', 'outgoing'),
        ])
        return move_lines.mapped('picking_id')

    def action_sgi_publish_coa(self):
        """Publica el CoA en el portal del cliente: adjunta el PDF a la(s)
        entrega(s) del lote (message_post), sin automatismo (botón explícito)."""
        self.ensure_one()
        if not self._sgi_coa_checks():
            raise UserError(
                "El lote %s no tiene inspecciones para el Certificado de Calidad." % self.name)
        pickings = self._sgi_delivery_pickings()
        if not pickings:
            raise UserError(
                "El lote %s no está en ninguna entrega; no hay dónde publicar el CoA." % self.name)
        report = self.env.ref('quimibond_sgi.action_report_coa')
        pdf_content, _ = self.env['ir.actions.report']._render_qweb_pdf(
            report.report_name, self.ids)
        for picking in pickings:
            attachment = self.env['ir.attachment'].create({
                'name': "CoA-%s.pdf" % self.name,
                'type': 'binary',
                'datas': base64.b64encode(pdf_content),
                'res_model': 'stock.picking',
                'res_id': picking.id,
                'mimetype': 'application/pdf',
            })
            picking.message_post(
                body="Certificado de Calidad (CoA) del lote %s publicado para el "
                     "cliente." % self.name,
                attachment_ids=[attachment.id])
        return True

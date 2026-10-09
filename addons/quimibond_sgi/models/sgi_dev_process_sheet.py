# -*- coding: utf-8 -*-
"""Ruta y ficha de proceso de tejido (57.126.0, C1 bloque G; brief §6.7).

- La **ruta** son las operaciones de la lista de materiales (C1.04b). El
  diagrama de flujo de proceso (F-P-D01-32) se **imprime** desde ahí; no se
  dibuja.
- **Tejido** usa `sgi.machine.sheet`, que ya existía: aquí se corrigen sus
  firmas a puestos vigentes (propone Diseño y Desarrollo de Procesos, valida
  el Jefe de Manufactura, mide Laboratorio) y cada parámetro gana el valor
  **real** de la corrida, el número de ajuste y el motivo de lista.
- Las fichas de **tintorería y acabado** no van aquí: las construye Jose
  Sacramento en `quimibond_ficha_tecnica_tela` (decisión de Jose,
  2026-10-08). El punto de enganche es la orden de muestra
  (`mrp.production.sgi_dev_project_id`, sus órdenes de trabajo y el botón
  «Validar corrida»).
"""
from odoo import api, fields, models
from odoo.exceptions import UserError

PARAM_VALIDATOR_JOB_TEJIDO = 'quimibond_sgi.dev_validator_job_tejido_id'
VALIDATOR_JOB_NAME_TEJIDO = 'JEFE DE MANUFACTURA'


class ProjectProjectDevRoute(models.Model):
    _inherit = 'project.project'

    def _sgi_dev_route_operations(self):
        """[(artículo, operación)] en el orden de la ruta: crudo → teñido → acabado, y dentro de cada
        artículo las operaciones de su lista de materiales."""
        self.ensure_one()
        out = []
        Bom = self.env['mrp.bom'].sudo()
        for product in (self.sgi_dev_product_crudo_id | self.sgi_dev_product_tenido_id | self.sgi_dev_product_id):
            bom = Bom._bom_find(product, company_id=self.company_id.id).get(product)
            if not bom:
                continue
            for op in bom.operation_ids.sorted(lambda o: (o.sequence, o.id)):
                out.append((product, op))
        return out

    def action_sgi_dev_print_flow(self):
        self.ensure_one()
        if not self._sgi_dev_route_operations():
            raise UserError("El diagrama de flujo se imprime desde la ruta: capture las operaciones en la lista de "
                            "materiales del artículo (C1.04b).")
        return self.env.ref('quimibond_sgi.action_report_dev_flow').report_action(self)


class MrpBomDevRoute(models.Model):
    """57.127.0 (Jose, 1a): la ruta se atribuye a quien la asigna, no al último que editó la lista."""
    _inherit = 'mrp.bom'

    sgi_route_assigned_by_id = fields.Many2one('res.users', string="Ruta asignada por", readonly=True, copy=False,
                                               help="Quien capturó o cambió por última vez las operaciones (C1.04b).")
    sgi_route_assigned_date = fields.Datetime(string="Ruta asignada el", readonly=True, copy=False)

    def _sgi_stamp_route(self):
        vals = {'sgi_route_assigned_by_id': self.env.uid, 'sgi_route_assigned_date': fields.Datetime.now()}
        for bom in self:
            if bom.operation_ids:
                super(MrpBomDevRoute, bom).write(vals)
            elif bom.sgi_route_assigned_by_id:
                super(MrpBomDevRoute, bom).write({'sgi_route_assigned_by_id': False, 'sgi_route_assigned_date': False})

    @api.model_create_multi
    def create(self, vals_list):
        boms = super().create(vals_list)
        boms.filtered('operation_ids')._sgi_stamp_route()
        return boms

    def write(self, vals):
        res = super().write(vals)
        if 'operation_ids' in vals:
            self._sgi_stamp_route()
        return res


class MrpRoutingWorkcenterDevRoute(models.Model):
    _inherit = 'mrp.routing.workcenter'

    @api.model_create_multi
    def create(self, vals_list):
        ops = super().create(vals_list)
        ops.mapped('bom_id')._sgi_stamp_route()
        return ops

    def write(self, vals):
        res = super().write(vals)
        if vals.keys() & {'workcenter_id', 'name', 'time_cycle_manual', 'sequence', 'bom_id'}:
            self.mapped('bom_id')._sgi_stamp_route()
        return res

    def unlink(self):
        boms = self.mapped('bom_id')
        res = super().unlink()
        boms.exists()._sgi_stamp_route()
        return res


class SgiMachineSheetDev(models.Model):
    """La ficha de tejido con las firmas por puestos vigentes y propuesto / real por parámetro."""
    _inherit = 'sgi.machine.sheet'

    project_id = fields.Many2one('project.project', string="Proyecto de desarrollo", index=True, ondelete='set null',
                                 domain="[('sgi_is_ft', '=', True), ('is_template', '=', False)]")
    production_id = fields.Many2one('mrp.production', string="Orden de la corrida", ondelete='set null')
    proposed_date = fields.Datetime(readonly=True, copy=False)
    validated_date = fields.Datetime(readonly=True, copy=False)
    lab_date = fields.Datetime(copy=False)
    # 57.126.0: las firmas viejas («Jefe técnico de tejido y acabado», «Jefe de ingeniería de
    # procesos») no existen como puestos; las columnas se quedan con el papel vigente.
    engineering_by_id = fields.Many2one(string="Propuso (Diseño y Desarrollo de Procesos)", readonly=True, copy=False)
    approved_by_id = fields.Many2one(string="Validó (Jefe de Manufactura)", readonly=True, copy=False)
    lab_user_id = fields.Many2one(string="Midió (Laboratorio)")

    @api.model
    def _sgi_dev_user_has_job(self, user, param_key):
        """El usuario tiene el puesto del parámetro o es jefe directo de quien lo
        tiene (57.143.0, suplencia)."""
        raw = self.env['ir.config_parameter'].sudo().get_param(param_key, '') or ''
        if not raw.strip().isdigit():
            return False
        job = self.env['hr.job'].sudo().browse(int(raw)).exists()
        return bool(job) and user in job._sgi_users(with_bosses=True)

    def action_propose(self):
        for sheet in self:
            if sheet.state != 'borrador':
                raise UserError("Solo un borrador se propone.")
            sheet.write({'engineering_by_id': self.env.uid, 'proposed_date': fields.Datetime.now()})
            sheet.message_post(body="Parámetros propuestos por %s." % self.env.user.name)
        return True

    def action_validate(self):
        for sheet in self:
            if not sheet.engineering_by_id:
                raise UserError("Primero propone Diseño de Procesos.")
            if not (self.env.user.has_group('quimibond_sgi.group_sgi_manager')
                    or self._sgi_dev_user_has_job(self.env.user, PARAM_VALIDATOR_JOB_TEJIDO)):
                raise UserError("La ficha de tejido la valida el Jefe de Manufactura (Ajustes → SGI → Desarrollo de "
                                "producto), su jefe directo o un administrador del SGI.")
            sheet.write({'approved_by_id': self.env.uid, 'validated_date': fields.Datetime.now()})
            sheet.message_post(body="Parámetros reales validados por %s." % self.env.user.name)
        return True


class SgiMachineSheetParamDev(models.Model):
    _inherit = 'sgi.machine.sheet.param'

    real = fields.Char(string="Real (corrida)", help="Lo que se corrió de verdad; lo valida el supervisor.")
    adjustment = fields.Integer(string="Número de ajuste")
    reason_id = fields.Many2one('sgi.dev.option', string="Motivo del ajuste", domain="[('kind', '=', 'motivo_ajuste')]")

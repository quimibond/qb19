# -*- coding: utf-8 -*-
"""Estructura del catálogo: etapas del proceso y entregables.

Menos texto, más piezas que se conectan:

- ``sgi.process.stage``: las etapas de un proceso (A. Recepción, B.
  Almacenaje…). Sustituyen el texto libre «sección» y el bloque fijo
  inicial/desarrollo/final: se ordenan y se renombran una vez.
- ``sgi.deliverable``: lo que pasa de una actividad a otra (el pedido
  confirmado, el programa semanal, el lote liberado). Se captura una vez, con
  su formato, su modelo de Odoo y su criterio de aceptación. Cada actividad
  dice qué entregables RECIBE y cuáles ENTREGA; con eso las ligas entre
  actividades, los flujos entre procesos y las entradas/salidas de cada
  proceso se calculan solos (``_sgi_sync_connections``).
"""
import re

from markupsafe import Markup

from odoo import models, fields, api
from odoo.exceptions import UserError, ValidationError
from odoo.tools.safe_eval import safe_eval

from .sgi_calendar import sgi_business_days

RE_STAGE = re.compile(r'^\s*([A-Za-z0-9]+(?:\.[0-9]+)*)[\.\)\-:]?\s+(.+?)\s*$')


class SgiProcessStage(models.Model):
    _name = 'sgi.process.stage'
    _description = "Etapa de un proceso SGI"
    _order = 'process_id, sequence, code, id'

    process_id = fields.Many2one(
        'sgi.process', string="Proceso", required=True, ondelete='cascade', index=True)
    sequence = fields.Integer(default=10)
    code = fields.Char(string="Letra", help="A, B, C… (opcional).")
    name = fields.Char(string="Etapa", required=True)
    activity_ids = fields.One2many('sgi.process.activity', 'stage_id', string="Actividades")
    activity_count = fields.Integer(compute='_compute_activity_count', string="Actividades")
    company_id = fields.Many2one(
        related='process_id.company_id', store=True, index=True, string="Empresa")

    _process_name_uniq = models.Constraint(
        'unique(process_id, name)',
        "La etapa ya existe en el proceso.",
    )

    @api.depends('code', 'name')
    def _compute_display_name(self):
        for stage in self:
            stage.display_name = "%s. %s" % (stage.code, stage.name) if stage.code else stage.name

    def _compute_activity_count(self):
        counts = {}
        if self.ids:
            counts = {stage.id: count for stage, count in self.env['sgi.process.activity']._read_group(
                [('stage_id', 'in', self.ids)], ['stage_id'], ['__count'])}
        for stage in self:
            stage.activity_count = counts.get(stage.id, 0)

    @api.model
    def _sgi_get_or_create(self, process, text):
        """«D. Inventario» → etapa (D, Inventario) del proceso; la crea si no
        existe. Es parte del propio proceso, no un catálogo global."""
        text = ' '.join((text or '').split())
        if not text or not process:
            return self.browse()
        match = RE_STAGE.match(text)
        code, name = (match.group(1), match.group(2)) if match else (False, text)
        stage = self.with_context(active_test=False).search(
            [('process_id', '=', process.id), ('name', '=ilike', name)], limit=1)
        if not stage:
            last = self.search([('process_id', '=', process.id)], order='sequence desc', limit=1)
            stage = self.create({'process_id': process.id, 'code': code, 'name': name,
                                 'sequence': (last.sequence or 0) + 10})
        elif code and stage.code != code:
            stage.code = code
        return stage


class SgiDeliverable(models.Model):
    _name = 'sgi.deliverable'
    _description = "Entregable SGI (lo que pasa de una actividad a otra)"
    _order = 'name'

    code = fields.Char(
        string="Código", index=True,
        help="Clave corta para la carga por API (C2-PEDIDO, C3-PROGRAMA…).")
    name = fields.Char(string="Entregable", required=True,
                       help="Qué pasa de un paso al otro: «Pedido confirmado».")
    document_id = fields.Many2one(
        'documents.document', string="Formato",
        domain=[('sgi_is_controlled', '=', True)],
        help="Formato controlado en el que viaja, si aplica.")
    odoo_model_id = fields.Many2one(
        'ir.model', string="Modelo de Odoo", ondelete='set null',
        help="Dónde vive el entregable en Odoo (sale.order, stock.picking…). "
             "Con modelo, la actividad que lo entrega se puede medir con él.")
    odoo_model_name = fields.Char(related='odoo_model_id.model', string="Modelo técnico")
    # El entregable también mide: modelo + dominio + fecha (+ usuario) dicen
    # cuándo quedó entregado. La actividad que lo entrega con el método
    # «Por su entregable» copia esto y no se captura dos veces.
    measure_domain = fields.Char(
        string="Filtro: ya está entregado", default='[]',
        help="Qué registros cuentan como entregados, ej. [('state', '=', 'sale')].")
    measure_date_field = fields.Char(
        string="Campo de fecha", default='create_date',
        help="Cuándo quedó entregado: date_order, date_done, invoice_date…")
    measure_user_field = fields.Char(
        string="Campo de usuario",
        help="Quién lo entregó (create_uid, user_id…): mide si lo hizo el puesto "
             "que debía.")
    acceptance_criteria = fields.Text(string="Criterio de aceptación")
    active = fields.Boolean(default=True)
    company_id = fields.Many2one(
        'res.company', string="Empresa", required=True, index=True,
        default=lambda self: self.env.company)

    producer_activity_ids = fields.Many2many(
        'sgi.process.activity', 'sgi_activity_output_rel', 'deliverable_id', 'activity_id',
        string="Lo entregan")
    input_line_ids = fields.One2many('sgi.activity.input', 'deliverable_id', string="Lo reciben")
    consumer_activity_ids = fields.Many2many(
        'sgi.process.activity', compute='_compute_consumers', string="Actividades que lo reciben")
    measured_activity_ids = fields.One2many(
        'sgi.process.activity', 'measure_deliverable_id', string="Se miden con él")
    link_ids = fields.One2many('sgi.activity.link', 'deliverable_id', string="Ligas",
                               context={'active_test': False})
    producer_process_ids = fields.Many2many(
        'sgi.process', compute='_compute_processes', string="Procesos que lo entregan")
    consumer_process_ids = fields.Many2many(
        'sgi.process', compute='_compute_processes', string="Procesos que lo reciben")
    orphan = fields.Selection([
        ('sin_origen', "Nadie lo entrega"),
        ('sin_destino', "Nadie lo recibe"),
    ], compute='_compute_processes', string="Cabo suelto",
        help="Un entregable que alguien recibe pero nadie entrega (o al revés) es "
             "un hueco en la cadena.")

    _code_company_uniq = models.Constraint(
        'unique(code, company_id)',
        "El código del entregable debe ser único por empresa.",
    )

    _SGI_MEASURE_KEYS = ('odoo_model_id', 'measure_domain', 'measure_date_field',
                         'measure_user_field')

    @api.depends('input_line_ids.activity_id')
    def _compute_consumers(self):
        for deliverable in self:
            deliverable.consumer_activity_ids = deliverable.input_line_ids.activity_id

    @api.depends('producer_activity_ids.process_id', 'input_line_ids.activity_id.process_id')
    def _compute_processes(self):
        for deliverable in self:
            producers = deliverable.producer_activity_ids
            consumers = deliverable.input_line_ids.activity_id
            deliverable.producer_process_ids = producers.process_id
            deliverable.consumer_process_ids = consumers.process_id
            if consumers and not producers:
                deliverable.orphan = 'sin_origen'
            elif producers and not consumers:
                deliverable.orphan = 'sin_destino'
            else:
                deliverable.orphan = False

    @api.constrains('odoo_model_id', 'measure_domain', 'measure_date_field', 'measure_user_field')
    def _check_measure(self):
        """Dominio con safe_eval (nunca eval) y campos que existen en el modelo."""
        for deliverable in self.filtered('odoo_model_id'):
            model_name = deliverable.odoo_model_id.model
            if model_name not in self.env:
                continue
            Model = self.env[model_name]
            try:
                domain = safe_eval(deliverable.measure_domain or '[]')
                if not isinstance(domain, (list, tuple)):
                    raise ValueError("no es una lista")
                Model.sudo().search_count(list(domain), limit=1)
            except Exception as exc:  # noqa: BLE001 - el mensaje va al usuario
                raise ValidationError("Entregable %s: filtro inválido para %s: %s" % (
                    deliverable.name, model_name, exc))
            for fname in (deliverable.measure_date_field, deliverable.measure_user_field):
                if fname and fname not in Model._fields:
                    raise ValidationError("Entregable %s: %s no tiene el campo «%s»." % (
                        deliverable.name, model_name, fname))
            user_field = deliverable.measure_user_field
            if user_field:
                field = Model._fields[user_field]
                if field.type != 'many2one' or field.comodel_name != 'res.users' or not field.store:
                    raise ValidationError(
                        "Entregable %s: «%s» no es un campo de usuario (res.users) "
                        "almacenado." % (deliverable.name, user_field))

    def _sgi_measure_vals(self):
        """Lo que copia la actividad que se mide con este entregable."""
        self.ensure_one()
        return {
            'measure_model_id': self.odoo_model_id.id,
            'measure_domain': self.measure_domain or '[]',
            'measure_date_field': self.measure_date_field or 'create_date',
            'measure_user_field': self.measure_user_field or False,
        }

    # ------------------------------------------------------------------
    # Conexiones calculadas
    # ------------------------------------------------------------------
    def _sgi_sync_connections(self):
        """Ligas entre actividades y flujos entre procesos que se derivan de
        quién entrega y quién recibe cada entregable: una liga por actividad
        que lo entrega × renglón «recibe»; un flujo por par de procesos
        distintos. Una liga desactivada con su motivo se queda desactivada;
        el flujo se apaga cuando todas sus ligas lo están. La liga o el flujo
        capturados a mano entre las mismas actividades o procesos se archivan:
        el entregable los reemplaza."""
        Link = self.env['sgi.activity.link'].sudo().with_context(
            active_test=False, sgi_connection_sync=True)
        Flow = self.env['sgi.process.flow'].sudo().with_context(
            active_test=False, sgi_connection_sync=True)
        for deliverable in self.with_context(active_test=False):
            producers = deliverable.producer_activity_ids.filtered('active')
            lines = deliverable.input_line_ids.filtered(lambda l: l.activity_id.active)
            if not deliverable.active:
                producers = producers.browse()
                lines = lines.browse()
            wanted = {(p.id, line.id) for p in producers for line in lines
                      if p != line.activity_id}
            existing = {(link.from_activity_id.id, link.input_id.id): link
                        for link in Link.search([('deliverable_id', '=', deliverable.id)])}
            stale = Link.browse([link.id for key, link in existing.items() if key not in wanted])
            if stale:
                stale.unlink()
            missing = sorted(wanted - set(existing))
            if missing:
                created = Link.create([{
                    'from_activity_id': frm, 'to_activity_id': Link.env['sgi.activity.input'].browse(
                        line_id).activity_id.id,
                    'input_id': line_id, 'deliverable_id': deliverable.id,
                    'name': deliverable.name} for frm, line_id in missing])
                created._sgi_retire_manual(deliverable)
            links = Link.search([('deliverable_id', '=', deliverable.id)])
            pairs = {}
            for link in links:
                if link.from_process_id != link.to_process_id:
                    key = (link.from_process_id.id, link.to_process_id.id)
                    pairs[key] = pairs.get(key, False) or link.active
            flows = {(flow.from_process_id.id, flow.to_process_id.id): flow
                     for flow in Flow.search([('deliverable_id', '=', deliverable.id)])}
            stale_flows = Flow.browse([flow.id for key, flow in flows.items() if key not in pairs])
            if stale_flows:
                stale_flows.unlink()
            for (frm, to), any_active in sorted(pairs.items()):
                flow = flows.get((frm, to))
                vals = {'active': any_active,
                        'inactive_reason': False if any_active
                        else "Todas sus ligas están desactivadas."}
                if flow:
                    if any(flow[k] != v for k, v in vals.items()):
                        flow.write(vals)
                    continue
                flow = Flow.create(dict(
                    vals, from_process_id=frm, to_process_id=to,
                    deliverable_id=deliverable.id, name=deliverable.name,
                    document_id=deliverable.document_id.id,
                    odoo_model_id=deliverable.odoo_model_id.id,
                    acceptance_criteria=deliverable.acceptance_criteria))
                flow._sgi_retire_manual(deliverable)

    def write(self, vals):
        res = super().write(vals)
        if {'producer_activity_ids', 'active'} & set(vals):
            self._sgi_sync_connections()
        # El nombre, el formato y el modelo viajan a las ligas y flujos que
        # salieron de este entregable.
        mirror = {k: vals[k] for k in ('name', 'document_id', 'odoo_model_id', 'acceptance_criteria')
                  if k in vals}
        if mirror:
            flows = self.env['sgi.process.flow'].sudo().with_context(
                active_test=False, sgi_connection_sync=True).search(
                [('deliverable_id', 'in', self.ids)])
            if flows:
                flows.write(mirror)
            if 'name' in mirror:
                links = self.env['sgi.activity.link'].sudo().with_context(
                    active_test=False, sgi_connection_sync=True).search(
                    [('deliverable_id', 'in', self.ids)])
                if links:
                    links.write({'name': mirror['name']})
        # La medición de las actividades que se miden con él.
        if set(self._SGI_MEASURE_KEYS) & set(vals):
            self.measured_activity_ids._sgi_apply_deliverable_measure()
        return res

    @api.model_create_multi
    def create(self, vals_list):
        deliverables = super().create(vals_list)
        deliverables._sgi_sync_connections()
        return deliverables

    def unlink(self):
        ctx = {'active_test': False, 'sgi_connection_sync': True}
        self.env['sgi.activity.link'].sudo().with_context(**ctx).search(
            [('deliverable_id', 'in', self.ids)]).unlink()
        self.env['sgi.process.flow'].sudo().with_context(**ctx).search(
            [('deliverable_id', 'in', self.ids)]).unlink()
        return super().unlink()

    def action_view_links(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Ligas — %s" % self.name,
            'res_model': 'sgi.activity.link',
            'view_mode': 'list',
            'domain': [('deliverable_id', '=', self.id)],
            'context': {'active_test': False},
        }


class SgiActivityInput(models.Model):
    """Un «recibe» de la actividad: qué entregable y en cuántos días hábiles
    debe llegar a ella. El plazo es de quien recibe (la misma salida puede
    urgirle a uno y no a otro) y de él sale el eslabón atorado."""
    _name = 'sgi.activity.input'
    _description = "Entregable que recibe una actividad SGI"
    _order = 'activity_id, sequence, id'

    activity_id = fields.Many2one(
        'sgi.process.activity', string="Actividad", required=True, ondelete='cascade', index=True)
    deliverable_id = fields.Many2one(
        'sgi.deliverable', string="Entregable", required=True, ondelete='restrict', index=True)
    sequence = fields.Integer(default=10)
    max_days = fields.Integer(
        string="Plazo (días hábiles)",
        help="Días hábiles que puede pasar entre que se entrega y que esta "
             "actividad lo toma. Pasado el plazo, el eslabón está atorado. "
             "0 = sin plazo (solo se revisa que ambas tengan evidencia).")
    process_id = fields.Many2one(related='activity_id.process_id', store=True, string="Proceso")
    company_id = fields.Many2one(related='activity_id.company_id', store=True, string="Empresa")

    _activity_deliverable_uniq = models.Constraint(
        'unique(activity_id, deliverable_id)',
        "La actividad ya recibe ese entregable.",
    )
    _max_days_positive = models.Constraint(
        'CHECK(max_days >= 0)',
        "El plazo no puede ser negativo.",
    )

    @api.constrains('activity_id', 'deliverable_id')
    def _check_not_own_output(self):
        for line in self:
            if line.deliverable_id in line.activity_id.output_deliverable_ids:
                raise ValidationError(
                    "La actividad %s no puede recibir y entregar el mismo entregable «%s»." % (
                        line.activity_id.display_name, line.deliverable_id.name))

    @api.model_create_multi
    def create(self, vals_list):
        lines = super().create(vals_list)
        lines.deliverable_id._sgi_sync_connections()
        lines.process_id._sgi_flag_procedure_dirty()
        return lines

    def write(self, vals):
        before = self.deliverable_id
        res = super().write(vals)
        if {'deliverable_id', 'activity_id'} & set(vals):
            (before | self.deliverable_id)._sgi_sync_connections()
        if set(vals) - {'sequence'}:
            self.process_id._sgi_flag_procedure_dirty()
        return res

    def unlink(self):
        deliverables, processes = self.deliverable_id, self.process_id
        res = super().unlink()
        deliverables.exists()._sgi_sync_connections()
        processes.exists()._sgi_flag_procedure_dirty()
        return res


class SgiProcessDeliverables(models.Model):
    """Entradas y salidas del proceso, calculadas de sus entregables (antes,
    dos campos de texto que repetían los flujos)."""
    _inherit = 'sgi.process'

    stage_ids = fields.One2many('sgi.process.stage', 'process_id', string="Etapas")
    input_deliverable_ids = fields.Many2many(
        'sgi.deliverable', compute='_compute_io_deliverables', string="Inicia con",
        help="Inicio del proceso: lo que sus actividades reciben y ninguna "
             "actividad del proceso produce.")
    output_deliverable_ids = fields.Many2many(
        'sgi.deliverable', compute='_compute_io_deliverables', string="Termina con",
        help="Fin del proceso: lo que sus actividades entregan y ninguna "
             "actividad del proceso recibe (sale hacia otros procesos o al cliente).")

    @api.depends('procedure_activity_ids.input_deliverable_ids',
                 'procedure_activity_ids.output_deliverable_ids')
    def _compute_io_deliverables(self):
        for process in self:
            acts = process.procedure_activity_ids
            consumed = acts.input_deliverable_ids
            produced = acts.output_deliverable_ids
            process.input_deliverable_ids = consumed - produced
            process.output_deliverable_ids = produced - consumed

    def _sgi_responsibilities_by_job(self):
        """Sección 3 del procedimiento generada de los roles: por puesto o
        familia (o rol relativo), qué ejecuta, aprueba, en qué participa y de
        qué se entera. [(etiqueta, [(rol, [actividades])])]"""
        self.ensure_one()
        labels = dict(self.env['sgi.activity.role']._fields['role'].selection)
        by_target = {}
        for act in self.procedure_activity_ids:
            for role in act.role_ids:
                target = role._sgi_target_label()
                by_target.setdefault(target, {}).setdefault(role.role, []).append(act)
        order = ['ejecuta', 'aprueba', 'participa', 'informa', 'escala']
        return [
            (target, [(labels[r], roles[r]) for r in order if r in roles])
            for target, roles in sorted(by_target.items())
        ]


class _SgiCalculatedConnection(models.AbstractModel):
    """Lo común a ligas y flujos calculados: no se editan a mano; si uno no
    aplica se desactiva con su motivo. Los capturados a mano (sin entregable)
    son de la versión anterior: se archivan cuando un entregable cubre las
    mismas actividades o procesos."""
    _name = 'sgi.calculated.connection.mixin'
    _description = "Conexión calculada de un entregable"

    deliverable_id = fields.Many2one(
        'sgi.deliverable', string="Entregable (catálogo)", index=True, ondelete='cascade',
        help="Si viene de un entregable, se calcula sola de quién lo entrega y "
             "quién lo recibe; no se edita a mano.")
    active = fields.Boolean(default=True)
    inactive_reason = fields.Char(
        string="Motivo de desactivación",
        help="Por qué esta conexión calculada no aplica (obligatorio al desactivarla).")
    origin = fields.Selection([
        ('entregable', "Calculada del entregable"),
        ('anterior', "Capturada a mano (versión anterior)"),
    ], compute='_compute_origin', string="Origen")

    # Lo único que se puede tocar a mano de una conexión calculada.
    _SGI_MANUAL_OK = {'active', 'inactive_reason'}

    @api.depends('deliverable_id')
    def _compute_origin(self):
        for rec in self:
            rec.origin = 'entregable' if rec.deliverable_id else 'anterior'

    @api.constrains('active', 'inactive_reason', 'deliverable_id')
    def _check_inactive_reason(self):
        if self.env.context.get('sgi_connection_sync'):
            return
        for rec in self:
            if rec.deliverable_id and not rec.active and not (rec.inactive_reason or '').strip():
                raise ValidationError(
                    "«%s» es una conexión calculada: para desactivarla escribe el "
                    "motivo." % rec.display_name)

    def _sgi_guard_manual_write(self, vals):
        if self.env.context.get('sgi_connection_sync'):
            return
        allowed = self._SGI_MANUAL_OK | getattr(self, '_SGI_MEASURE_FIELDS', set())
        blocked = set(vals) - allowed
        if blocked and self.filtered('deliverable_id'):
            raise UserError(
                "Las conexiones calculadas no se editan a mano: sale de lo que "
                "entrega y recibe cada actividad. Cambia el entregable o las "
                "actividades; si una no aplica, desactívala con su motivo.")

    def _sgi_retire_manual(self, deliverable):
        """Archiva la conexión capturada a mano que este entregable reemplaza."""
        manual = self._sgi_manual_twins()
        if manual:
            manual.with_context(sgi_connection_sync=True).write({
                'active': False,
                'inactive_reason': "Reemplazada por el entregable «%s»." % deliverable.name,
            })


class SgiProcessFlowDeliverable(models.Model):
    _name = 'sgi.process.flow'
    _inherit = ['sgi.process.flow', 'sgi.calculated.connection.mixin']

    def _sgi_manual_twins(self):
        Flow = self.env['sgi.process.flow'].with_context(active_test=False)
        twins = Flow.browse()
        for flow in self:
            twins |= Flow.search([('deliverable_id', '=', False), ('active', '=', True),
                                  ('from_process_id', '=', flow.from_process_id.id),
                                  ('to_process_id', '=', flow.to_process_id.id)])
        return twins

    def write(self, vals):
        self._sgi_guard_manual_write(vals)
        return super().write(vals)

    def unlink(self):
        if not self.env.context.get('sgi_connection_sync') and self.filtered('deliverable_id'):
            raise UserError("Un flujo calculado se quita quitando el entregable de las "
                            "actividades, no a mano.")
        return super().unlink()


class SgiActivityLinkDeliverable(models.Model):
    _name = 'sgi.activity.link'
    _inherit = ['sgi.activity.link', 'sgi.calculated.connection.mixin']

    input_id = fields.Many2one(
        'sgi.activity.input', string="Renglón «recibe»", index=True, ondelete='cascade',
        readonly=True)
    max_days = fields.Integer(
        related='input_id.max_days', string="Plazo (días hábiles)")

    def _sgi_manual_twins(self):
        Link = self.env['sgi.activity.link'].with_context(active_test=False)
        twins = Link.browse()
        for link in self:
            twins |= Link.search([('deliverable_id', '=', False), ('active', '=', True),
                                  ('from_activity_id', '=', link.from_activity_id.id),
                                  ('to_activity_id', '=', link.to_activity_id.id)])
        return twins

    def write(self, vals):
        self._sgi_guard_manual_write(vals)
        res = super().write(vals)
        if 'active' in vals and not self.env.context.get('sgi_connection_sync'):
            # El flujo entre procesos se apaga cuando todas sus ligas lo están.
            self.deliverable_id._sgi_sync_connections()
        return res

    def unlink(self):
        if not self.env.context.get('sgi_connection_sync') and self.filtered('deliverable_id'):
            raise UserError("Una liga calculada no se borra: desactívala con su motivo "
                            "o quita el entregable de las actividades.")
        return super().unlink()

    def _sgi_chain_verdict(self, now):
        """Con plazo: atorado si el origen entregó y el destino no ha
        ejecutado después en más de ``max_days`` días hábiles. Devuelve
        (estado, rezago) o None si la liga no tiene plazo (regla anterior)."""
        self.ensure_one()
        if not self.max_days:
            return None
        frm, to = self.from_activity_id, self.to_activity_id
        delivered = frm.measure_last_date
        if not delivered:
            return (False, 0.0)
        taken = to.measure_last_date
        if taken and taken >= delivered:
            return ('fluye', 0.0)
        waited = sgi_business_days(self.env, delivered, now, self.company_id)
        return ('atorado' if waited > self.max_days else 'fluye', float(waited))


class SgiActivityDeliverables(models.Model):
    _inherit = 'sgi.process.activity'

    input_ids = fields.One2many(
        'sgi.activity.input', 'activity_id', string="Recibe",
        help="Qué entregables recibe y en cuántos días hábiles deben llegarle.")
    input_deliverable_ids = fields.Many2many(
        'sgi.deliverable', compute='_compute_input_deliverables',
        inverse='_inverse_input_deliverables', string="Entregables que recibe")
    output_deliverable_ids = fields.Many2many(
        'sgi.deliverable', 'sgi_activity_output_rel', 'activity_id', 'deliverable_id',
        string="Entrega")
    measure_deliverable_id = fields.Many2one(
        'sgi.deliverable', string="Se mide con el entregable", ondelete='restrict', index=True,
        help="Con el método «Por su entregable», la actividad copia el modelo, "
             "el filtro, la fecha y el usuario de este entregable.")

    @api.depends('input_ids.deliverable_id')
    def _compute_input_deliverables(self):
        for act in self:
            act.input_deliverable_ids = act.input_ids.deliverable_id

    def _inverse_input_deliverables(self):
        """Escribir la lista de entregables conserva el plazo de los que ya
        estaban."""
        for act in self:
            current = act.input_ids
            wanted = act.input_deliverable_ids
            (current.filtered(lambda l: l.deliverable_id not in wanted)).unlink()
            new = wanted - current.deliverable_id
            if new:
                self.env['sgi.activity.input'].create(
                    [{'activity_id': act.id, 'deliverable_id': d.id} for d in new])

    @api.constrains('output_deliverable_ids')
    def _check_io(self):
        for act in self:
            both = act.input_ids.deliverable_id & act.output_deliverable_ids
            if both:
                raise ValidationError(
                    "La actividad %s no puede recibir y entregar el mismo entregable «%s»." % (
                        act.display_name, both[0].name))

    @api.constrains('measure_deliverable_id', 'output_deliverable_ids', 'measure_method')
    def _check_measure_deliverable(self):
        for act in self.filtered('measure_deliverable_id'):
            if act.measure_deliverable_id not in act.output_deliverable_ids:
                raise ValidationError(
                    "La actividad %s se mide con «%s», pero no lo entrega." % (
                        act.display_name, act.measure_deliverable_id.name))

    _SGI_IO_FIELDS = {'output_deliverable_ids', 'active', 'process_id'}
    _SGI_DELIVERABLE_MEASURE_TRIGGER = {'measure_method', 'measure_deliverable_id',
                                        'output_deliverable_ids'}

    def _sgi_apply_deliverable_measure(self):
        """«Por su entregable»: elige el entregable si solo hay uno con modelo
        y copia su modelo, filtro, fecha y usuario a la medición."""
        for act in self.filtered(lambda a: a.measure_method == 'entregable'):
            vals = {}
            deliverable = act.measure_deliverable_id
            if not deliverable:
                candidates = act.output_deliverable_ids.filtered('odoo_model_id')
                if len(candidates) == 1:
                    deliverable = candidates
                    vals['measure_deliverable_id'] = deliverable.id
            if deliverable:
                vals.update(deliverable._sgi_measure_vals())
            changed = {k: v for k, v in vals.items()
                       if (act[k].id if isinstance(act[k], models.BaseModel) else act[k]) != v}
            if changed:
                act.with_context(sgi_deliverable_measure=True).write(changed)

    @api.model_create_multi
    def create(self, vals_list):
        acts = super().create(vals_list)
        (acts.input_ids.deliverable_id | acts.output_deliverable_ids)._sgi_sync_connections()
        acts._sgi_apply_deliverable_measure()
        return acts

    def write(self, vals):
        before = self.input_ids.deliverable_id | self.output_deliverable_ids
        res = super().write(vals)
        if self._SGI_IO_FIELDS & set(vals):
            (before | self.input_ids.deliverable_id | self.output_deliverable_ids)._sgi_sync_connections()
        if self._SGI_DELIVERABLE_MEASURE_TRIGGER & set(vals) \
                and not self.env.context.get('sgi_deliverable_measure'):
            self._sgi_apply_deliverable_measure()
        return res

    def unlink(self):
        deliverables = self.input_ids.deliverable_id | self.output_deliverable_ids
        res = super().unlink()
        deliverables.exists()._sgi_sync_connections()
        return res

    def _sgi_sentence_parts(self):
        """Piezas de la frase del procedimiento: lista de (etiqueta, texto).
        La etiqueta («Ejecuta», «Aprueba», «Recibe»…) va en negritas en el PDF;
        sin etiqueta (None) el texto se imprime tal cual («Automática.»)."""
        self.ensure_one()
        roles = {}
        for role in self.role_ids:
            label = role._sgi_target_label()
            if role.condition:
                label = "%s (%s)" % (label, role.condition)
            if role.role == 'escala' and role.after_days:
                label = "%s a los %d días hábiles" % (label, role.after_days)
            roles.setdefault(role.role, []).append(label)
        parts = []
        if roles.get('ejecuta'):
            parts.append(("Ejecuta", ", ".join(roles['ejecuta'])))
        elif self.automation_level_current == 'automatico':
            parts.append((None, "Automática"))
        for key, label in (('aprueba', "Aprueba"), ('participa', "Participa"), ('informa', "Se entera"),
                           ('escala', "Si se atora, escala a")):
            if roles.get(key):
                parts.append((label, ", ".join(roles[key])))
        if self.input_ids:
            parts.append(("Recibe", ", ".join(
                "%s (%d días hábiles)" % (line.deliverable_id.name, line.max_days)
                if line.max_days else line.deliverable_id.name
                for line in self.input_ids)))
        if self.output_deliverable_ids:
            parts.append(("Entrega", ", ".join(self.output_deliverable_ids.mapped('name'))))
        if self.related_procedure_id:
            parts.append(("Conforme a", self.related_procedure_id.sgi_code or self.related_procedure_id.name))
        if self.instruction_id:
            parts.append(("Instructivo", self.instruction_id.sgi_code or self.instruction_id.name))
        return parts

    def _sgi_sentence(self):
        """La actividad como una frase del procedimiento, armada de sus piezas
        (en vez de un párrafo redactado a mano). Texto plano."""
        return " ".join(
            "%s: %s." % (label, text) if label else "%s." % text
            for label, text in self._sgi_sentence_parts())

    def _sgi_sentence_html(self):
        """La misma frase para el PDF: cada etiqueta en negritas."""
        return Markup(" ").join(
            Markup("<b>%s:</b> %s.") % (label, text) if label else Markup("%s.") % text
            for label, text in self._sgi_sentence_parts())

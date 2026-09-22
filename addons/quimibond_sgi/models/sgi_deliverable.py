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

from odoo import models, fields, api, Command
from odoo.exceptions import ValidationError

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
        help="Clave corta para la carga por API (PEDIDO-CONF, PROGRAMA-SEM…).")
    name = fields.Char(string="Entregable", required=True,
                       help="Qué pasa de un paso al otro: «Pedido confirmado».")
    document_id = fields.Many2one(
        'documents.document', string="Formato",
        domain=[('sgi_is_controlled', '=', True)],
        help="Formato controlado en el que viaja, si aplica.")
    odoo_model_id = fields.Many2one(
        'ir.model', string="Modelo de Odoo", ondelete='set null',
        help="Dónde vive el entregable en Odoo (sale.order, mrp.production…).")
    acceptance_criteria = fields.Text(string="Criterio de aceptación")
    active = fields.Boolean(default=True)
    company_id = fields.Many2one(
        'res.company', string="Empresa", required=True, index=True,
        default=lambda self: self.env.company)

    producer_activity_ids = fields.Many2many(
        'sgi.process.activity', 'sgi_activity_output_rel', 'deliverable_id', 'activity_id',
        string="Lo entregan")
    consumer_activity_ids = fields.Many2many(
        'sgi.process.activity', 'sgi_activity_input_rel', 'deliverable_id', 'activity_id',
        string="Lo reciben")
    link_ids = fields.One2many('sgi.activity.link', 'deliverable_id', string="Ligas")
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

    @api.depends('producer_activity_ids.process_id', 'consumer_activity_ids.process_id')
    def _compute_processes(self):
        for deliverable in self:
            deliverable.producer_process_ids = deliverable.producer_activity_ids.process_id
            deliverable.consumer_process_ids = deliverable.consumer_activity_ids.process_id
            if deliverable.consumer_activity_ids and not deliverable.producer_activity_ids:
                deliverable.orphan = 'sin_origen'
            elif deliverable.producer_activity_ids and not deliverable.consumer_activity_ids:
                deliverable.orphan = 'sin_destino'
            else:
                deliverable.orphan = False

    @api.constrains('producer_activity_ids', 'consumer_activity_ids')
    def _check_not_self(self):
        for deliverable in self:
            both = deliverable.producer_activity_ids & deliverable.consumer_activity_ids
            if both:
                raise ValidationError(
                    "La actividad %s no puede entregar y recibir el mismo entregable «%s»." % (
                        both[0].display_name, deliverable.name))

    # ------------------------------------------------------------------
    # Conexiones calculadas
    # ------------------------------------------------------------------
    def _sgi_sync_connections(self):
        """Ligas entre actividades y flujos entre procesos que se derivan de
        quién entrega y quién recibe cada entregable. Solo toca las ligas y
        flujos que nacieron de un entregable (``deliverable_id``); las ligas
        capturadas a mano se quedan."""
        Link = self.env['sgi.activity.link'].sudo()
        Flow = self.env['sgi.process.flow'].sudo()
        for deliverable in self.with_context(active_test=False):
            producers = deliverable.producer_activity_ids.filtered('active')
            consumers = deliverable.consumer_activity_ids.filtered('active')
            if not deliverable.active:
                producers = consumers = producers.browse()
            wanted = {(p.id, c.id) for p in producers for c in consumers if p != c}
            existing = {(link.from_activity_id.id, link.to_activity_id.id): link
                        for link in Link.search([('deliverable_id', '=', deliverable.id)])}
            stale = Link.browse([link.id for key, link in existing.items() if key not in wanted])
            if stale:
                stale.unlink()
            missing = wanted - set(existing)
            if missing:
                Link.create([{'from_activity_id': frm, 'to_activity_id': to,
                              'deliverable_id': deliverable.id, 'name': deliverable.name}
                             for frm, to in sorted(missing)])
            wanted_flows = {(p.process_id.id, c.process_id.id) for p in producers for c in consumers
                            if p.process_id != c.process_id}
            flows = {(flow.from_process_id.id, flow.to_process_id.id): flow
                     for flow in Flow.search([('deliverable_id', '=', deliverable.id)])}
            stale_flows = Flow.browse([flow.id for key, flow in flows.items() if key not in wanted_flows])
            if stale_flows:
                stale_flows.unlink()
            for frm, to in sorted(wanted_flows - set(flows)):
                Flow.create({
                    'from_process_id': frm, 'to_process_id': to,
                    'deliverable_id': deliverable.id, 'name': deliverable.name,
                    'document_id': deliverable.document_id.id,
                    'odoo_model_id': deliverable.odoo_model_id.id,
                    'acceptance_criteria': deliverable.acceptance_criteria,
                })

    def write(self, vals):
        res = super().write(vals)
        if {'producer_activity_ids', 'consumer_activity_ids', 'active'} & set(vals):
            self._sgi_sync_connections()
        # El nombre, el formato y el modelo viajan a las ligas y flujos que
        # salieron de este entregable.
        mirror = {k: vals[k] for k in ('name', 'document_id', 'odoo_model_id', 'acceptance_criteria')
                  if k in vals}
        if mirror:
            flows = self.env['sgi.process.flow'].sudo().search([('deliverable_id', 'in', self.ids)])
            if flows:
                flows.write(mirror)
            if 'name' in mirror:
                links = self.env['sgi.activity.link'].sudo().search([('deliverable_id', 'in', self.ids)])
                if links:
                    links.write({'name': mirror['name']})
        return res

    @api.model_create_multi
    def create(self, vals_list):
        deliverables = super().create(vals_list)
        deliverables._sgi_sync_connections()
        return deliverables

    def unlink(self):
        links = self.env['sgi.activity.link'].sudo().search([('deliverable_id', 'in', self.ids)])
        flows = self.env['sgi.process.flow'].sudo().search([('deliverable_id', 'in', self.ids)])
        links.unlink()
        flows.unlink()
        return super().unlink()

    def action_view_links(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Ligas — %s" % self.name,
            'res_model': 'sgi.activity.link',
            'view_mode': 'list',
            'domain': [('deliverable_id', '=', self.id)],
        }


class SgiProcessDeliverables(models.Model):
    """Entradas y salidas del proceso, calculadas de sus entregables (antes,
    dos campos de texto que repetían los flujos)."""
    _inherit = 'sgi.process'

    stage_ids = fields.One2many('sgi.process.stage', 'process_id', string="Etapas")
    input_deliverable_ids = fields.Many2many(
        'sgi.deliverable', compute='_compute_io_deliverables', string="Entradas",
        help="Lo que sus actividades reciben y ninguna actividad del proceso entrega.")
    output_deliverable_ids = fields.Many2many(
        'sgi.deliverable', compute='_compute_io_deliverables', string="Salidas",
        help="Lo que sus actividades entregan y ninguna actividad del proceso recibe "
             "(lo que sale hacia otros procesos o hacia el cliente).")

    @api.depends('procedure_activity_ids.input_deliverable_ids',
                 'procedure_activity_ids.output_deliverable_ids')
    def _compute_io_deliverables(self):
        for process in self:
            acts = process.procedure_activity_ids
            consumed = acts.input_deliverable_ids
            produced = acts.output_deliverable_ids
            process.input_deliverable_ids = consumed - produced
            process.output_deliverable_ids = produced - consumed

    def action_sgi_extract_references(self):
        """Botón del proceso: liga las claves de documentos escritas en el
        texto de sus actividades."""
        self.ensure_one()
        return self.procedure_activity_ids.action_sgi_extract_references()

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
        order = ['ejecuta', 'aprueba', 'participa', 'informa']
        return [
            (target, [(labels[r], roles[r]) for r in order if r in roles])
            for target, roles in sorted(by_target.items())
        ]


class SgiProcessFlowDeliverable(models.Model):
    _inherit = 'sgi.process.flow'

    deliverable_id = fields.Many2one(
        'sgi.deliverable', string="Entregable (catálogo)", index=True, ondelete='cascade',
        help="Si viene de un entregable, el flujo se calcula solo de quién lo entrega "
             "y quién lo recibe; no se edita a mano.")


class SgiActivityLinkDeliverable(models.Model):
    _inherit = 'sgi.activity.link'

    deliverable_id = fields.Many2one(
        'sgi.deliverable', string="Entregable (catálogo)", index=True, ondelete='cascade',
        help="Si viene de un entregable, la liga se calcula sola.")


class SgiActivityDeliverables(models.Model):
    _inherit = 'sgi.process.activity'

    input_deliverable_ids = fields.Many2many(
        'sgi.deliverable', 'sgi_activity_input_rel', 'activity_id', 'deliverable_id',
        string="Recibe")
    output_deliverable_ids = fields.Many2many(
        'sgi.deliverable', 'sgi_activity_output_rel', 'activity_id', 'deliverable_id',
        string="Entrega")

    @api.constrains('input_deliverable_ids', 'output_deliverable_ids')
    def _check_io(self):
        for act in self:
            both = act.input_deliverable_ids & act.output_deliverable_ids
            if both:
                raise ValidationError(
                    "La actividad %s no puede recibir y entregar el mismo entregable «%s»." % (
                        act.display_name, both[0].name))

    _SGI_IO_FIELDS = {'input_deliverable_ids', 'output_deliverable_ids', 'active', 'process_id'}

    @api.model_create_multi
    def create(self, vals_list):
        acts = super().create(vals_list)
        (acts.input_deliverable_ids | acts.output_deliverable_ids)._sgi_sync_connections()
        return acts

    def write(self, vals):
        before = self.input_deliverable_ids | self.output_deliverable_ids
        res = super().write(vals)
        if self._SGI_IO_FIELDS & set(vals):
            (before | self.input_deliverable_ids | self.output_deliverable_ids)._sgi_sync_connections()
        return res

    def unlink(self):
        deliverables = self.input_deliverable_ids | self.output_deliverable_ids
        res = super().unlink()
        deliverables.exists()._sgi_sync_connections()
        return res

    def _sgi_sentence(self):
        """La actividad como una frase del procedimiento, armada de sus piezas
        (en vez de un párrafo redactado a mano)."""
        self.ensure_one()
        roles = {}
        for role in self.role_ids:
            label = role._sgi_target_label()
            if role.condition:
                label = "%s (%s)" % (label, role.condition)
            roles.setdefault(role.role, []).append(label)
        parts = []
        if roles.get('ejecuta'):
            parts.append("Ejecuta: %s." % ", ".join(roles['ejecuta']))
        elif self.automation_level_current == 'automatico':
            parts.append("Automática.")
        for key, label in (('aprueba', "Aprueba"), ('participa', "Participa"), ('informa', "Se entera")):
            if roles.get(key):
                parts.append("%s: %s." % (label, ", ".join(roles[key])))
        if self.input_deliverable_ids:
            parts.append("Recibe: %s." % ", ".join(self.input_deliverable_ids.mapped('name')))
        if self.output_deliverable_ids:
            parts.append("Entrega: %s." % ", ".join(self.output_deliverable_ids.mapped('name')))
        if self.related_procedure_id:
            parts.append("Conforme a %s." % (self.related_procedure_id.sgi_code or self.related_procedure_id.name))
        if self.instruction_id:
            parts.append("Instructivo %s." % (self.instruction_id.sgi_code or self.instruction_id.name))
        return " ".join(parts)


# Claves de documento dentro del texto: P-A22, F-P-A28-21, IT-P-A28-01,
# F-IT-P-P01-08-01 y la nomenclatura nueva (PR-C6, IT-C6-06, F-C6-01, CO-01…).
RE_DOC_CODE = re.compile(
    r'\b(F-IT-P-[A-Z]\d{2}-\d{2}-\d{2}|IT-P-[A-Z]\d{2}-\d{2}|F-P-[A-Z]\d{2}-\d{2}|P-[A-Z]\d{2}'
    r'|(?:PR|IT|F|MA)-[A-Z]\d(?:-\d{2})?|CO-\d{2}|DP-\d{2})\b')


class SgiActivityExtract(models.Model):
    _inherit = 'sgi.process.activity'

    def _sgi_extract_references(self):
        """Lee la descripción y el nombre, encuentra las claves de documentos
        y los puestos mencionados. Devuelve {actividad: {formats, procedure,
        instruction, jobs, missing}} sin escribir nada."""
        Doc = self.env['documents.document']
        jobs = self.env['hr.job'].search([('company_id', 'in', [self.env.company.id, False])])
        from .sgi_catalog import sgi_normalize_name
        job_names = sorted(((sgi_normalize_name(j.name), j) for j in jobs if j.name),
                           key=lambda x: -len(x[0]))
        out = {}
        for act in self:
            text = ' '.join(filter(None, [act.name, act.description, act.note]))
            found = {'formats': Doc.browse(), 'procedure': Doc.browse(),
                     'instruction': Doc.browse(), 'jobs': self.env['hr.job'], 'missing': []}
            for code in dict.fromkeys(RE_DOC_CODE.findall(text or '')):
                doc = Doc._sgi_find_by_code(code, states=None)
                if not doc:
                    found['missing'].append(code)
                elif doc.sgi_doc_type == 'procedimiento':
                    found['procedure'] = found['procedure'] or doc
                elif doc.sgi_doc_type == 'instructivo':
                    found['instruction'] = found['instruction'] or doc
                else:
                    found['formats'] |= doc
            norm = sgi_normalize_name(text)
            for name, job in job_names:
                if name and len(name) > 6 and name in norm:
                    found['jobs'] |= job
                    norm = norm.replace(name, ' ')
            out[act] = found
        return out

    def action_sgi_extract_references(self):
        """Convierte las claves que están escritas en el texto en ligas reales
        (formatos, procedimiento, instructivo). Solo agrega; nunca borra. Los
        puestos mencionados se reportan para capturarlos como roles."""
        lines = []
        for act, found in self._sgi_extract_references().items():
            vals = {}
            new_formats = found['formats'] - act.format_document_ids
            if new_formats:
                vals['format_document_ids'] = [Command.link(d.id) for d in new_formats]
            if found['procedure'] and not act.related_procedure_id:
                vals['related_procedure_id'] = found['procedure'].id
            if found['instruction'] and not act.instruction_id:
                vals['instruction_id'] = found['instruction'].id
            if vals:
                act.write(vals)
            bits = []
            if new_formats:
                bits.append("formatos %s" % ", ".join(new_formats.mapped('sgi_code')))
            if 'related_procedure_id' in vals:
                bits.append("procedimiento %s" % found['procedure'].sgi_code)
            if 'instruction_id' in vals:
                bits.append("instructivo %s" % found['instruction'].sgi_code)
            jobs_missing = found['jobs'] - act.role_ids._sgi_jobs_or_empty()
            if jobs_missing:
                bits.append("puestos sin rol: %s" % ", ".join(
                    ' '.join(j.name.split()) for j in jobs_missing))
            if found['missing']:
                bits.append("claves sin documento: %s" % ", ".join(found['missing']))
            if bits:
                lines.append("%s → %s" % (act.display_name, "; ".join(bits)))
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': "Referencias del texto",
                'message': "\n".join(lines[:60]) or "No había claves ni puestos por ligar.",
                'sticky': bool(lines),
                'type': 'success' if lines else 'info',
            },
        }


class SgiActivityRoleJobs(models.Model):
    _inherit = 'sgi.activity.role'

    def _sgi_jobs_or_empty(self):
        """Puestos de los roles que sí se resuelven a un puesto (los relativos
        como «solicitante» se ignoran en vez de anular todo)."""
        jobs = self.env['hr.job']
        for role in self:
            jobs |= role._sgi_jobs() or self.env['hr.job']
        return jobs

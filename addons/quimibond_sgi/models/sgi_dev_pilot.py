# -*- coding: utf-8 -*-
"""Pilotaje y estudio de habilidad (57.133.0; brief §6.12 y §8; Jose 2026-10-08, 5.4).

* **Pilotaje** (``sgi.dev.pilot``): los primeros lotes de producción del
  artículo (no las órdenes de muestra) se marcan solos como pilotaje cuando el
  artículo está «En pilotaje»: al terminar la orden de fabricación entra su
  lote hasta completar los lotes del parámetro (brief: tres). **Un lote que
  no cumple no cuenta**: libera su lugar y el siguiente lote entra.
* **Lecturas por lote** (``sgi.dev.pilot.reading``): el laboratorio captura
  N lecturas por lote de cada característica **crítica** de la tabla del
  proyecto. Cuántas lecturas y cuáles características son parámetros que el
  brief deja sin definir (sección 8, Ingeniería de Calidad): el número de
  lecturas por lote queda vacío (no se exige) y la marca «Crítica» de los
  renglones nace apagada en el catálogo y en los proyectos.
* **Estudio de habilidad** (``sgi.dev.pilot.study``): Cp y Cpk por
  característica crítica con las lecturas de los lotes conformes, **contra la
  especificación del cliente** (brief §5.1), sigma muestral (n − 1). El Cpk
  mínimo también es parámetro vacío: sin valor el estudio informa y no
  dictamina.
"""
import base64
import math

from odoo import api, fields, models
from odoo.exceptions import UserError

PARAM_LOTS = 'quimibond_sgi.dev_pilot_lots'
PARAM_READINGS = 'quimibond_sgi.dev_pilot_readings_per_lot'
PARAM_CPK_MIN = 'quimibond_sgi.dev_pilot_cpk_min'
DEFAULT_LOTS = 3  # brief §6.12: los tres primeros lotes
LOT_VERDICTS = [('pendiente', "Pendiente"), ('conforme', "Conforme"), ('no_conforme', "No conforme")]


def _int_param(env, key, default=0):
    raw = (env['ir.config_parameter'].sudo().get_param(key, '') or '').strip()
    return int(raw) if raw.isdigit() else default


def _float_param(env, key, default=0.0):
    raw = (env['ir.config_parameter'].sudo().get_param(key, '') or '').strip()
    try:
        return float(raw) if raw else default
    except ValueError:
        return default


def capability(values, lo, hi):
    """Cp y Cpk de una serie contra límites (None donde no aplica). Sigma muestral.
    Devuelve {'n', 'mean', 'sigma', 'cp', 'cpk'}; cp/cpk None si no se pueden calcular."""
    n = len(values)
    out = {'n': n, 'mean': 0.0, 'sigma': 0.0, 'cp': None, 'cpk': None}
    if n < 2:
        if n == 1:
            out['mean'] = values[0]
        return out
    mean = sum(values) / n
    sigma = math.sqrt(sum((v - mean) ** 2 for v in values) / (n - 1))
    out.update({'mean': mean, 'sigma': sigma})
    if sigma <= 0:
        return out
    if lo is not None and hi is not None:
        out['cp'] = (hi - lo) / (6 * sigma)
    sides = []
    if hi is not None:
        sides.append((hi - mean) / (3 * sigma))
    if lo is not None:
        sides.append((mean - lo) / (3 * sigma))
    if sides:
        out['cpk'] = min(sides)
    return out


class SgiDevCharacteristicTemplateCritical(models.Model):
    _inherit = 'sgi.dev.characteristic.template'

    critical = fields.Boolean(string="Crítica (estudio de habilidad)", default=False,
                              help="Entra al estudio de habilidad del pilotaje. El brief lo deja por definir "
                                   "(Ingeniería de Calidad): nace apagada.")

    def _line_vals(self, sequence=None):
        return dict(super()._line_vals(sequence=sequence), critical=self.critical)


class SgiDevCharacteristicCritical(models.Model):
    _inherit = 'sgi.dev.characteristic'

    critical = fields.Boolean(string="Crítica (estudio de habilidad)", default=False,
                              help="Entra al estudio de habilidad del pilotaje con las lecturas por lote.")


class SgiDevPilot(models.Model):
    _name = 'sgi.dev.pilot'
    _description = "Pilotaje del desarrollo (primeros lotes y estudio de habilidad)"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'id desc'

    name = fields.Char(string="Pilotaje", compute='_compute_name', store=True)
    project_id = fields.Many2one('project.project', string="Desarrollo", required=True, ondelete='cascade', index=True,
                                 domain="[('sgi_is_ft', '=', True), ('is_template', '=', False)]")
    partner_id = fields.Many2one(related='project_id.partner_id', string="Cliente")
    product_id = fields.Many2one('product.product', string="Artículo", required=True, index=True)
    company_id = fields.Many2one('res.company', default=lambda self: self.env.company, required=True)
    state = fields.Selection([('abierto', "Abierto"), ('cerrado', "Cerrado")], string="Estado", default='abierto',
                             required=True, tracking=True, copy=False)
    lots_required = fields.Integer(string="Lotes del pilotaje", default=lambda self: _int_param(self.env, PARAM_LOTS, DEFAULT_LOTS),
                                   help="Cuántos lotes conformes cierran el pilotaje (brief: tres).")
    readings_required = fields.Integer(string="Lecturas por lote", default=lambda self: _int_param(self.env, PARAM_READINGS, 0),
                                       help="Lecturas por característica y lote. 0: no se exige (el brief no lo define).")
    lot_ids = fields.One2many('sgi.dev.pilot.lot', 'pilot_id', string="Lotes")
    reading_ids = fields.One2many('sgi.dev.pilot.reading', 'pilot_id', string="Lecturas")
    study_ids = fields.One2many('sgi.dev.pilot.study', 'pilot_id', string="Estudio de habilidad")
    lot_count = fields.Integer(compute='_compute_counts')
    conforming_count = fields.Integer(string="Lotes conformes", compute='_compute_counts')
    critical_count = fields.Integer(string="Características críticas", compute='_compute_counts')
    cpk_min = fields.Float(string="Cpk mínimo", compute='_compute_counts', digits=(16, 2),
                           help="Del parámetro; 0: el estudio informa y no dictamina.")
    closed_by_id = fields.Many2one('res.users', string="Cerró", readonly=True, copy=False)
    closed_at = fields.Datetime(string="Cerrado el", readonly=True, copy=False)
    attachment_id = fields.Many2one('ir.attachment', string="PDF del estudio", readonly=True, copy=False)

    @api.depends('product_id.default_code', 'project_id.sgi_ft_folio', 'project_id.name')
    def _compute_name(self):
        for pilot in self:
            pilot.name = "Pilotaje %s" % (pilot.product_id.default_code or pilot.project_id.sgi_ft_folio
                                          or pilot.project_id.name or '')

    @api.depends('lot_ids.verdict', 'project_id.sgi_dev_line_ids.critical')
    def _compute_counts(self):
        cpk_min = _float_param(self.env, PARAM_CPK_MIN, 0.0)
        for pilot in self:
            pilot.lot_count = len(pilot.lot_ids)
            pilot.conforming_count = len(pilot.lot_ids.filtered(lambda l: l.verdict == 'conforme'))
            pilot.critical_count = len(pilot._critical_lines())
            pilot.cpk_min = cpk_min

    def _critical_lines(self):
        self.ensure_one()
        return self.project_id.sgi_dev_line_ids.filtered(lambda l: l.critical and l.kind == 'num')

    # ------------------------------------------------------------------------
    # Primeros lotes de producción
    # ------------------------------------------------------------------------
    @api.model
    def _sgi_dev_register_production(self, mo):
        """Al terminar una orden de producción de un artículo «En pilotaje» (no una orden de muestra), su
        lote entra al pilotaje abierto del desarrollo mientras falten lotes; los no conformes no cuentan."""
        tmpl = mo.product_id.product_tmpl_id
        project = tmpl.sgi_dev_project_id
        if not project or tmpl.sgi_dev_state != 'pilotaje' or mo.sgi_dev_project_id:
            return self.browse()
        pilot = self.search([('project_id', '=', project.id), ('product_id', '=', mo.product_id.id),
                             ('state', '=', 'abierto')], limit=1)
        if not pilot:
            pilot = self.create({'project_id': project.id, 'product_id': mo.product_id.id})
        if mo in pilot.lot_ids.mapped('production_id'):
            return pilot
        counted = pilot.lot_ids.filtered(lambda l: l.verdict != 'no_conforme')
        if len(counted) >= pilot.lots_required:
            return pilot
        # 57.138.0: en Odoo 19 la orden lleva `lot_producing_ids` (varios lotes); el pilotaje toma el
        # primero y nombra todos. `lot_producing_id` ya no existe (tumbó el build de producción).
        lots = mo.lot_producing_ids
        lot_names = ", ".join(lots.mapped('name')) or '—'
        self.env['sgi.dev.pilot.lot'].create({
            'pilot_id': pilot.id, 'production_id': mo.id, 'lot_id': lots[:1].id,
            'sequence': (len(pilot.lot_ids) + 1) * 10, 'date': mo.date_finished or fields.Datetime.now(),
            'qty_produced': mo.qty_produced or mo.product_qty})
        pilot.message_post(body="Lote %s de la orden %s entra al pilotaje (%d de %d)." % (
            lot_names, mo.name, len(counted) + 1, pilot.lots_required))
        project.message_post(body="Pilotaje: lote %s (%s) es el %d de %d." % (
            lot_names, mo.name, len(counted) + 1, pilot.lots_required))
        return pilot

    # ------------------------------------------------------------------------
    # Estudio de habilidad
    # ------------------------------------------------------------------------
    def action_compute_study(self):
        Study = self.env['sgi.dev.pilot.study']
        for pilot in self:
            pilot.study_ids.unlink()
            lots = pilot.lot_ids.filtered(lambda l: l.verdict == 'conforme')
            cpk_min = pilot.cpk_min
            for line in pilot._critical_lines():
                readings = pilot.reading_ids.filtered(lambda r: r.characteristic_id == line and r.lot_id in lots)
                values = readings.mapped('value')
                lo, hi = line._limits('spec') if line._has_spec() else (None, None)
                stats = capability(values, lo, hi)
                verdict = False
                if cpk_min and stats['cpk'] is not None:
                    verdict = 'cumple' if stats['cpk'] >= cpk_min else 'no_cumple'
                Study.create({
                    'pilot_id': pilot.id, 'characteristic_id': line.id, 'n': stats['n'], 'lots': len(lots),
                    'mean': stats['mean'], 'sigma': stats['sigma'],
                    'cp': stats['cp'] if stats['cp'] is not None else 0.0, 'cp_defined': stats['cp'] is not None,
                    'cpk': stats['cpk'] if stats['cpk'] is not None else 0.0, 'cpk_defined': stats['cpk'] is not None,
                    'verdict': verdict,
                })
        return True

    def _check_close(self):
        for pilot in self:
            conforme = pilot.lot_ids.filtered(lambda l: l.verdict == 'conforme')
            if len(conforme) < pilot.lots_required:
                raise UserError("El pilotaje necesita %d lote(s) conforme(s); hay %d." % (pilot.lots_required, len(conforme)))
            if pilot.lot_ids.filtered(lambda l: l.verdict == 'pendiente'):
                raise UserError("Hay lotes sin dictamen: evalúelos o márquelos no conformes.")
            if pilot.readings_required:
                for lot in conforme:
                    for line in pilot._critical_lines():
                        n = len(pilot.reading_ids.filtered(lambda r: r.lot_id == lot and r.characteristic_id == line))
                        if n < pilot.readings_required:
                            raise UserError("El lote %s tiene %d lectura(s) de «%s»; se piden %d." % (
                                lot.display_name, n, line.name, pilot.readings_required))
        return True

    def action_close(self):
        for pilot in self:
            if pilot.state != 'abierto':
                raise UserError("El pilotaje ya está cerrado.")
            pilot._check_close()
            pilot.action_compute_study()
            pilot.write({'state': 'cerrado', 'closed_by_id': self.env.uid, 'closed_at': fields.Datetime.now()})
            report = self.env.ref('quimibond_sgi.action_report_dev_pilot')
            pdf, _kind = self.env['ir.actions.report'].sudo()._render_qweb_pdf(report.report_name, res_ids=pilot.ids)
            att = self.env['ir.attachment'].create({
                'name': "Estudio de habilidad %s.pdf" % (pilot.product_id.default_code or pilot.project_id.name),
                'datas': base64.b64encode(pdf), 'mimetype': 'application/pdf',
                'res_model': pilot._name, 'res_id': pilot.id})
            pilot.write({'attachment_id': att.id})
            bad = pilot.study_ids.filtered(lambda s: s.verdict == 'no_cumple')
            pilot.message_post(body="Pilotaje cerrado por %s con %d lotes conformes; estudio de habilidad: %d "
                                    "característica(s)%s." % (self.env.user.name, pilot.conforming_count,
                                                               len(pilot.study_ids),
                                                               (", %d debajo del Cpk mínimo" % len(bad)) if bad else ''),
                               attachment_ids=att.ids)
            pilot.project_id.message_post(body="Pilotaje de %s cerrado: verificación hecha (estudio de habilidad adjunto "
                                               "al pilotaje)." % pilot.product_id.display_name)
        return True

    def action_print(self):
        self.ensure_one()
        return self.env.ref('quimibond_sgi.action_report_dev_pilot').report_action(self)


class SgiDevPilotLot(models.Model):
    _name = 'sgi.dev.pilot.lot'
    _description = "Lote del pilotaje"
    _order = 'pilot_id, sequence, id'

    pilot_id = fields.Many2one('sgi.dev.pilot', string="Pilotaje", required=True, ondelete='cascade', index=True)
    sequence = fields.Integer(default=10)
    production_id = fields.Many2one('mrp.production', string="Orden de producción", ondelete='set null')
    lot_id = fields.Many2one('stock.lot', string="Lote", ondelete='set null')
    date = fields.Datetime(string="Terminado el")
    qty_produced = fields.Float(string="Cantidad", digits=(16, 2))
    verdict = fields.Selection(LOT_VERDICTS, string="Dictamen", default='pendiente', required=True,
                               help="Conforme: todas las características críticas con lecturas dentro de la "
                                    "especificación del cliente. No conforme: no cuenta para el pilotaje.")
    reading_count = fields.Integer(compute='_compute_reading_count')

    @api.depends('lot_id.name', 'production_id.name')
    def _compute_display_name(self):
        for lot in self:
            lot.display_name = lot.lot_id.name or lot.production_id.name or ("Lote %d" % lot.id)

    def _compute_reading_count(self):
        for lot in self:
            lot.reading_count = len(lot.pilot_id.reading_ids.filtered(lambda r: r.lot_id == lot))

    def action_evaluate(self):
        """Dictamen del lote con sus lecturas: la media de cada característica crítica dentro de la
        especificación del cliente. Sin lecturas sigue pendiente."""
        for lot in self:
            lines = lot.pilot_id._critical_lines()
            readings = lot.pilot_id.reading_ids.filtered(lambda r: r.lot_id == lot)
            if not readings or not lines:
                continue
            ok = True
            for line in lines:
                values = readings.filtered(lambda r: r.characteristic_id == line).mapped('value')
                if not values or not line._has_spec():
                    continue
                mean = sum(values) / len(values)
                if not line._within(mean, line._limits('spec')):
                    ok = False
            lot.verdict = 'conforme' if ok else 'no_conforme'
        return True


class SgiDevPilotReading(models.Model):
    _name = 'sgi.dev.pilot.reading'
    _description = "Lectura de laboratorio en un lote de pilotaje"
    _order = 'pilot_id, lot_id, characteristic_id, sequence, id'

    pilot_id = fields.Many2one('sgi.dev.pilot', string="Pilotaje", required=True, ondelete='cascade', index=True)
    lot_id = fields.Many2one('sgi.dev.pilot.lot', string="Lote", required=True, ondelete='cascade',
                             domain="[('pilot_id', '=', pilot_id)]")
    characteristic_id = fields.Many2one('sgi.dev.characteristic', string="Característica crítica", required=True,
                                        ondelete='restrict',
                                        domain="[('project_id', '=', parent.project_id), ('critical', '=', True), ('kind', '=', 'num')]")
    unit = fields.Char(related='characteristic_id.unit')
    spec_label = fields.Char(related='characteristic_id.spec_label', string="Especificación del cliente")
    sequence = fields.Integer(string="Lectura", default=1)
    value = fields.Float(string="Valor", digits=(16, 3), required=True)
    within_spec = fields.Boolean(string="Dentro de la especificación", compute='_compute_within', store=True)

    @api.depends('value', 'characteristic_id.spec_nominal', 'characteristic_id.spec_limit',
                 'characteristic_id.spec_tol_minus', 'characteristic_id.spec_tol_plus', 'characteristic_id.spec_tol_pct')
    def _compute_within(self):
        for r in self:
            char = r.characteristic_id
            r.within_spec = bool(char and char._has_spec() and char._within(r.value, char._limits('spec')))


class SgiDevPilotStudy(models.Model):
    _name = 'sgi.dev.pilot.study'
    _description = "Estudio de habilidad por característica"
    _order = 'pilot_id, id'

    pilot_id = fields.Many2one('sgi.dev.pilot', string="Pilotaje", required=True, ondelete='cascade', index=True)
    characteristic_id = fields.Many2one('sgi.dev.characteristic', string="Característica", required=True, ondelete='restrict')
    unit = fields.Char(related='characteristic_id.unit')
    spec_label = fields.Char(related='characteristic_id.spec_label', string="Especificación del cliente")
    lots = fields.Integer(string="Lotes")
    n = fields.Integer(string="Lecturas")
    mean = fields.Float(string="Media", digits=(16, 3))
    sigma = fields.Float(string="Sigma (n−1)", digits=(16, 4))
    cp = fields.Float(string="Cp", digits=(16, 2))
    cp_defined = fields.Boolean()
    cpk = fields.Float(string="Cpk", digits=(16, 2))
    cpk_defined = fields.Boolean()
    verdict = fields.Selection([('cumple', "Cumple el Cpk mínimo"), ('no_cumple', "Debajo del Cpk mínimo")],
                               string="Dictamen", help="Vacío si el Cpk mínimo no está definido o no se pudo calcular.")


class ProjectProjectDevPilot(models.Model):
    _inherit = 'project.project'

    sgi_dev_pilot_ids = fields.One2many('sgi.dev.pilot', 'project_id', string="Pilotajes")
    sgi_dev_pilot_count = fields.Integer(compute='_compute_sgi_dev_pilot_count')

    @api.depends('sgi_dev_pilot_ids')
    def _compute_sgi_dev_pilot_count(self):
        for project in self:
            project.sgi_dev_pilot_count = len(project.sgi_dev_pilot_ids)

    def action_sgi_dev_pilots(self):
        self.ensure_one()
        if len(self.sgi_dev_pilot_ids) == 1:
            return {'type': 'ir.actions.act_window', 'res_model': 'sgi.dev.pilot', 'res_id': self.sgi_dev_pilot_ids.id,
                    'view_mode': 'form', 'target': 'current'}
        action = self.env['ir.actions.act_window']._for_xml_id('quimibond_sgi.sgi_dev_pilot_action')
        action['domain'] = [('project_id', '=', self.id)]
        action['context'] = {'default_project_id': self.id, 'default_product_id': self.sgi_dev_product_id.id}
        return action


class MrpProductionDevPilot(models.Model):
    _inherit = 'mrp.production'

    def write(self, vals):
        res = super().write(vals)
        if vals.get('state') == 'done':
            Pilot = self.env['sgi.dev.pilot'].sudo()
            for mo in self.filtered(lambda m: m.product_id.product_tmpl_id.sgi_dev_state == 'pilotaje'
                                    and not m.sgi_dev_project_id):
                Pilot._sgi_dev_register_production(mo)
        return res

# -*- coding: utf-8 -*-
import logging
import re
import time

from dateutil.relativedelta import relativedelta

from odoo import models, fields, api
from odoo.exceptions import ValidationError
from odoo.tools.safe_eval import safe_eval

from .sgi_calendar import sgi_today
from .sgi_guard import sgi_require_system
from .sgi_health_const import HEALTH_MODES

_logger = logging.getLogger(__name__)

# 57.104.0 (B3): meses hacia atrás que el cron diario vuelve a medir las «sin
# dato» y las capturadas no validadas (desde el día 1 del mes de hace N meses).
# No se siembra: se lee con este valor por omisión.
RECOMPUTE_MONTHS_PARAM = 'quimibond_sgi.indicator_recompute_months'
DEFAULT_RECOMPUTE_MONTHS = 2
# Tiempo tope (segundos) de la corrida del cron (``recent=True``); lo que no
# alcanzó sigue al día siguiente desde donde se quedó (cursor por id).
RECOMPUTE_BUDGET_SECONDS = 240
RECOMPUTE_CURSOR_PARAM = 'quimibond_sgi.indicator_recompute_cursor'


def _now():
    """Reloj del tiempo tope (aparte, para poder simularlo en las pruebas)."""
    return time.monotonic()


def _sgi_merge_note(computed, previous):
    """Nota nueva del cálculo más las líneas de la nota anterior que no trae:
    un recálculo nunca borra lo que una persona escribió."""
    lines = [line for line in (computed or '').splitlines() if line.strip()]
    for line in (previous or '').splitlines():
        if line.strip() and line not in lines:
            lines.append(line)
    return "\n".join(lines) or False


# 57.98.0 (I-01): reportes propios que imprimen el pie del formato controlado
# en cada hoja (layout quimibond_sgi.sgi_report_layout): (xmlid del reporte,
# referencia del mapeo o None = por el modelo del reporte). El Diagnóstico
# lista los que hoy imprimen sin clave (``_sgi_unmapped_reports``).
SGI_FORMAT_REPORTS = (
    ('quimibond_sgi.action_report_nc', None),
    ('quimibond_sgi.action_report_incident', None),
    ('quimibond_sgi.action_report_mgmt_review', None),
    ('quimibond_sgi.action_report_audit_plan', 'format_ref_audit_plan'),
    ('quimibond_sgi.action_report_audit_report', 'format_ref_audit_report'),
    ('quimibond_sgi.action_report_coa', None),
    ('quimibond_sgi.action_report_compliance_matrix', None),
    ('quimibond_sgi.action_report_risk_matrix', None),
    ('quimibond_sgi.action_report_competence_matrix', None),
    ('quimibond_sgi.action_report_env_aspect', None),
    ('quimibond_sgi.action_report_fmea', None),
    ('quimibond_sgi.action_report_loto', None),
    ('quimibond_sgi.action_report_master_list', 'format_ref_master_list'),
    ('quimibond_sgi.action_report_legal_matrix', None),
    ('quimibond_sgi.action_report_master_list_all', 'format_ref_master_list'),
    ('quimibond_sgi.action_report_news', 'format_ref_news'),
    ('quimibond_sgi.action_report_retention', None),
    ('quimibond_sgi.action_report_work_permit', None),
    ('quimibond_sgi.action_report_machine_sheet', 'format_ref_machine_sheet'),
    # 57.101.0: reportes y diagramas. Las referencias que no existen en
    # sgi_format_map_data.xml (format_ref_8d, _indicator_sheet, _diagrams,
    # _audit_program, _ryo_matrix) dejan el pie solo con la página y el
    # Diagnóstico los lista «(sin clave del SGI)» hasta que se decida su
    # formato (plan 57.101.0, Q1-Q3). La solicitud de desarrollo y las
    # eficiencias eligen la referencia por registro; aquí va la general.
    ('quimibond_sgi.action_report_8d', 'format_ref_8d'),
    ('quimibond_sgi.action_report_dev_request', 'format_ref_dev_general'),
    ('quimibond_sgi.action_report_epp_delivery', 'format_ref_epp_responsiva'),
    ('quimibond_sgi.action_report_staff_efficiency', 'format_ref_staff_efficiency'),
    ('quimibond_sgi.action_report_indicator_sheet', 'format_ref_indicator_sheet'),
    ('quimibond_sgi.action_report_indicator_sheet_process', 'format_ref_indicator_sheet'),
    ('quimibond_sgi.action_report_sgi_diagram', 'format_ref_diagrams'),
    ('quimibond_sgi.action_report_audit_program', 'format_ref_audit_program'),
    ('quimibond_sgi.action_report_risk_heatmap', 'format_ref_ryo_matrix'),
)
# 57.101.0: sufijo del Diagnóstico para los reportes cuya referencia de formato
# no existe en el código (el Jefe MAST no la puede dar de alta en pantalla).
SGI_NO_KEY_SUFFIX = " (sin clave del SGI)"


class SgiFormatMap(models.Model):
    """Mapeo formato SGI ↔ documento de Odoo que lo sustituye.

    El registro nativo (cotización, OC, remisión…) porta la clave del formato
    controlado que reemplaza; la revisión NUNCA se captura aquí: se lee en vivo
    del documento vigente en la app Documentos (única fuente de verdad).

    C-006 (auditoría 2026-09): el mapeo apunta al DOCUMENTO (``document_id``),
    no al texto de su clave. La clave y la revisión que se imprimen salen de la
    revisión vigente de ese documento, así que un cambio de clave (clave nueva
    D-02) o de revisión se refleja solo. ``sgi_code`` queda como la clave con
    la que se sembró el mapeo: sirve para ligarlo y, mientras no esté ligado,
    para buscar el documento (también por su clave anterior).

    Los mapeos sin modelo (``model_id`` vacío) son los formatos que el código
    usa por referencia (``format_ref_*`` en ``data/sgi_format_map_data.xml``):
    responsiva de EPP, etiquetas de calibración, pie del procedimiento, etc.

    57.60.0 (bloque 2 de formularios): un modelo puede tener **varios**
    mapeos. El que no tiene criterio es el **general** del modelo (uno activo
    por modelo, como antes); los que tienen criterio (tipo de operación,
    centro de trabajo, categoría de producto o filtro) aplican solo a los
    registros que lo cumplen, en orden de prioridad (``sequence``). Así cada
    orden de producción, vale o transferencia imprime su propia clave, y el
    registro que no cumple ningún criterio sigue con la general.
    """
    _name = 'sgi.format.map'
    _description = "Formato SGI en documentos de Odoo"
    _order = 'model_name, sequence, sgi_code, id'

    model_id = fields.Many2one('ir.model', string="Modelo de Odoo",
                               ondelete='cascade',
                               help="El documento de Odoo que sustituye al formato en Excel. "
                                    "Vacío en los formatos que el SGI usa por referencia "
                                    "desde un reporte (etiquetas, responsivas, pies).")
    model_name = fields.Char(related='model_id.model', string="Modelo técnico", store=True)
    document_id = fields.Many2one(
        'documents.document', string="Documento controlado", ondelete='set null',
        index=True, domain=[('sgi_is_controlled', '=', True)],
        help="Formato controlado que se imprime. La clave y la revisión salen de "
             "su revisión vigente, en vivo: si cambia la clave o sube la "
             "revisión, el pie cambia solo.")
    document_alt_id = fields.Many2one(
        'documents.document', string="Documento alternativo", ondelete='set null',
        index=True, domain=[('sgi_is_controlled', '=', True)],
        help="Formato que aplica cuando el registro está confirmado (solo ventas: "
             "cotización vs pedido; presupuesto vs pronóstico).")
    sgi_code = fields.Char(string="Clave al ligar",
                           help="Clave con la que se sembró el mapeo (ej. F-P-A28-04). Solo "
                                "sirve mientras no hay documento ligado; lo impreso sale "
                                "del documento.")
    sgi_code_alt = fields.Char(
        string="Clave alternativa al ligar",
        help="Clave que aplica cuando el registro está confirmado (solo ventas: "
             "cotización vs pedido). Vacío = siempre la clave principal.")
    live_label = fields.Char(string="Clave y revisión vigentes", compute='_compute_live_label')
    live_alt_label = fields.Char(string="Alternativa vigente", compute='_compute_live_label')
    active = fields.Boolean(default=True)
    note = fields.Char(string="Nota")

    # --- 57.60.0: cuándo aplica (varios formatos por modelo) -----------------
    # 57.60.0: sale ``unique(model_id)``; la regla ahora es un mapeo GENERAL
    # activo por modelo (``_check_single_general``) y los demás con criterio.
    sequence = fields.Integer(
        string="Prioridad", default=10,
        help="Si un registro cumple el criterio de más de un mapeo del mismo "
             "modelo, se usa el de número más bajo. El mapeo general (sin "
             "criterio) se usa solo cuando ningún otro aplica.")
    # 57.67.0: los criterios leen también los archivados (``active_test``).
    # Sin eso, un mapeo con un tipo de operación o centro archivado (en
    # producción «Traslados internos» de Toluca, id 5) se leía sin criterio,
    # se calculaba GENERAL y chocaba con el general del modelo.
    picking_type_ids = fields.Many2many(
        'stock.picking.type', 'sgi_format_map_picking_type_rel', 'map_id', 'picking_type_id',
        context={'active_test': False},
        string="Tipos de operación",
        help="Solo los registros de estos tipos de operación (transferencias, "
             "vales, órdenes de producción) imprimen este formato. Vacío = "
             "cualquier tipo.")
    workcenter_ids = fields.Many2many(
        'mrp.workcenter', 'sgi_format_map_workcenter_rel', 'map_id', 'workcenter_id',
        context={'active_test': False},
        string="Centros de trabajo",
        help="Solo las órdenes con una operación en alguno de estos centros de "
             "trabajo imprimen este formato. Vacío = cualquier centro.")
    product_categ_ids = fields.Many2many(
        'product.category', 'sgi_format_map_product_categ_rel', 'map_id', 'categ_id',
        string="Categorías de producto",
        help="Solo los registros cuyo producto es de estas categorías (o de sus "
             "subcategorías) imprimen este formato. Vacío = cualquier producto.")
    record_domain = fields.Char(
        string="Filtro adicional",
        help="Condición opcional sobre el registro, por ejemplo "
             "[('location_dest_id.usage', '=', 'supplier')] para las "
             "devoluciones a proveedor. Vacío = sin filtro.")
    is_general = fields.Boolean(
        string="General del modelo", compute='_compute_is_general', store=True,
        help="Sin criterio: aplica a todo registro del modelo que no cumpla el "
             "criterio de otro mapeo.")
    selector_label = fields.Char(string="Cuándo aplica", compute='_compute_selector_label')

    _SGI_SELECTOR_FIELDS = ('picking_type_ids', 'workcenter_ids', 'product_categ_ids',
                            'record_domain')

    def _sgi_domain(self):
        """Filtro adicional como lista (vacío si no hay o es ``[]``)."""
        self.ensure_one()
        text = (self.record_domain or '').strip()
        if not text:
            return []
        domain = safe_eval(text)
        if not isinstance(domain, (list, tuple)):
            raise ValueError("no es una lista")
        return list(domain)

    @api.depends('picking_type_ids', 'workcenter_ids', 'product_categ_ids', 'record_domain')
    def _compute_is_general(self):
        for fmap in self:
            fmap.is_general = not (
                fmap.picking_type_ids or fmap.workcenter_ids or fmap.product_categ_ids
                or (fmap.record_domain or '').strip() not in ('', '[]'))

    @api.depends('picking_type_ids', 'workcenter_ids', 'product_categ_ids', 'record_domain',
                 'model_id')
    def _compute_selector_label(self):
        for fmap in self:
            parts = []
            if fmap.picking_type_ids:
                parts.append("Tipo: %s" % ", ".join(fmap.picking_type_ids.mapped('name')))
            if fmap.workcenter_ids:
                parts.append("Centro: %s" % ", ".join(fmap.workcenter_ids.mapped('name')))
            if fmap.product_categ_ids:
                parts.append("Categoría: %s" % ", ".join(fmap.product_categ_ids.mapped('name')))
            if (fmap.record_domain or '').strip() not in ('', '[]'):
                parts.append("Filtro: %s" % fmap.record_domain.strip())
            if parts:
                fmap.selector_label = " · ".join(parts)
            else:
                fmap.selector_label = "General del modelo" if fmap.model_id else "Por referencia"

    @api.constrains('model_id', 'picking_type_ids', 'workcenter_ids', 'product_categ_ids',
                    'record_domain')
    def _check_selectors(self):
        """El criterio necesita modelo y que el modelo tenga de dónde leerlo."""
        for fmap in self:
            if fmap.is_general:
                continue
            if not fmap.model_id or fmap.model_id.model not in self.env:
                raise ValidationError(
                    "El mapeo %s tiene criterio pero no modelo: el criterio solo "
                    "aplica a los registros de un modelo de Odoo." % (fmap.sgi_code or fmap.id))
            Model = self.env[fmap.model_id.model]
            if fmap.picking_type_ids and 'picking_type_id' not in Model._fields:
                raise ValidationError(
                    "%s no tiene tipo de operación: quite los tipos de operación "
                    "del mapeo %s." % (fmap.model_id.name, fmap.sgi_code))
            if fmap.workcenter_ids and not (
                    'workorder_ids' in Model._fields or 'workcenter_id' in Model._fields):
                raise ValidationError(
                    "%s no tiene centro de trabajo: quite los centros de trabajo "
                    "del mapeo %s." % (fmap.model_id.name, fmap.sgi_code))
            if fmap.product_categ_ids and not (
                    'product_id' in Model._fields or 'move_ids' in Model._fields):
                raise ValidationError(
                    "%s no tiene producto: quite las categorías de producto del "
                    "mapeo %s." % (fmap.model_id.name, fmap.sgi_code))
            try:
                Model.sudo().search_count(fmap._sgi_domain(), limit=1)
            except Exception as exc:  # noqa: BLE001 - el mensaje va al usuario
                raise ValidationError(
                    "Filtro adicional inválido para %s en el mapeo %s: %s" % (
                        fmap.model_id.name, fmap.sgi_code, exc))

    @api.constrains('model_id', 'active', 'picking_type_ids', 'workcenter_ids',
                    'product_categ_ids', 'record_domain')
    def _check_single_general(self):
        """Un solo mapeo general activo por modelo (antes: ``unique(model_id)``)."""
        for fmap in self.filtered(lambda m: m.model_id and m.active and m.is_general):
            twins = self.search([
                ('model_id', '=', fmap.model_id.id), ('id', '!=', fmap.id),
                ('is_general', '=', True)])
            if twins:
                raise ValidationError(
                    "Ya existe un mapeo general (sin criterio) para %s: %s. Dele a "
                    "este un criterio (tipo de operación, centro de trabajo, "
                    "categoría o filtro) o archive el otro." % (
                        fmap.model_id.name, twins[0].sgi_code or twins[0].id))

    def _sgi_record_products(self, record):
        if 'product_id' in record._fields:
            return record.product_id
        if 'move_ids' in record._fields:
            return record.move_ids.product_id
        return self.env['product.product']

    def _sgi_matches(self, record):
        """Si ``record`` cumple todos los criterios de este mapeo."""
        self.ensure_one()
        record = record.sudo()
        if self.picking_type_ids:
            ptype = record.picking_type_id if 'picking_type_id' in record._fields else False
            if not ptype or ptype.id not in self.picking_type_ids.ids:
                return False
        if self.workcenter_ids:
            centers = self.env['mrp.workcenter']
            if 'workcenter_id' in record._fields:
                centers |= record.workcenter_id
            if 'workorder_ids' in record._fields:
                centers |= record.workorder_ids.workcenter_id
            if not set(centers.ids) & set(self.workcenter_ids.ids):
                return False
        if self.product_categ_ids:
            categs = self._sgi_record_products(record).categ_id
            allowed = self.env['product.category'].sudo().search(
                [('id', 'child_of', self.product_categ_ids.ids)])
            if not categs or not set(categs.ids) & set(allowed.ids):
                return False
        try:
            domain = self._sgi_domain()
            if domain and not record.filtered_domain(domain):
                return False
        except Exception:  # noqa: BLE001 - un filtro roto no tumba la impresión
            _logger.warning("SGI: filtro inválido en el mapeo de formato %s (%s).",
                            self.id, self.record_domain)
            return False
        return True

    @api.model
    def _sgi_map_for(self, record):
        """Mapeo que aplica a ``record``: el primero con criterio que cumple
        (por prioridad) o, si ninguno, el general del modelo. Vacío si no hay."""
        record = record[:1]
        if not record:
            return self.browse()
        maps = self.sudo().search([('model_name', '=', record._name)])
        for fmap in maps.filtered(lambda m: not m.is_general):
            if fmap._sgi_matches(record):
                return fmap
        return maps.filtered('is_general')[:1]

    @api.depends('document_id.sgi_code', 'document_id.sgi_revision', 'document_id.sgi_state',
                 'document_alt_id.sgi_code', 'document_alt_id.sgi_revision',
                 'document_alt_id.sgi_state', 'sgi_code', 'sgi_code_alt')
    def _compute_live_label(self):
        for fmap in self:
            fmap.live_label = fmap.sgi_live_label()
            fmap.live_alt_label = fmap.sgi_live_label(alt=True) \
                if (fmap.document_alt_id or fmap.sgi_code_alt) else False

    @api.constrains('sgi_code', 'sgi_code_alt')
    def _check_codes(self):
        for fmap in self:
            for code in filter(None, (fmap.sgi_code, fmap.sgi_code_alt)):
                code = code.strip()
                # C-007: la nomenclatura vive en los tipos de documento. La
                # clave heredada del Dropbox (SGI_CODE_REGEX) sigue valiendo:
                # al actualizar de 56.28 los tipos aún no exigen clave (la
                # 56.32.0 lo enciende en su post-migrate, DESPUÉS de cargar
                # este XML) y sin ese respaldo ninguna clave pasaba (56.38.2).
                from .sgi_document import SGI_CODE_REGEX
                if not (SGI_CODE_REGEX.match(code)
                        or self.env['sgi.document.type'].sudo()._sgi_any_match(code)):
                    raise ValidationError(
                        "La clave '%s' no cumple la nomenclatura del SGI "
                        "(ej. F-P-A28-04, F-IT-P-P01-08-01)." % code)

    @api.model_create_multi
    def create(self, vals_list):
        records = super().create(vals_list)
        # 57.13.0: un @api.constrains solo corre si sus campos vienen en el
        # alta; sin documento ni clave en los valores, el mapeo vacío pasaba.
        records._check_target()
        return records

    @api.constrains('document_id', 'sgi_code')
    def _check_target(self):
        for fmap in self:
            if not fmap.document_id and not (fmap.sgi_code or '').strip():
                raise ValidationError(
                    "El formato necesita su documento controlado (o, mientras se "
                    "liga, la clave).")

    @api.onchange('document_id', 'document_alt_id')
    def _onchange_document(self):
        for fmap in self:
            if fmap.document_id:
                fmap.sgi_code = fmap.document_id.sgi_code
            if fmap.document_alt_id:
                fmap.sgi_code_alt = fmap.document_alt_id.sgi_code

    @api.model
    def _get_for_model(self, model_name):
        """Mapeo general del modelo (el primero si no hay general). Para el
        registro concreto use ``_sgi_map_for``, que respeta los criterios."""
        return self.search([('model_name', '=', model_name)],
                           order='is_general desc, sequence, id', limit=1)

    # --- Resolución en vivo (C-006) -----------------------------------------
    @api.model
    def _sgi_live_document(self, doc):
        """Revisión vigente del formato ``doc``: el propio documento si está
        vigente; si no (ya salió otra revisión), la vigente con su misma clave.
        Vacío si ninguna revisión está vigente."""
        Doc = self.env['documents.document'].sudo()
        doc = doc.sudo()
        if not doc:
            return Doc
        if doc.active and doc.sgi_state == 'vigente':
            return doc
        if not doc.sgi_code:
            return Doc
        return Doc.search([
            ('sgi_code', '=', doc.sgi_code),
            ('sgi_state', '=', 'vigente'),
            ('sgi_is_controlled', '=', True),
        ], order='sgi_revision desc, id desc', limit=1)

    def _sgi_target(self, alt=False):
        """(documento, clave al ligar) de la variante pedida."""
        self.ensure_one()
        if alt and (self.document_alt_id or self.sgi_code_alt):
            return self.document_alt_id, self.sgi_code_alt
        return self.document_id, self.sgi_code

    def sgi_live_document(self, alt=False):
        """Documento vigente que se imprime con este mapeo (vacío si no hay)."""
        Doc = self.env['documents.document'].sudo()
        if not self:
            return Doc
        doc, code = self._sgi_target(alt)
        if doc:
            return self._sgi_live_document(doc)
        # Sin ligar todavía: por la clave (o la clave anterior).
        return Doc._sgi_find_by_code(code) if code else Doc

    def sgi_live_parts(self, alt=False):
        """(clave, revisión) vivas; revisión False si no hay vigente, y
        (False, False) si el mapeo no existe."""
        if not self:
            return False, False
        live = self.sgi_live_document(alt)
        if live:
            return live.sgi_code, live.sgi_revision_label
        doc, code = self._sgi_target(alt)
        return (doc.sudo().sgi_code if doc else code) or False, False

    def sgi_live_label(self, alt=False):
        """'F-P-A28-12 · Rev. 03' | 'F-P-A28-12' (sin vigente) | False."""
        code, revision = self.sgi_live_parts(alt)
        if not code:
            return False
        return "%s · Rev. %s" % (code, revision) if revision else code

    @api.model
    def _sgi_ref(self, name):
        """Mapeo por referencia (xmlid ``quimibond_sgi.<name>``); vacío si no
        existe o está archivado."""
        fmap = self.env.ref('quimibond_sgi.%s' % name, raise_if_not_found=False)
        if not fmap or fmap._name != self._name or not fmap.active:
            return self.browse()
        return fmap.sudo()

    @api.model
    def sgi_ref_parts(self, name):
        return self._sgi_ref(name).sgi_live_parts()

    @api.model
    def sgi_ref_label(self, name):
        return self._sgi_ref(name).sgi_live_label()

    @api.model
    def sgi_footer_label(self, record, ref=False):
        """V-M08 (57.44.0): clave y revisión del pie «formato controlado» de un
        reporte (``report/sgi_format_footer.xml``). Con ``ref``, el mapeo por
        referencia (``format_ref_*``); si no, el del modelo del registro: con el
        mixin de formato respeta sus reglas por registro, y sin él busca el
        mapeo del modelo (MAST lo da de alta en «Formatos en documentos de
        Odoo»). Sin mapeo devuelve False y el pie no se pinta."""
        if ref:
            return self.sudo().sgi_ref_label(ref)
        record = record[:1] if record else record
        if not record:
            return False
        if 'sgi_format_banner' in record._fields:
            return record.sudo().sgi_format_info()
        fmap = self.sudo()._sgi_map_for(record)
        return fmap.sgi_live_label() if fmap else False

    @api.model
    def _sgi_footer_document(self, record, ref=False):
        """57.98.0 (I-01): documento vigente que se imprime en el pie, por el
        mismo camino que ``sgi_footer_label`` (referencia, mixin o modelo)."""
        Doc = self.env['documents.document'].sudo()
        if ref:
            return self.sudo().sgi_ref_document(ref)
        record = record[:1] if record else record
        if not record:
            return Doc
        if 'sgi_format_banner' in record._fields:
            return record.sudo()._sgi_format_document()
        fmap = self.sudo()._sgi_map_for(record)
        return fmap.sgi_live_document() if fmap else Doc

    @api.model
    def sgi_footer_info(self, record, ref=False):
        """57.98.0 (I-01): {'label': 'F-P-A10-01 · Rev. 00', 'issue_date':
        date | False} del pie «formato controlado»; False sin mapeo. La fecha
        de emisión es la del documento vigente ligado (sin fecha, el pie no
        la pinta)."""
        label = self.sgi_footer_label(record, ref)
        if not label:
            return False
        doc = self._sgi_footer_document(record, ref)
        return {'label': label, 'issue_date': doc.sgi_issue_date if doc else False}

    @api.model
    def _sgi_unmapped_reports(self):
        """57.98.0 (I-01): nombres de los reportes del SGI que hoy imprimen
        sin clave controlada (para el Diagnóstico). Por referencia, el mapeo
        ``format_ref_*`` activo con clave; por modelo, basta un mapeo activo
        del modelo del reporte. Un modelo con solo mapeos con criterio
        (57.60.0) cuenta como mapeado, aunque algún registro no cumpla
        ningún criterio e imprima sin clave.

        57.101.0: si la referencia ni siquiera existe (``format_ref_8d``…),
        el nombre lleva «(sin clave del SGI)»: no se resuelve en «Formatos en
        documentos de Odoo», necesita código. Una referencia archivada sí
        existe y conserva el nombre tal cual."""
        names = []
        for report_xmlid, ref in SGI_FORMAT_REPORTS:
            report = self.env.ref(report_xmlid, raise_if_not_found=False)
            if not report:
                continue
            if ref and not self.env.ref('quimibond_sgi.%s' % ref, raise_if_not_found=False):
                names.append(report.name + SGI_NO_KEY_SUFFIX)
                continue
            if ref:
                mapped = bool(self._sgi_ref(ref).sgi_live_label())
            else:
                mapped = bool(self.sudo().search_count([('model_name', '=', report.model)], limit=1))
            if not mapped:
                names.append(report.name)
        return names

    @api.model
    def sgi_ref_document(self, name):
        return self._sgi_ref(name).sgi_live_document()

    @api.model
    def _sgi_repoint(self, old_docs, new_doc):
        """Una revisión nueva entra en vigor: los mapeos que apuntaban a las
        revisiones anteriores pasan a la nueva (la resolución en vivo ya la
        encontraba; así la liga queda al día y la vista lo muestra)."""
        if not old_docs or not new_doc:
            return
        maps = self.sudo().with_context(active_test=False)
        maps.search([('document_id', 'in', old_docs.ids)]).write({'document_id': new_doc.id})
        maps.search([('document_alt_id', 'in', old_docs.ids)]).write(
            {'document_alt_id': new_doc.id})

    @api.model
    def _revision_of(self, code):
        """Compatibilidad: revisión del documento vigente con esa clave (o con
        esa clave anterior). El código del SGI ya no busca por texto: usa el
        documento ligado al mapeo (``sgi_live_parts``)."""
        doc = self.env['documents.document'].sudo()._sgi_find_by_code(code)
        return doc.sgi_revision_label if doc else False


class SgiConfig(models.AbstractModel):
    """Utilidades de configuración del SGI (siembra idempotente)."""
    _name = 'sgi.config'
    _description = "Configuración SGI"

    # Parámetros operativos: default de arranque. Solo se crean si NO existen;
    # lo editado en Ajustes > Técnico > Parámetros del sistema nunca se pisa.
    _SGI_DEFAULT_PARAMS = {
        # Verbos que revisa la especificación de actividades (sgi_activity_spec).
        'quimibond_sgi.vague_verbs': ('dar seguimiento,gestionar,coordinar,apoyar,asegurar,'
                                      'atender,ver,checar,manejar,controlar'),
        'quimibond_sgi.compare_verbs': 'verificar,comparar,revisar,validar,conciliar,inspeccionar',
        'quimibond_sgi.nc_escalation_days': '5',
        # Días por omisión de una prueba piloto documental.
        'quimibond_sgi.pilot_days': '60',
        'quimibond_sgi.nc_escalation_days_external': '3',
        # NC-1/NC-3 (49.0.0): plazos por etapa en días hábiles desde que se
        # abre la NC, días de gracia antes de escalar a MAST y días para la
        # verificación de eficacia tras la última acción correctiva.
        'quimibond_sgi.nc_days_containment': '1',
        'quimibond_sgi.nc_days_root_cause': '10',
        'quimibond_sgi.nc_days_plan': '15',
        'quimibond_sgi.nc_escalation_mast_days': '3',
        'quimibond_sgi.nc_effectiveness_days': '90',
        # NC-6: días hábiles que tiene el proveedor para contestar por el portal.
        'quimibond_sgi.nc_days_supplier_response': '5',
        'quimibond_sgi.nc_recurrence_months': '12',
        'quimibond_sgi.action_escalation_manager_days': '7',
        'quimibond_sgi.action_escalation_director_days': '15',
        'quimibond_sgi.doc_review_notice_days': '60',
        'quimibond_sgi.doc_review_notice_days_final': '30',
        'quimibond_sgi.doc_ack_pending_days': '7',
        'quimibond_sgi.doc_pilot_notice_days': '7',
        'quimibond_sgi.fmea_npr_action': '100',
        'quimibond_sgi.risk_ryo_inmediata': '16',
        'quimibond_sgi.risk_ryo_media': '9',
        'quimibond_sgi.risk_ryo_intermedia': '4',
        'quimibond_sgi.supplier_weight_otd': '0.7',
        'quimibond_sgi.supplier_weight_quality': '0.3',
        'quimibond_sgi.supplier_nc_penalty': '10.0',
        # Días de gracia del OTD de proveedores (comparación por día calendario).
        'quimibond_sgi.supplier_otd_tolerance_days': '1',
        # 57.10.0 (A-020): quimibond_sgi.pesaje_tolerance_kg la siembra quimibond_sgi_pesaje.
        # I-3: desperdicio en kg y compras de materia prima (sgi_indicator_i3.py).
        'quimibond_sgi.waste_location_ids': '39,43',
        'quimibond_sgi.waste_input_categ_ids': '350,356',
        'quimibond_sgi.raw_material_categ_id': '318',
        # P-21: tipos de operación que cuentan como reproceso (MA-04).
        # Re-proceso Tintorería (106) y Re-proceso Acabado (107); «Acabado
        # producto en proceso» (151) entra cuando producción lo confirme.
        'quimibond_sgi.rework_picking_type_ids': '106,107',
        # 57.14.0 (indicadores 2): categorías de producto terminado de C1-04
        # (319 «Producto Terminado», con sus hijas).
        'quimibond_sgi.finished_product_categ_ids': '319',
        # I-6: día hábil del mes en que se miden los indicadores mensuales.
        'quimibond_sgi.monthly_measure_business_day': '3',
        # I-4: día del mes siguiente en que vence la causa y acción de un rojo.
        'quimibond_sgi.red_plan_due_day': '10',
        # I-2: mínimo de casos para que una medición cuente para NC.
        'quimibond_sgi.indicator_min_sample': '5',
        # P-7: no surtir lotes sin liberar (models/sgi_release.py).
        'quimibond_sgi.release_block_enabled': 'True',
        'quimibond_sgi.release_block_picking_type_ids': '113,210',
        'quimibond_sgi.unreleased_location_ids': '324,44,36,246,45',
        'quimibond_sgi.waste_subproduct_category': 'SubProducto',
        # COA (sgi_coa): el bloqueo al validar una salida sin COA se enciende
        # cuando el cambio se implemente formalmente; la excepción es del
        # puesto Jefe de Calidad.
        'quimibond_sgi.coa_block_validation': 'False',
        'quimibond_sgi.coa_exception_job_id': '204',
        'quimibond_sgi.monthly_sales_budget': '0',
        'quimibond_sgi.rh_user_id': '0',
        'quimibond_sgi.purchase_approval_category_id': '0',
        # Capacidad instalada mensual de producción (misma unidad que la
        # producción, p.ej. kg) para el KPI MA-02. 0 = captura manual.
        'quimibond_sgi.production_monthly_capacity': '0',
        # Proveedor de energía (res.partner) para el KPI TR-03. 0 = sin configurar.
        'quimibond_sgi.energy_partner_id': '0',
        # Encuesta que alimenta el KPI CA-02 (survey.survey). 0 = usar la
        # sembrada del módulo. Permite re-apuntar al histórico archivado.
        'quimibond_sgi.satisfaction_survey_id': '0',
        # 57.100.0 (N-13): días para evaluar la eficacia de la capacitación y
        # encuesta opcional al jefe (survey.survey; 0 = sin encuesta).
        'quimibond_sgi.training_effectiveness_days': '90',
        'quimibond_sgi.training_effectiveness_survey_id': '0',
        # 57.100.0 (N-14): meses de ventas que hacen «cliente del producto» en el ECO.
        'quimibond_sgi.ppap_sales_window_months': '12',
        # 57.100.0 (IA, puerta Q16): apagada hasta la autorización escrita de
        # Jose. Proveedor Anthropic; modelo configurable; segundos de espera;
        # si se mandan las 3 NC cerradas del mismo proceso. La llave
        # (quimibond_sgi.ai_api_key) no se siembra: la captura un administrador.
        'quimibond_sgi.ai_enabled': 'False',
        'quimibond_sgi.ai_backend': 'anthropic',
        'quimibond_sgi.ai_model': 'claude-opus-5-5',
        'quimibond_sgi.ai_timeout': '60',
        'quimibond_sgi.ai_include_history': 'True',
        # 57.11.0 (A-016): los 9 parámetros del presupuesto y del pronóstico de
        # ventas los siembra quimibond_ventas_presupuesto (mismas claves).
    }

    @api.model
    def seed_parameters(self):
        sgi_require_system(self.env)  # F-008
        Param = self.env['ir.config_parameter'].sudo()
        for key, value in self._SGI_DEFAULT_PARAMS.items():
            if Param.get_param(key) is False:
                Param.set_param(key, value)
        return True

    @api.model
    def _sgi_recompute_months(self):
        raw = self.env['ir.config_parameter'].sudo().get_param(RECOMPUTE_MONTHS_PARAM, '')
        return int(raw) if str(raw).strip().isdigit() else DEFAULT_RECOMPUTE_MONTHS

    @api.model
    def _sgi_recompute_domain(self, recent=True):
        """57.104.0 (B3): mediciones «sin dato» y capturadas que el recálculo
        vuelve a medir. Nunca: validadas, manuales, de salud (57.99.0), de foto
        (57.90.0), con NC ni corregidas a mano. Con ``recent='all'`` sin
        ventana; si no, desde el día 1 del mes de hace N meses."""
        domain = [
            ('state', 'in', ('sin_dato', 'capturado')),
            ('indicator_id.calc_mode', '!=', 'manual'),
            ('indicator_id.calc_mode', 'not in', list(HEALTH_MODES)),
            ('indicator_id.snapshot', '=', False),
            ('alert_id', '=', False),
            ('sgi_value_by_hand', '=', False),
        ]
        if recent != 'all':
            start = sgi_today(self.env).replace(day=1) - relativedelta(
                months=self._sgi_recompute_months())
            domain.append(('period_date', '>=', start))
        return domain

    @staticmethod
    def _sgi_measure_summary(state, value, numerator, denominator):
        """«sin dato» o «valor (numerador/denominador)», para comparar y para
        el chatter de la medición."""
        if state == 'sin_dato':
            return "sin dato"
        text = "%s" % round(value or 0.0, 2)
        if numerator or denominator:
            text += " (%s/%s)" % (round(numerator or 0.0, 2), round(denominator or 0.0, 2))
        return text

    @api.model
    def _sgi_note_by_person(self, measure):
        """Si alguien que no es el sistema editó la nota de la medición (queda
        en su seguimiento: la nota lleva ``tracking``)."""
        root = self.env.ref('base.partner_root', raise_if_not_found=False)
        values = self.env['mail.tracking.value'].sudo().search([
            ('mail_message_id.model', '=', 'sgi.indicator.measure'),
            ('mail_message_id.res_id', '=', measure.id),
            ('field_id.name', '=', 'note'), ('field_id.model', '=', 'sgi.indicator.measure')])
        return any(v.mail_message_id.author_id != root for v in values)

    @api.model
    def recompute_pending_measures(self, indicators=None, recent=False):
        """Re-mide las mediciones PENDIENTES de indicadores automáticos.
        Es la contracara de la deuda B.16: el cron solo crea la medición
        faltante, así que activar un modo o corregir el motor dejaba los
        meses ya generados en blanco para siempre. Solo escribe cuando el
        cálculo ahora sí devuelve valor; sin dato sigue pendiente y no se
        toca (cero ruido en el chatter). Las validadas jamás se tocan.

        57.5.0 (D-12, A-006): ya no corre en cada actualización del módulo;
        lo corre el cron diario de indicadores y el botón «Recalcular
        mediciones» del Administrador SGI. Cada medición va en su savepoint:
        un indicador con error se registra en el log y no detiene a los
        demás. ``indicators`` acota a esos indicadores.

        57.104.0 (B3): con ``recent=True`` (el cron diario programado) también
        re-mide las «sin dato» y las capturadas (no validadas) de los últimos
        N meses (``quimibond_sgi.indicator_recompute_months``, 2 por omisión:
        desde el día 1 del mes de hace N meses). Con ``recent='all'`` (el botón
        del Administrador SGI con indicadores seleccionados) re-mide todas las
        no validadas de esos indicadores, sin ventana. Nunca toca: validadas,
        indicadores de foto (57.90.0) ni de salud (57.99.0), mediciones con NC,
        con causa o acciones capturadas, ni las que alguien corrigió a mano
        (``sgi_value_by_hand``). Solo escribe si algo cambió, y deja en el
        chatter de la medición el antes y el después. Un error por medición
        queda como aviso (WARNING) en el log.

        La corrida del cron (``recent=True``) tiene tiempo tope
        (``RECOMPUTE_BUDGET_SECONDS``): al agotarse deja un cursor y la
        siguiente sigue desde ahí. El recálculo conserva las líneas de la nota
        que escribió una persona.

        Devuelve ``{'revisadas': n, 'capturadas': n, 'recalculadas': n,
        'errores': n, 'sin_tiempo': n}``."""
        sgi_require_system(self.env)  # F-008
        Measure = self.env['sgi.indicator.measure']
        pending = [
            ('state', '=', 'pendiente'),
            ('indicator_id.calc_mode', '!=', 'manual'),
            ('sgi_value_by_hand', '=', False),
        ]
        scope = [('indicator_id', 'in', indicators.ids)] if indicators is not None else []
        measures = Measure.search(pending + scope)
        if recent:
            stale = Measure.search(self._sgi_recompute_domain(recent) + scope)
            # Un rojo con causa o acciones ya se trabajó: su valor no se mueve.
            measures |= stale.filtered(lambda m: not (m.cause or m.action_line_ids))
        Param = self.env['ir.config_parameter'].sudo()
        budget = RECOMPUTE_BUDGET_SECONDS if recent is True else None
        if budget:
            # Con tiempo tope se sigue desde donde se quedó la corrida anterior
            # (ids mayores al cursor primero) para que la cola no se atore.
            cursor = int(Param.get_param(RECOMPUTE_CURSOR_PARAM, 0) or 0)
            measures = measures.sorted('id')
            measures = measures.filtered(lambda m: m.id > cursor) \
                | measures.filtered(lambda m: m.id <= cursor)
        else:
            measures = measures.sorted(lambda m: (m.indicator_id.id, m.period_date))
        result = {'revisadas': len(measures), 'capturadas': 0, 'recalculadas': 0,
                  'errores': 0, 'sin_tiempo': 0}
        latest = {}
        start = _now()
        for position, measure in enumerate(measures):
            if budget and _now() - start > budget:
                result['sin_tiempo'] = len(measures) - position
                Param.set_param(RECOMPUTE_CURSOR_PARAM, str(measures[position - 1].id)
                                if position else '0')
                _logger.info("SGI: recálculo de mediciones detenido por tiempo (%d s): faltan %d; "
                             "siguen en la próxima corrida.", budget, result['sin_tiempo'])
                break
            indicator = measure.indicator_id
            try:
                with self.env.cr.savepoint():
                    date_from, date_to = indicator._sgi_period_bounds(measure.period_date)
                    vals = indicator._sgi_measure_vals(date_from, date_to)
                    # I-3: la nota que escribió una persona no se pierde.
                    if 'note' in vals and measure.note and measure.note != vals['note'] \
                            and self._sgi_note_by_person(measure):
                        vals['note'] = _sgi_merge_note(vals['note'], measure.note)
                    if measure.state == 'pendiente':
                        if vals.get('state') != 'capturado':
                            continue
                        measure.with_context(sgi_calc_write=True).write(vals)
                        result['capturadas'] += 1
                    else:
                        # I-4: una capturada nunca regresa a pendiente (p. ej.
                        # un modo con plazo que aún no vence).
                        if vals.get('state') not in ('capturado', 'sin_dato'):
                            continue
                        before = self._sgi_measure_summary(
                            measure.state, measure.value, measure.numerator, measure.denominator)
                        after = self._sgi_measure_summary(
                            vals['state'], vals.get('value'), vals.get('numerator'),
                            vals.get('denominator'))
                        if before == after:
                            continue
                        # Un solo rastro: el mensaje con el antes y el después.
                        measure.with_context(sgi_calc_write=True, tracking_disable=True).write(vals)
                        measure.message_post(
                            body="Recalculada por el SGI (57.104.0): antes %s, ahora %s." % (
                                before, after))
                        result['recalculadas'] += 1
                    if measure.period_date >= latest.get(indicator.id, (measure.period_date, None))[0]:
                        latest[indicator.id] = (measure.period_date, vals)
            except Exception as error:  # noqa: BLE001 - un indicador no detiene a los demás
                result['errores'] += 1
                _logger.warning("SGI: no se pudo recalcular la medición %s de %s: %s",
                                measure.id, indicator.display_name, str(error)[:250])
        else:
            if budget:
                Param.set_param(RECOMPUTE_CURSOR_PARAM, '0')  # vuelta completa
        # El diagnóstico del indicador sale del último periodo tocado.
        for indicator_id, (_period, vals) in latest.items():
            indicator = self.env['sgi.indicator'].browse(indicator_id)
            indicator._sgi_set_calc(*indicator._sgi_calc_diagnose(vals))
        _logger.info("SGI: mediciones pendientes recalculadas: %(revisadas)d revisadas, "
                     "%(capturadas)d capturadas, %(recalculadas)d recalculadas, "
                     "%(errores)d con error.", result)
        return result

    @api.model
    def migrate_document_families(self):
        """H21: llena sgi_parent_document_id (FK) donde esté vacío, infiriendo el
        procedimiento padre P-Xnn vigente por la nomenclatura. Idempotente: solo
        toca los documentos sin padre. Los que no matcheen quedan vacíos y se
        reporta el conteo en el log (carga manual posterior de MAST)."""
        sgi_require_system(self.env)  # F-008
        Doc = self.env['documents.document']
        family_re = re.compile(r'(P-[AGCDEIMPSV]\d{2})')
        docs = Doc.search([
            ('sgi_is_controlled', '=', True),
            ('sgi_parent_document_id', '=', False),
        ])
        filled = unmatched = 0
        for doc in docs:
            code = (doc.sgi_code or '').strip().upper()
            match = family_re.search(code)
            if not match or code == match.group(1):
                continue  # sin clave de familia, o es el propio padre P-Xnn
            parent = Doc.search([
                ('sgi_code', '=', match.group(1)),
                ('sgi_state', '=', 'vigente'),
            ], limit=1)
            if parent and parent.id != doc.id:
                doc.sgi_parent_document_id = parent.id
                filled += 1
            else:
                unmatched += 1
        _logger.info(
            "SGI familia documental (H21): %d FK llenados por nomenclatura, "
            "%d sin procedimiento padre vigente (quedan para captura de MAST).",
            filled, unmatched)
        return True

    # Entrega 4 (decisión de Jose, 2026-09-29): Dirección dejó de implicar al
    # Jefe MAST, que le daba estas tres administraciones. Dirección las
    # conserva asignadas directo al usuario; pierde solo lo de Jefe MAST.
    _SGI_DIRECTOR_ADMIN_GROUPS = (
        'project.group_project_manager',
        'approvals.group_approval_manager',
        'helpdesk.group_helpdesk_manager',
    )

    @api.model
    def _sgi_director_keep_admin_groups(self):
        """Agrega DIRECTO (group_ids) a cada usuario interno activo con
        «Dirección de Operaciones (SGI)» explícito las administraciones de
        Proyecto, Aprobaciones y Soporte. Salta el grupo que no exista.
        Idempotente: solo escribe lo que falta. Devuelve {login: [xmlid]}."""
        director = self.env.ref('quimibond_sgi.group_sgi_director', raise_if_not_found=False)
        if not director:
            return {}
        groups = self.env['res.groups']
        for xmlid in self._SGI_DIRECTOR_ADMIN_GROUPS:
            group = self.env.ref(xmlid, raise_if_not_found=False)
            if group:
                groups |= group
            else:
                _logger.warning("SGI: no existe el grupo %s; Dirección se queda sin él.", xmlid)
        users = self.env['res.users'].sudo().with_context(active_test=True).search([
            ('group_ids', 'in', director.id), ('share', '=', False)])
        added = {}
        for user in users:
            missing = groups - user.group_ids
            if missing:
                user.write({'group_ids': [(4, g.id) for g in missing]})
                added[user.login] = missing.mapped(lambda g: g.get_external_id().get(g.id) or g.name)
        if added:
            _logger.info("SGI: Dirección conserva sus administraciones directas; agregadas: %s", added)
        else:
            _logger.info("SGI: Dirección ya tenía sus administraciones directas (%d usuario(s)).", len(users))
        return added


class SgiFormatMixin(models.AbstractModel):
    """Agrega al modelo la clave del formato SGI que sustituye (pantalla y PDF)."""
    _name = 'sgi.format.mixin'
    _description = "Mixin: clave de formato SGI"

    sgi_format_banner = fields.Char(
        string="Formato SGI", compute='_compute_sgi_format_banner')

    def _sgi_format_applies(self):
        """Si este registro en particular porta la clave del mapeo GENERAL
        (hook por modelo). Un mapeo con criterio ya dice a qué registros
        aplica, así que este hook no lo filtra (57.60.0)."""
        self.ensure_one()
        return True

    def _sgi_format_use_alt(self, fmap):
        """Si este registro usa la variante alternativa del mapeo (hook por
        modelo: pedido confirmado, pronóstico)."""
        self.ensure_one()
        return False

    def sgi_format_info(self):
        """'F-P-A28-04 · Rev. 03' | 'F-P-A28-04' (sin doc vigente) | False.
        Clave y revisión salen del documento ligado al mapeo (C-006)."""
        self.ensure_one()
        fmap = self.env['sgi.format.map'].sudo()._sgi_map_for(self)
        if not fmap or (fmap.is_general and not self._sgi_format_applies()):
            return False
        return fmap.sgi_live_label(alt=self._sgi_format_use_alt(fmap))

    def _sgi_format_document(self):
        """57.98.0 (I-01): documento vigente del formato de este registro
        (misma regla que ``sgi_format_info``); vacío si no aplica."""
        self.ensure_one()
        fmap = self.env['sgi.format.map'].sudo()._sgi_map_for(self)
        if not fmap or (fmap.is_general and not self._sgi_format_applies()):
            return self.env['documents.document'].sudo()
        return fmap.sgi_live_document(alt=self._sgi_format_use_alt(fmap))

    def _compute_sgi_format_banner(self):
        for record in self:
            record.sgi_format_banner = record.sgi_format_info()


# --- Aplicación del mixin a los modelos mapeados -----------------------------

class SaleOrder(models.Model):
    _name = 'sale.order'
    _inherit = ['sale.order', 'sgi.format.mixin']

    def _sgi_format_use_alt(self, fmap):
        self.ensure_one()
        return self.state == 'sale'


class PurchaseOrder(models.Model):
    _name = 'purchase.order'
    _inherit = ['purchase.order', 'sgi.format.mixin']


class StockPicking(models.Model):
    _name = 'stock.picking'
    _inherit = ['stock.picking', 'sgi.format.mixin']

    def _sgi_format_applies(self):
        self.ensure_one()
        return self.picking_type_code == 'outgoing'


class MrpProduction(models.Model):
    _name = 'mrp.production'
    _inherit = ['mrp.production', 'sgi.format.mixin']


class MaintenanceRequest(models.Model):
    _name = 'maintenance.request'
    _inherit = ['maintenance.request', 'sgi.format.mixin']


class QualityAlert(models.Model):
    _name = 'quality.alert'
    _inherit = ['quality.alert', 'sgi.format.mixin']


class StockLot(models.Model):
    _name = 'stock.lot'
    _inherit = ['stock.lot', 'sgi.format.mixin']


class SgiManagementReview(models.Model):
    _name = 'sgi.management.review'
    _inherit = ['sgi.management.review', 'sgi.format.mixin']


class SgiCalibration(models.Model):
    """57.62.0: la verificación de laboratorio imprime su formato
    (``format_map_lab_verification``)."""
    _name = 'sgi.calibration'
    _inherit = ['sgi.calibration', 'sgi.format.mixin']


class MaintenanceEquipment(models.Model):
    """57.62.0: el equipo de laboratorio porta el formato del instrumental
    (``format_map_lab_equipment``)."""
    _name = 'maintenance.equipment'
    _inherit = ['maintenance.equipment', 'sgi.format.mixin']

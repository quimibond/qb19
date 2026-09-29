# -*- coding: utf-8 -*-
import logging
import re

from odoo import models, fields, api
from odoo.exceptions import ValidationError

from .sgi_guard import sgi_require_system

_logger = logging.getLogger(__name__)


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
    """
    _name = 'sgi.format.map'
    _description = "Formato SGI en documentos de Odoo"
    _order = 'sgi_code'

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

    _model_uniq = models.Constraint(
        'unique(model_id)',
        "Ya existe un mapeo de formato para este modelo.",
    )

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
        return self.search([('model_name', '=', model_name)], limit=1)

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
        # Umbral (%) de cumplimiento acumulado del presupuesto de ventas bajo el
        # cual el cierre de mes avisa al responsable del equipo.
        'quimibond_sgi.sales_budget_alert_pct': '80',
        # Tipo de cambio presupuestal USD→MXN para sugerir precios de listas en
        # otra moneda. 0 = usar el tipo de cambio vigente del día de captura.
        'quimibond_sgi.budget_planning_rate': '0',
        # Control de precios (lista vs facturado): tolerancia % del gap para
        # 'leve' y umbral 'grave'.
        'quimibond_sgi.price_gap_tolerance_pct': '3.0',
        'quimibond_sgi.price_gap_grave_pct': '10.0',
        # Cumplimiento mínimo (%) del presupuesto por debajo del cual se pide
        # justificación del incumplimiento (P-A28 4.3.6.1). No bloquea nada.
        'quimibond_sgi.budget_fulfillment_min': '80',
        # Cobertura del pronóstico (P-A28 4.2.2.7): tolerancia % sobre 100 para
        # 'excedido', y horizonte de captura en semanas (semana actual + N-1).
        'quimibond_sgi.forecast_over_tolerance_pct': '10.0',
        'quimibond_sgi.forecast_capture_horizon_weeks': '3',
        # Precio de lista mínimo plausible (moneda compañía): por debajo se toma
        # como placebo (placeholder $1) y la línea queda sin precio de lista.
        'quimibond_sgi.price_min_plausible': '5.0',
        # Lista de precios PRESUPUESTAL para líneas sin cliente (id). 0 = sin
        # configurar: esas líneas quedan sin precio (nunca una lista arbitraria).
        'quimibond_sgi.budget_pricelist_id': '0',
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
    def recompute_pending_measures(self, indicators=None):
        """Re-mide las mediciones PENDIENTES de indicadores automáticos.
        Es la contracara de la deuda B.16: el cron solo crea la medición
        faltante, así que activar un modo o corregir el motor dejaba los
        meses ya generados en blanco para siempre. Solo escribe cuando el
        cálculo ahora sí devuelve valor; sin dato sigue pendiente y no se
        toca (cero ruido en el chatter). Las validadas jamás se tocan.

        57.5.0 (D-12, A-006): ya no corre en cada actualización del módulo;
        lo corre el cron diario de indicadores y el botón «Recalcular
        mediciones pendientes» del Administrador SGI. Cada medición va en su
        savepoint: un indicador con error se registra en el log y no detiene
        a los demás. ``indicators`` acota a esos indicadores. Devuelve
        ``{'revisadas': n, 'capturadas': n, 'errores': n}``."""
        sgi_require_system(self.env)  # F-008
        domain = [
            ('state', '=', 'pendiente'),
            ('indicator_id.calc_mode', '!=', 'manual'),
        ]
        if indicators is not None:
            domain.append(('indicator_id', 'in', indicators.ids))
        measures = self.env['sgi.indicator.measure'].search(domain)
        result = {'revisadas': len(measures), 'capturadas': 0, 'errores': 0}
        for measure in measures:
            indicator = measure.indicator_id
            try:
                with self.env.cr.savepoint():
                    date_from, date_to = indicator._sgi_period_bounds(measure.period_date)
                    vals = indicator._sgi_measure_vals(date_from, date_to)
                    if vals.get('state') != 'capturado':
                        continue
                    measure.write(vals)
                    result['capturadas'] += 1
            except Exception:  # noqa: BLE001 - un indicador no detiene a los demás
                result['errores'] += 1
                _logger.exception("SGI: no se pudo recalcular la medición %s de %s.",
                                  measure.id, indicator.display_name)
        _logger.info("SGI: mediciones pendientes recalculadas: %(revisadas)d revisadas, "
                     "%(capturadas)d capturadas, %(errores)d con error.", result)
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
        """Si este registro en particular porta la clave (hook por modelo)."""
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
        fmap = self.env['sgi.format.map'].sudo()._get_for_model(self._name)
        if not fmap or not self._sgi_format_applies():
            return False
        return fmap.sgi_live_label(alt=self._sgi_format_use_alt(fmap))

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

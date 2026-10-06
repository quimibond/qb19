# -*- coding: utf-8 -*-
"""Proyecto único de desarrollo de producto con ciclo de vida (57.118.0, C1 bloque 2).

Un solo proyecto que nace como **análisis** y recibe folio FT cuando el
cliente aprueba el inicio; no dos proyectos. Lo que decidió Jose el
2026-10-06 (plan `docs/superpowers/plans/2026-10-06-c1-desarrollo-producto-plan.md`,
decisión 3):

- ``sgi_is_ft`` es la **bandera «desarrollo de producto»**: la pone el tipo
  de proyecto (plantilla o captura), ya no el nombre. Habilita las pestañas
  de desarrollo y es el filtro de las mediciones del SGI (antes
  ``name =like 'FT-%'``).
- El **folio FT** es un campo aparte (``sgi_ft_folio``): secuencia anual
  FT-001-2027 asignada al pasar a «Muestra» (desarrollo aprobado por el
  cliente) o con el botón; admite capturar a mano un folio histórico.
- **Etapas de avance** (``project.project.stage``, datos de este módulo):
  solicitud, análisis, cotización, aprobación del cliente, muestra, respuesta
  del cliente, pilotaje, liberado, cerrado sin producto. El cliente vive solo
  en ``partner_id``.
- El **nombre** se arma solo con folio (o «Análisis»), código del artículo o
  producto pedido y revisión; en la ficha queda en solo lectura.
- **Origen** cliente o interno (solicitante de ``hr.employee``).
- **Revisión** entera con bitácora (``sgi.dev.revision``): cada cambio a la
  especificación de un renglón después del análisis deja fecha, quién, la
  característica, valor anterior y nuevo; «Subir revisión» abre la siguiente.
- **Correo propio** por proyecto: el alias nativo de Proyectos se nombra
  ``desarrollo-<id>`` al crear y ``ft-039-2026`` al asignar el folio.
- **Pestaña comercial** que sustituye al Análisis de mercado Industrial, con
  listas (``sgi.dev.option``) en lugar de texto.
- **Muestra física** con fechas, carpeta y etiqueta imprimible.
- **Resultado del análisis**: producto de línea (liga el artículo y cierra
  sin FT), producto nuevo o no factible con motivo de lista.
- **Relojes en horas** por etapa (``sgi.dev.stage.log``) y de materia prima
  (``sgi.dev.mp.wait``): las horas de desarrollo de una etapa descuentan el
  tiempo en que hubo materia prima pendiente. Horas calendario.

La migración 19.0.57.118.0 marca los proyectos FT- existentes, las plantillas
de Diseño y Desarrollo y el proyecto de análisis, separa folio, código y
revisión del nombre viejo, toma el cliente de la etapa cuando faltaba y
reescribe los filtros de medición que decían ``name =like 'FT-%'``.
"""
import re

from odoo import api, fields, models
from odoo.exceptions import UserError

FT_FOLIO_RE = re.compile(r'^\s*FT[-\s]?(\d{1,4})\s*[-/]\s*(\d{4})\s*(.*)$', re.IGNORECASE)
REV_RE = re.compile(r'\bREV\.?\s*(\d+)\s*$', re.IGNORECASE)
# Nombres de las etapas de avance, en orden. Las claves son los xmlids de
# data/sgi_dev_project_data.xml (sgi_dev_stage_<clave>).
DEV_STAGES = [
    ('solicitud', "Solicitud"),
    ('analisis', "Análisis"),
    ('cotizacion', "Cotización"),
    ('aprobacion_cliente', "Aprobación del cliente"),
    ('muestra', "Muestra"),
    ('respuesta_cliente', "Respuesta del cliente"),
    ('pilotaje', "Pilotaje"),
    ('liberado', "Liberado"),
    ('cerrado_sin_producto', "Cerrado sin producto"),
]
# A partir de esta etapa el proyecto es un desarrollo aprobado: lleva folio FT.
FOLIO_FROM_STAGE = 'muestra'
# Desde esta etapa los cambios a la especificación se anotan como revisión.
REVISION_FROM_STAGE = 'cotizacion'
YARD_M = 0.9144
OPTION_KINDS = [
    ('aplicacion', "Aplicación"),
    ('mercado', "Mercado"),
    ('laminado', "Tipo de laminado"),
    ('requisito_legal', "Requisito legal o reglamentario"),
    ('motivo_no_factible', "Motivo de no factibilidad"),
]
# Dominios de medición viejos → nuevos (migración y mapa de procesos).
LEGACY_DOMAIN_REPLACEMENTS = (
    ("('project_id.name', '=like', 'FT-%')", "('project_id.sgi_is_ft', '=', True)"),
    ("('name', '=like', 'FT-%')", "('sgi_is_ft', '=', True)"),
)


class SgiDevOption(models.Model):
    """Opción de lista para el proyecto de desarrollo (aplicación, mercado, laminado, requisito legal,
    motivo de no factibilidad)."""
    _name = 'sgi.dev.option'
    _description = "Opción de lista del desarrollo de producto"
    _order = 'kind, sequence, name, id'

    kind = fields.Selection(OPTION_KINDS, string="Lista", required=True, index=True,
                            help="A qué lista pertenece la opción.")
    sequence = fields.Integer(default=10, help="Orden dentro de su lista.")
    name = fields.Char(string="Opción", required=True, help="Texto de la opción tal como se elige.")
    active = fields.Boolean(default=True, help="Las opciones archivadas no se proponen en proyectos nuevos.")

    _kind_name_uniq = models.Constraint(
        'unique(kind, name)',
        "Esa opción ya existe en esa lista.",
    )


class SgiDevRevision(models.Model):
    """Renglón de la bitácora de revisiones de un desarrollo: qué cambió, cuándo y quién lo pidió."""
    _name = 'sgi.dev.revision'
    _description = "Revisión del desarrollo de producto"
    _order = 'date desc, id desc'

    project_id = fields.Many2one('project.project', required=True, ondelete='cascade', index=True,
                                 help="Proyecto de desarrollo al que pertenece la revisión.")
    revision = fields.Integer(string="Revisión", help="Número de revisión del proyecto al que corresponde el cambio.")
    date = fields.Datetime(string="Fecha", default=fields.Datetime.now, required=True,
                           help="Cuándo se registró el cambio.")
    user_id = fields.Many2one('res.users', string="Registró", default=lambda self: self.env.user,
                              help="Usuario que registró el cambio en Odoo.")
    requested_by = fields.Selection([('cliente', "Cliente"), ('interno', "Interno")], string="Lo pidió",
                                    default='cliente', help="Quién pidió el cambio: el cliente o alguien de Quimibond.")
    characteristic_id = fields.Many2one('sgi.dev.characteristic', string="Característica", ondelete='set null',
                                        help="Renglón de la tabla de características que cambió.")
    characteristic_name = fields.Char(string="Característica (texto)",
                                      help="Nombre de la característica, por si el renglón se borra.")
    old_value = fields.Char(string="Valor anterior", help="Especificación antes del cambio.")
    new_value = fields.Char(string="Valor nuevo", help="Especificación después del cambio.")
    quotation_ref = fields.Char(string="Cotización asociada",
                                help="Referencia de la cotización que recoge el cambio (la liga directa llega con "
                                     "el cotizador nuevo).")
    note = fields.Char(string="Observaciones", help="Texto libre; no capture aquí valores.")


class SgiDevStageLog(models.Model):
    """Paso de un desarrollo por una etapa: hora de inicio y fin, horas totales y horas de desarrollo
    (sin el tiempo con materia prima pendiente)."""
    _name = 'sgi.dev.stage.log'
    _description = "Reloj por etapa del desarrollo de producto"
    _order = 'date_start desc, id desc'

    project_id = fields.Many2one('project.project', required=True, ondelete='cascade', index=True,
                                 help="Proyecto de desarrollo.")
    stage_id = fields.Many2one('project.project.stage', string="Etapa", required=True, ondelete='cascade',
                               help="Etapa en la que estuvo el proyecto.")
    date_start = fields.Datetime(string="Inicio", required=True, default=fields.Datetime.now,
                                 help="Cuándo entró el proyecto a la etapa.")
    date_end = fields.Datetime(string="Fin", help="Cuándo salió de la etapa; vacío mientras siga ahí.")
    hours_total = fields.Float(string="Horas en la etapa", compute='_compute_hours', digits=(16, 1),
                               help="Horas calendario entre la entrada y la salida (o ahora).")
    hours_mp = fields.Float(string="Horas con materia prima pendiente", compute='_compute_hours', digits=(16, 1),
                            help="Parte de esas horas en que el proyecto esperaba materia prima.")
    hours_dev = fields.Float(string="Horas de desarrollo", compute='_compute_hours', digits=(16, 1),
                             help="Horas en la etapa sin contar la espera de materia prima.")

    @api.depends('date_start', 'date_end', 'project_id.sgi_dev_mp_wait_ids.date_start',
                 'project_id.sgi_dev_mp_wait_ids.date_end')
    def _compute_hours(self):
        now = fields.Datetime.now()
        for log in self:
            end = log.date_end or now
            total = max((end - log.date_start).total_seconds(), 0) / 3600.0 if log.date_start else 0.0
            mp = 0.0
            for wait in log.project_id.sgi_dev_mp_wait_ids:
                lo = max(wait.date_start, log.date_start)
                hi = min(wait.date_end or now, end)
                if hi > lo:
                    mp += (hi - lo).total_seconds() / 3600.0
            log.hours_total = total
            log.hours_mp = min(mp, total)
            log.hours_dev = total - min(mp, total)


class SgiDevMpWait(models.Model):
    """Periodo en que un desarrollo esperó materia prima: detiene el reloj de desarrollo."""
    _name = 'sgi.dev.mp.wait'
    _description = "Espera de materia prima del desarrollo"
    _order = 'date_start desc, id desc'

    project_id = fields.Many2one('project.project', required=True, ondelete='cascade', index=True,
                                 help="Proyecto de desarrollo.")
    date_start = fields.Datetime(string="Desde", required=True, default=fields.Datetime.now,
                                 help="Cuándo se detectó que faltaba materia prima.")
    date_end = fields.Datetime(string="Hasta", help="Cuándo llegó la materia prima; vacío mientras siga pendiente.")
    hours = fields.Float(string="Horas", compute='_compute_hours', digits=(16, 1),
                         help="Horas calendario de espera (hasta ahora si sigue pendiente).")
    note = fields.Char(string="Qué faltó", help="Materia prima que se esperaba (texto breve).")

    @api.depends('date_start', 'date_end')
    def _compute_hours(self):
        now = fields.Datetime.now()
        for wait in self:
            wait.hours = max(((wait.date_end or now) - wait.date_start).total_seconds(), 0) / 3600.0


class ProjectProjectDev(models.Model):
    _inherit = 'project.project'

    # --- Bandera, folio, nombre -------------------------------------------------
    sgi_ft_folio = fields.Char(string="Folio FT", index=True, copy=False, tracking=True,
                               help="Folio del desarrollo (FT-039-2026). Lo asigna la secuencia al pasar a "
                                    "«Muestra»; un folio histórico se puede capturar a mano.")
    sgi_dev_stage_key = fields.Char(string="Clave de la etapa", compute='_compute_sgi_dev_stage_key',
                                    help="Clave interna de la etapa de avance del desarrollo (solicitud, analisis…).")
    sgi_dev_stage_seq = fields.Integer(string="Orden de la etapa", compute='_compute_sgi_dev_stage_key',
                                       help="Posición de la etapa de avance; 0 si la etapa no es de desarrollo.")
    sgi_dev_product_name = fields.Char(string="Producto pedido", tracking=True,
                                       help="Cómo llama el cliente al producto mientras no hay código de artículo. "
                                            "Forma parte del nombre del proyecto.")
    sgi_dev_product_id = fields.Many2one('product.product', string="Artículo en desarrollo", tracking=True,
                                         help="Artículo generado para el desarrollo (lo crea el generador de código). "
                                              "Su referencia interna forma parte del nombre del proyecto.")
    sgi_dev_revision = fields.Integer(string="Revisión", default=0, tracking=True, copy=False,
                                      help="Revisión del desarrollo. Sube con «Subir revisión» cuando el cliente "
                                           "ajusta lo que pidió; cada cambio queda en la bitácora.")
    sgi_dev_revision_ids = fields.One2many('sgi.dev.revision', 'project_id', string="Bitácora de revisiones")
    # --- Origen ------------------------------------------------------------------
    sgi_dev_origin = fields.Selection([('cliente', "Cliente"), ('interno', "Interno")], string="Origen",
                                      default='cliente', tracking=True,
                                      help="Quién pide el desarrollo: un cliente o alguien de Dirección.")
    sgi_dev_contact_id = fields.Many2one('res.partner', string="Contacto del cliente",
                                         help="Persona del cliente que hizo la solicitud.")
    sgi_dev_requester_employee_id = fields.Many2one('hr.employee', string="Solicitante interno",
                                                    help="Quien pide el desarrollo cuando es interno.")
    # --- Comercial (sustituye al Análisis de mercado Industrial) ------------------
    sgi_dev_team_id = fields.Many2one('crm.team', string="Equipo de ventas",
                                      help="Equipo de ventas que atiende la solicitud (Industrial, Confección…).")
    sgi_dev_salesperson_id = fields.Many2one('res.users', string="Vendedor",
                                             help="Vendedor que atiende al cliente en este desarrollo.")
    sgi_dev_customer_status = fields.Selection([('actual', "Cliente actual"), ('prospecto', "Prospecto")],
                                               string="Cliente actual o prospecto", compute='_compute_sgi_dev_customer_status',
                                               help="Actual si el cliente tiene pedidos de venta confirmados; prospecto si no.")
    sgi_dev_program = fields.Char(string="Programa",
                                  help="Programa o plataforma del cliente para el que es el producto (nombre).")
    sgi_dev_application_id = fields.Many2one('sgi.dev.option', string="Aplicación",
                                             domain=[('kind', '=', 'aplicacion')],
                                             help="Para qué se va a usar el producto (lista).")
    sgi_dev_program_years = fields.Float(string="Tiempo de programa (años)", digits=(16, 1),
                                         help="Cuántos años durará el programa del cliente.")
    sgi_dev_consumption_annual = fields.Float(string="Consumo anual", digits=(16, 1),
                                              help="Consumo anual estimado, en la unidad elegida.")
    sgi_dev_consumption_uom = fields.Selection([('m', "m"), ('yd', "yd"), ('kg', "kg")], string="Unidad del consumo",
                                               default='m', help="Unidad del consumo anual: metros, yardas o kilogramos.")
    sgi_dev_consumption_monthly = fields.Float(string="Consumo mensual", compute='_compute_sgi_dev_consumption',
                                               store=True, digits=(16, 1),
                                               help="Consumo anual entre doce, en la misma unidad (calculado).")
    sgi_dev_consumption_annual_m = fields.Float(string="Consumo anual (m)", compute='_compute_sgi_dev_consumption',
                                                store=True, digits=(16, 1),
                                                help="Consumo anual en metros (las yardas se convierten; en kg queda vacío).")
    sgi_dev_consumption_annual_yd = fields.Float(string="Consumo anual (yd)", compute='_compute_sgi_dev_consumption',
                                                 store=True, digits=(16, 1),
                                                 help="Consumo anual en yardas (los metros se convierten; en kg queda vacío).")
    sgi_dev_lamination_id = fields.Many2one('sgi.dev.option', string="Tipo de laminado",
                                            domain=[('kind', '=', 'laminado')],
                                            help="Tipo de laminado que lleva el producto, si aplica (lista).")
    sgi_dev_legal_ids = fields.Many2many('sgi.dev.option', 'sgi_dev_project_legal_rel', 'project_id', 'option_id',
                                         string="Requisitos legales y reglamentarios",
                                         domain=[('kind', '=', 'requisito_legal')],
                                         help="Requisitos legales y reglamentarios que aplican (lista, no texto).")
    sgi_dev_market_id = fields.Many2one('sgi.dev.option', string="Mercado", domain=[('kind', '=', 'mercado')],
                                        help="Mercado al que va el producto (lista).")
    sgi_dev_required_date = fields.Date(string="Fecha requerida", tracking=True,
                                        help="Fecha en que el cliente necesita el producto o la muestra.")
    sgi_dev_spec_number = fields.Char(string="Número de especificación",
                                      help="Número o clave de la especificación del cliente.")
    # --- Muestra física -----------------------------------------------------------
    sgi_dev_sample_received = fields.Date(string="Muestra recibida", tracking=True,
                                          help="Fecha en que llegó la muestra física del cliente (a nombre de Ventas).")
    sgi_dev_sample_handed = fields.Date(string="Muestra entregada a Diseño", tracking=True,
                                        help="Fecha en que Ventas entregó la muestra a Diseño de Producto.")
    sgi_dev_sample_folder = fields.Char(string="Ubicación en carpeta",
                                        help="Carpeta y posición donde se guarda el recorte tamaño carta de la muestra.")
    # --- Resultado del análisis ----------------------------------------------------
    sgi_dev_analysis_result = fields.Selection([
        ('linea', "Producto de línea"), ('nuevo', "Producto nuevo"), ('no_factible', "No factible"),
    ], string="Resultado del análisis", tracking=True,
        help="Producto de línea: un artículo existente cumple todo, se cotiza ese y el proyecto cierra sin FT. "
             "Producto nuevo: sigue el desarrollo. No factible: cierra con motivo.")
    sgi_dev_base_product_id = fields.Many2one('product.product', string="Artículo de línea o base", tracking=True,
                                              help="Artículo existente que cumple la solicitud (producto de línea) o que "
                                                   "sirve de base al desarrollo nuevo.")
    sgi_dev_not_feasible_reason_id = fields.Many2one('sgi.dev.option', string="Motivo de no factibilidad",
                                                     domain=[('kind', '=', 'motivo_no_factible')],
                                                     help="Por qué no es factible (lista).")
    # --- Relojes -------------------------------------------------------------------
    sgi_dev_stage_log_ids = fields.One2many('sgi.dev.stage.log', 'project_id', string="Reloj por etapa")
    sgi_dev_mp_wait_ids = fields.One2many('sgi.dev.mp.wait', 'project_id', string="Esperas de materia prima")
    sgi_dev_mp_pending = fields.Boolean(string="Materia prima pendiente", compute='_compute_sgi_dev_clocks',
                                        help="Hay una espera de materia prima abierta: el reloj de desarrollo está detenido.")
    sgi_dev_hours_dev = fields.Float(string="Horas de desarrollo", compute='_compute_sgi_dev_clocks', digits=(16, 1),
                                     help="Horas calendario desde la solicitud sin contar las esperas de materia prima.")
    sgi_dev_hours_mp = fields.Float(string="Horas de materia prima", compute='_compute_sgi_dev_clocks', digits=(16, 1),
                                    help="Horas calendario acumuladas esperando materia prima.")
    sgi_dev_hours_stage = fields.Float(string="Horas en la etapa actual", compute='_compute_sgi_dev_clocks',
                                       digits=(16, 1), help="Horas de desarrollo en la etapa en la que está el proyecto.")

    # ------------------------------------------------------------------------
    # Etapas
    # ------------------------------------------------------------------------
    @api.model
    def _sgi_dev_stage(self, key):
        return self.env.ref('quimibond_sgi.sgi_dev_stage_%s' % key, raise_if_not_found=False)

    @api.model
    def _sgi_dev_stage_keys(self):
        """{stage_id: (clave, orden)} de las etapas de avance del desarrollo."""
        out = {}
        for i, (key, _label) in enumerate(DEV_STAGES, start=1):
            stage = self._sgi_dev_stage(key)
            if stage:
                out[stage.id] = (key, i)
        return out

    @api.depends('stage_id')
    def _compute_sgi_dev_stage_key(self):
        keys = self._sgi_dev_stage_keys()
        for project in self:
            key, seq = keys.get(project.stage_id.id, ('', 0))
            project.sgi_dev_stage_key = key
            project.sgi_dev_stage_seq = seq

    @api.model
    def _sgi_dev_stage_order(self, key):
        return next((i for i, (k, _l) in enumerate(DEV_STAGES, start=1) if k == key), 0)

    # ------------------------------------------------------------------------
    # Cálculos
    # ------------------------------------------------------------------------
    @api.depends('partner_id')
    def _compute_sgi_dev_customer_status(self):
        Sale = self.env['sale.order'].sudo()
        for project in self:
            if not project.partner_id:
                project.sgi_dev_customer_status = False
                continue
            commercial = project.partner_id.commercial_partner_id
            has_sales = Sale.search_count([('partner_id', 'child_of', commercial.id),
                                           ('state', '=', 'sale')], limit=1)
            project.sgi_dev_customer_status = 'actual' if has_sales else 'prospecto'

    @api.depends('sgi_dev_consumption_annual', 'sgi_dev_consumption_uom')
    def _compute_sgi_dev_consumption(self):
        for project in self:
            annual, uom = project.sgi_dev_consumption_annual, project.sgi_dev_consumption_uom
            project.sgi_dev_consumption_monthly = annual / 12.0 if annual else 0.0
            project.sgi_dev_consumption_annual_m = (annual if uom == 'm' else annual * YARD_M if uom == 'yd' else 0.0)
            project.sgi_dev_consumption_annual_yd = (annual if uom == 'yd' else annual / YARD_M if uom == 'm' else 0.0)

    @api.depends('sgi_dev_stage_log_ids.date_start', 'sgi_dev_stage_log_ids.date_end',
                 'sgi_dev_mp_wait_ids.date_start', 'sgi_dev_mp_wait_ids.date_end')
    def _compute_sgi_dev_clocks(self):
        for project in self:
            logs = project.sgi_dev_stage_log_ids
            project.sgi_dev_mp_pending = any(not w.date_end for w in project.sgi_dev_mp_wait_ids)
            project.sgi_dev_hours_dev = sum(logs.mapped('hours_dev'))
            project.sgi_dev_hours_mp = sum(project.sgi_dev_mp_wait_ids.mapped('hours'))
            current = logs.filtered(lambda l: not l.date_end)[:1]
            project.sgi_dev_hours_stage = current.hours_dev if current else 0.0

    # ------------------------------------------------------------------------
    # Nombre, folio, alias
    # ------------------------------------------------------------------------
    def _sgi_dev_name(self):
        self.ensure_one()
        parts = [self.sgi_ft_folio or "Análisis"]
        code = self.sgi_dev_product_id.default_code or self.sgi_dev_product_name
        if code:
            parts.append(code)
        elif self.partner_id and not self.sgi_ft_folio:
            parts.append(self.partner_id.commercial_partner_id.name or self.partner_id.name)
        if self.sgi_dev_revision:
            parts.append("rev. %d" % self.sgi_dev_revision)
        return " ".join(parts)

    def _sgi_dev_sync_name(self):
        for project in self.filtered('sgi_is_ft'):
            name = project._sgi_dev_name()
            if project.name != name:
                super(ProjectProjectDev, project).write({'name': name})

    def action_sgi_dev_assign_folio(self):
        """Asigna el folio FT de la secuencia anual (FT-001-2027) si no lo tiene."""
        for project in self.filtered(lambda p: p.sgi_is_ft and not p.sgi_ft_folio):
            folio = self.env['ir.sequence'].with_company(project.company_id or self.env.company).next_by_code('sgi.dev.ft')
            if not folio:
                raise UserError("No existe la secuencia de folios FT (sgi.dev.ft). Actualice el módulo SGI.")
            project.write({'sgi_ft_folio': folio})
        return True

    def _sgi_dev_alias_name(self):
        self.ensure_one()
        if self.sgi_ft_folio:
            return re.sub(r'[^a-z0-9]+', '-', self.sgi_ft_folio.lower()).strip('-')
        return 'desarrollo-%d' % self.id

    def _sgi_dev_sync_alias(self):
        """Nombra el alias nativo del proyecto (desarrollo-<id> o ft-039-2026) para que los
        correos del cliente queden pegados. Sin dominio de alias configurado el alias sigue
        inactivo, sin error."""
        for project in self.filtered('sgi_is_ft'):
            wanted = project._sgi_dev_alias_name()
            if project.alias_id and project.alias_name != wanted:
                try:
                    with self.env.cr.savepoint():
                        project.alias_name = wanted
                except Exception:  # noqa: BLE001 — un alias en uso no detiene el desarrollo
                    continue

    # ------------------------------------------------------------------------
    # Revisión y bitácora
    # ------------------------------------------------------------------------
    def _sgi_dev_logs_revisions(self):
        self.ensure_one()
        return self.sgi_is_ft and self.sgi_dev_stage_seq >= self._sgi_dev_stage_order(REVISION_FROM_STAGE)

    def action_sgi_dev_new_revision(self):
        """Sube la revisión del desarrollo (el cliente ajustó lo que pidió) y lo anota en la bitácora."""
        for project in self.filtered('sgi_is_ft'):
            project.write({'sgi_dev_revision': project.sgi_dev_revision + 1})
            self.env['sgi.dev.revision'].create({
                'project_id': project.id, 'revision': project.sgi_dev_revision,
                'note': "Revisión %d abierta" % project.sgi_dev_revision,
            })
        return True

    def _sgi_dev_log_revision(self, line, old_label, new_label):
        self.ensure_one()
        self.env['sgi.dev.revision'].sudo().create({
            'project_id': self.id, 'revision': self.sgi_dev_revision, 'user_id': self.env.uid,
            'characteristic_id': line.id, 'characteristic_name': line.name,
            'old_value': old_label, 'new_value': new_label,
        })

    # ------------------------------------------------------------------------
    # Relojes
    # ------------------------------------------------------------------------
    def _sgi_dev_open_stage_log(self, when=None):
        """Cierra el reloj de la etapa anterior y abre el de la actual."""
        Log = self.env['sgi.dev.stage.log'].sudo()
        when = when or fields.Datetime.now()
        for project in self.filtered(lambda p: p.sgi_is_ft and p.stage_id):
            open_logs = project.sgi_dev_stage_log_ids.filtered(lambda l: not l.date_end)
            if open_logs and open_logs[:1].stage_id == project.stage_id and len(open_logs) == 1:
                continue
            open_logs.write({'date_end': when})
            Log.create({'project_id': project.id, 'stage_id': project.stage_id.id, 'date_start': when})

    def action_sgi_dev_mp_wait_start(self):
        for project in self.filtered(lambda p: p.sgi_is_ft and not p.sgi_dev_mp_pending):
            self.env['sgi.dev.mp.wait'].create({'project_id': project.id})
        return True

    def action_sgi_dev_mp_wait_stop(self):
        for project in self.filtered('sgi_is_ft'):
            project.sgi_dev_mp_wait_ids.filtered(lambda w: not w.date_end).write({'date_end': fields.Datetime.now()})
        return True

    def action_sgi_dev_print_sample_label(self):
        self.ensure_one()
        return self.env.ref('quimibond_sgi.action_report_dev_sample_label').report_action(self)

    # ------------------------------------------------------------------------
    # Resultado del análisis
    # ------------------------------------------------------------------------
    def action_sgi_dev_close_as_line_product(self):
        """Un artículo existente cumple todo: se cotiza ese y el proyecto cierra sin FT."""
        for project in self.filtered('sgi_is_ft'):
            if not project.sgi_dev_base_product_id:
                raise UserError("Indique el artículo de línea que cumple la solicitud antes de cerrar.")
            vals = {'sgi_dev_analysis_result': 'linea'}
            stage = self._sgi_dev_stage('cerrado_sin_producto')
            if stage:
                vals['stage_id'] = stage.id
            project.write(vals)
        return True

    def action_sgi_dev_close_not_feasible(self):
        for project in self.filtered('sgi_is_ft'):
            if not project.sgi_dev_not_feasible_reason_id:
                raise UserError("Indique el motivo de no factibilidad antes de cerrar.")
            vals = {'sgi_dev_analysis_result': 'no_factible'}
            stage = self._sgi_dev_stage('cerrado_sin_producto')
            if stage:
                vals['stage_id'] = stage.id
            project.write(vals)
        return True

    # ------------------------------------------------------------------------
    # Ciclo de vida: create / write
    # ------------------------------------------------------------------------
    @api.model_create_multi
    def create(self, vals_list):
        projects = super().create(vals_list)
        dev = projects.filtered('sgi_is_ft')
        if dev:
            solicitud = self._sgi_dev_stage('solicitud')
            for project in dev:
                if solicitud and project.stage_id.id not in self._sgi_dev_stage_keys():
                    super(ProjectProjectDev, project).write({'stage_id': solicitud.id})
            dev._sgi_dev_sync_name()
            dev._sgi_dev_sync_alias()
            dev._sgi_dev_open_stage_log()
        return projects

    def write(self, vals):
        res = super().write(vals)
        dev = self.filtered('sgi_is_ft')
        if not dev:
            return res
        if 'stage_id' in vals:
            seq = self._sgi_dev_stage_order(FOLIO_FROM_STAGE)
            dev.filtered(lambda p: p.sgi_dev_stage_seq >= seq and not p.sgi_ft_folio
                         and p.sgi_dev_stage_key != 'cerrado_sin_producto'
                         and p.sgi_dev_analysis_result not in ('linea', 'no_factible')).action_sgi_dev_assign_folio()
            dev._sgi_dev_open_stage_log()
        if vals.keys() & {'sgi_ft_folio', 'sgi_dev_product_name', 'sgi_dev_product_id', 'sgi_dev_revision',
                          'partner_id', 'sgi_is_ft'}:
            dev._sgi_dev_sync_name()
        if vals.keys() & {'sgi_ft_folio', 'sgi_is_ft'}:
            dev._sgi_dev_sync_alias()
        if 'sgi_is_ft' in vals and not any(p.sgi_dev_stage_log_ids for p in dev):
            dev._sgi_dev_open_stage_log()
        return res

    # ------------------------------------------------------------------------
    # Migración 57.118.0: proyectos FT- viejos, plantillas y filtros de medición
    # ------------------------------------------------------------------------
    @api.model
    def _sgi_dev_parse_legacy_name(self, name):
        """'FT-043/2024 WK140Q48JNT165 REV3' → ('FT-043-2024', 'WK140Q48JNT165', 3).
        None si el nombre no empieza con FT-."""
        m = FT_FOLIO_RE.match(name or '')
        if not m:
            return None
        folio = "FT-%03d-%s" % (int(m.group(1)), m.group(2))
        rest = m.group(3).strip()
        revision = 0
        r = REV_RE.search(rest)
        if r:
            revision = int(r.group(1))
            rest = rest[:r.start()].strip()
        return folio, rest, revision

    @api.model
    def _sgi_dev_migrate_legacy(self):
        """Marca como desarrollo los proyectos FT-, las plantillas de Diseño y Desarrollo y el
        proyecto de análisis; separa folio, producto y revisión del nombre; toma el cliente de
        la etapa cuando faltaba. Idempotente."""
        Project = self.sudo().with_context(active_test=False)
        Partner = self.env['res.partner'].sudo()
        touched = Project.browse()
        for project in Project.search([('name', '=ilike', 'FT-%')]):
            parsed = project._sgi_dev_parse_legacy_name(project.name)
            vals = {'sgi_is_ft': True}
            if parsed and not project.sgi_ft_folio:
                folio, rest, revision = parsed
                vals.update({'sgi_ft_folio': folio, 'sgi_dev_revision': revision})
                if rest and not project.sgi_dev_product_name:
                    vals['sgi_dev_product_name'] = rest
            if not project.partner_id and project.stage_id and project.stage_id.id not in self._sgi_dev_stage_keys():
                partner = Partner.search([('is_company', '=', True), ('name', '=ilike', project.stage_id.name)],
                                         limit=1)
                if partner:
                    vals['partner_id'] = partner.id
            project.write(vals)
            touched |= project
        templates = Project.search(['|', ('name', '=ilike', 'PLANTILLA - Diseño y Desarrollo%'),
                                    ('name', '=ilike', 'ANALISIS DE PROYECTO%'), ('sgi_is_ft', '=', False)])
        templates.write({'sgi_is_ft': True})
        return touched | templates

    @api.model
    def _sgi_dev_migrate_measure_domains(self):
        """Los filtros de medición que decían «nombre empieza con FT-» pasan a la bandera."""
        changed = 0
        for model, field in (('sgi.deliverable', 'measure_domain'), ('sgi.process.activity', 'measure_domain'),
                             ('sgi.process.activity', 'applies_domain')):
            if model not in self.env or field not in self.env[model]._fields:
                continue
            records = self.env[model].sudo().with_context(active_test=False).search([(field, 'ilike', 'FT-%')])
            for rec in records:
                text = rec[field] or ''
                new = text
                for old, repl in LEGACY_DOMAIN_REPLACEMENTS:
                    new = new.replace(old, repl)
                if new != text:
                    rec.write({field: new})
                    changed += 1
        return changed

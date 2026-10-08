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
import logging
import re

from odoo import api, fields, models
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

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
    ('motivo_ajuste', "Motivo de ajuste de parámetro de proceso"),  # 57.126.0 (C1, bloque G)
    ('paqueteria', "Paquetería"),  # 57.129.0 (Jose 3.3): medio de envío de la muestra
    ('motivo_rechazo_cliente', "Motivo de rechazo del cliente"),  # 57.129.0 (Jose 3.3)
    ('cuidado', "Instrucción de cuidado"),  # 57.132.0 (Jose 5.3): especificaciones del producto
]
# Etapas viejas que no son clientes: no se adivina cliente desde ellas.
NON_CUSTOMER_STAGES = {'hecha', 'cancelada', 'odoo', 'nuevo', 'por hacer', 'analisis de proyectos',
                       'análisis de proyectos'}
# Etapa vieja que significa desarrollo interno (quien pide es Dirección).
INTERNAL_STAGES = {'quimibond'}
# Etapas viejas → etapa de avance (57.120.2). Un proyecto con folio FT ya pasó
# la aprobación del cliente: lo que no esté cancelado ni hecho va a «Muestra».
LEGACY_STAGE_MAP = {'cancelada': 'cerrado_sin_producto', 'hecha': 'liberado',
                    'analisis de proyectos': 'analisis', 'análisis de proyectos': 'analisis'}
LEGACY_STAGE_DEFAULT = 'muestra'
# Dominios de medición viejos → nuevos (migración y mapa de procesos). Las
# plantillas no cuentan (Jose, 2026-10-06).
LEGACY_DOMAIN_REPLACEMENTS = (
    ("('project_id.name', '=like', 'FT-%')",
     "('project_id.sgi_is_ft', '=', True), ('project_id.is_template', '=', False)"),
    ("('name', '=like', 'FT-%')", "('sgi_is_ft', '=', True), ('is_template', '=', False)"),
    ("('project_id.sgi_is_ft', '=', True)]",
     "('project_id.sgi_is_ft', '=', True), ('project_id.is_template', '=', False)]"),
    ("('project_id.sgi_is_ft', '=', True),",
     "('project_id.sgi_is_ft', '=', True), ('project_id.is_template', '=', False),"),
    ("[('sgi_is_ft', '=', True)]", "[('sgi_is_ft', '=', True), ('is_template', '=', False)]"),
)
# Modelos nuevos que el MCP debe poder leer (y los catálogos, escribir) para
# la corrección final de datos del brief (sección 7). Jose, 2026-10-06.
MCP_MODELS = {
    'sgi.dev.characteristic': True, 'sgi.dev.characteristic.template': True, 'sgi.dev.option': True,
    'sgi.dev.revision': True, 'sgi.dev.stage.log': False, 'sgi.dev.mp.wait': True,
    'sgi.dev.lab.request': True, 'sgi.dev.feasibility.item': True, 'sgi.dev.feasibility': True,
    'ficha.tecnica.caracteristica': True, 'ficha.tecnica.clave.codigo': True, 'ficha.tecnica.spec': True,
    'ficha.tecnica.tejido': True, 'ficha.tecnica.acabado': True, 'project.project.stage': False,
    'sgi.dev.similar': False, 'sgi.dev.similar.line': False,  # 57.122.0: asistente, solo lectura
    'sgi.machine.sheet': True, 'sgi.machine.sheet.param': True,  # 57.126.0: ficha de tejido con real / ajuste
    'sgi.dev.shipment': True, 'sgi.dev.shipment.roll': True,  # 57.129.0: envío de muestra y respuesta del cliente
    'sgi.dev.coa': True, 'sgi.dev.coa.line': True,  # 57.130.0: reporte de conformidad desde la tabla
    'sgi.dev.tech.sheet': True, 'sgi.dev.tech.sheet.line': True, 'sgi.dev.tech.sheet.sign': False,  # 57.132.0
    'sgi.dev.customer.spec': True, 'sgi.dev.customer.spec.line': True,  # 57.132.0
    'sgi.dev.pilot': True, 'sgi.dev.pilot.lot': True, 'sgi.dev.pilot.reading': True,  # 57.133.0
    'sgi.dev.pilot.study': False,  # 57.133.0: lo calcula el pilotaje
    'sgi.dev.escalation': False,  # 57.135.0: lo escribe el cron de avisos por tiempo
    'sgi.dev.change.request': True,  # 57.137.0: solicitud de modificación del proyecto
}


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
    # 57.127.0 (Jose, 1b): quién movió el proyecto a la etapa; con esto se miden C1.13 y C1.14.
    user_id = fields.Many2one('res.users', string="Lo pasó a la etapa", readonly=True, index=True,
                              help="Usuario que movió el proyecto a esta etapa.")
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

    def _sgi_dev_names_to_sync(self):
        """Proyectos cuyo nombre arma Odoo: desarrollos con folio o con producto.
        Las plantillas y los análisis sin producto conservan el nombre que
        escribió la persona (57.120.2: la migración renombró «Análisis» las
        plantillas 480 y 481 y el proyecto 490)."""
        return self.filtered(lambda p: p.sgi_is_ft and not p.is_template
                             and (p.sgi_ft_folio or p.sgi_dev_product_id or p.sgi_dev_product_name))

    @api.model
    def _sgi_dev_langs(self):
        return [code for code, _name in self.env['res.lang'].get_installed()]

    @api.model
    def _sgi_dev_lang_names(self, record, field='name'):
        """Valores de un campo traducible en todos los idiomas instalados, sin repetir y con el
        de la compañía primero. 57.120.3: la migración corría sin idioma (en_US) y los nombres
        que la gente escribió («FT-012-2025 …», «Cancelada», «Coordinador de Laboratorio y MP»)
        viven en es_MX; en en_US quedó el nombre con el que se creó el registro."""
        if not record:
            return []
        langs = self._sgi_dev_langs()
        company_lang = self.env.company.partner_id.lang
        if company_lang in langs:
            langs = [company_lang] + [lang for lang in langs if lang != company_lang]
        out = []
        for lang in langs:
            value = record.with_context(lang=lang)[field]
            if value and value not in out:
                out.append(value)
        return out

    @api.model
    def _sgi_dev_lang_keys(self, record, field='name'):
        """Los mismos valores, en minúsculas y sin espacios, para comparar con las etapas viejas."""
        return {value.strip().lower() for value in self._sgi_dev_lang_names(record, field)}

    @api.model
    def _sgi_dev_search_langs(self, model, domain):
        """``search(domain)`` en cada idioma instalado, unidos: un dominio sobre un campo
        traducible solo mira el idioma del contexto (sin idioma, en_US)."""
        Model = self.env[model].sudo().with_context(active_test=False)
        found = Model.browse()
        for lang in self._sgi_dev_langs():
            found |= Model.with_context(lang=lang).search(domain)
        return found

    @api.model
    def _sgi_dev_legacy_ft_projects(self):
        """Proyectos cuyo nombre empieza con FT- en cualquier idioma."""
        return self._sgi_dev_search_langs('project.project', [('name', '=ilike', 'FT-%')])

    @api.model
    def _sgi_dev_legacy_ft_name(self, project):
        """El nombre «FT-…» del proyecto (en el idioma donde lo tenga); '' si no lo tiene."""
        for name in self._sgi_dev_lang_names(project):
            if FT_FOLIO_RE.match(name):
                return name
        return ''

    def _sgi_dev_write_name_all_langs(self, name):
        """Escribe el nombre en todos los idiomas instalados. ``name`` es
        traducible: un write sin ``lang`` solo cambia en_US y los usuarios en
        es_MX siguen viendo el nombre viejo (57.120.2)."""
        for project in self:
            langs = self._sgi_dev_langs()
            if len(langs) > 1:
                project.update_field_translations('name', {lang: name for lang in langs})
            else:
                super(ProjectProjectDev, project).write({'name': name})

    def _sgi_dev_sync_name(self):
        for project in self._sgi_dev_names_to_sync():
            name = project._sgi_dev_name()
            langs = self._sgi_dev_langs()
            if any(project.with_context(lang=lang).name != name for lang in langs):
                project._sgi_dev_write_name_all_langs(name)

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
        correos del cliente queden pegados. Solo si la base tiene dominio de alias; el
        savepoint cubre únicamente la escritura del alias (``flush=False``): en 57.118.0 el
        savepoint con flush previo tragó en silencio la escritura de los proyectos
        (57.120.2)."""
        if not self.env['mail.alias.domain'].sudo().search_count([]):
            return
        for project in self.filtered('sgi_is_ft'):
            wanted = project._sgi_dev_alias_name()
            alias = project.alias_id.sudo()
            if not alias or alias.alias_name == wanted:
                continue
            try:
                with self.env.cr.savepoint(flush=False):
                    alias.write({'alias_name': wanted})
                    alias.flush_recordset(['alias_name'])
            except Exception as exc:  # noqa: BLE001 — un alias en uso no detiene el desarrollo
                alias.invalidate_recordset(['alias_name'])
                _logger.warning("SGI desarrollo: no se pudo nombrar el alias %s del proyecto %s: %s",
                                wanted, project.id, exc)

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
            Log.create({'project_id': project.id, 'stage_id': project.stage_id.id, 'date_start': when,
                        'user_id': self.env.uid})

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
        if dev and not self.env.context.get('sgi_dev_migration'):
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
        if not dev or self.env.context.get('sgi_dev_migration'):
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
    def _sgi_dev_stage_partner_map(self):
        """{etapa vieja: (cliente, regla)} para los proyectos FT- sin cliente.

        Regla «uso»: el cliente que más proyectos FT de esa etapa ya tienen
        (así SHAWMUT da SHAWMUT LLC y LA DIFERENCE da LA DIFFERENCE, aunque el
        nombre no coincida). Regla «nombre»: si ningún proyecto de la etapa
        tiene cliente, la única empresa cuyo nombre contiene el de la etapa.
        Las etapas que no son clientes (Hecha, Cancelada, Odoo…) y QUIMIBOND
        (origen interno) no entran."""
        Project = self.sudo().with_context(active_test=False)
        Partner = self.env['res.partner'].sudo()
        dev_stage_ids = set(self._sgi_dev_stage_keys())
        projects = Project.browse(self._sgi_dev_legacy_ft_projects().ids)
        out = {}
        for stage in projects.mapped('stage_id'):
            keys = self._sgi_dev_lang_keys(stage)
            if stage.id in dev_stage_ids or keys & NON_CUSTOMER_STAGES or keys & INTERNAL_STAGES:
                continue
            same = projects.filtered(lambda p: p.stage_id == stage and p.partner_id)
            counts = {}
            for p in same:
                partner = p.partner_id.commercial_partner_id
                counts[partner] = counts.get(partner, 0) + 1
            if counts:
                partner = max(counts, key=lambda k: (counts[k], -k.id))
                out[stage] = (partner, 'uso (%d proyecto%s)' % (counts[partner], 's' if counts[partner] != 1 else ''))
                continue
            found = Partner
            for stage_name in self._sgi_dev_lang_names(stage):
                found = Partner.search([('is_company', '=', True), ('name', 'ilike', stage_name)])
                if found:
                    break
            if len(found) == 1:
                out[stage] = (found, 'nombre')
            else:
                out[stage] = (Partner, 'sin cliente en Odoo' if not found else 'nombre ambiguo (%d)' % len(found))
        return out

    @api.model
    def _sgi_dev_migration_preview(self):
        """Tabla etapa → cliente que aplicaría la migración, con los proyectos sin cliente de cada etapa."""
        Project = self.sudo().with_context(active_test=False)
        projects = Project.browse(self._sgi_dev_legacy_ft_projects().ids).filtered(lambda p: not p.partner_id)
        mapping = self._sgi_dev_stage_partner_map()
        rows = []
        for stage in projects.mapped('stage_id').sorted('sequence'):
            keys = self._sgi_dev_lang_keys(stage)
            pending = projects.filtered(lambda p: p.stage_id == stage)
            if keys & INTERNAL_STAGES:
                partner, rule = self.env['res.partner'], 'origen interno'
            elif keys & NON_CUSTOMER_STAGES or stage.id in set(self._sgi_dev_stage_keys()):
                partner, rule = self.env['res.partner'], 'no es cliente'
            else:
                partner, rule = mapping.get(stage, (self.env['res.partner'], 'sin cliente en Odoo'))
            rows.append({'stage': stage, 'partner': partner, 'rule': rule, 'projects': pending,
                         'target': self._sgi_dev_legacy_stage_target(stage)})
        return rows

    @api.model
    def _sgi_dev_migrate_legacy(self):
        """Marca como desarrollo los proyectos FT-, las plantillas de Diseño y Desarrollo y el
        proyecto de análisis; separa folio, producto y revisión del nombre; toma el cliente de
        la etapa cuando faltaba (ver _sgi_dev_stage_partner_map) y marca origen interno a los
        de la etapa QUIMIBOND. Idempotente.

        57.120.2: corre con ``sgi_dev_migration`` (los ganchos de write no hacen nada), escribe
        el nombre en todos los idiomas y hace ``flush`` al final para que un error se vea en el
        update en lugar de perderse.

        57.120.3: busca los nombres en todos los idiomas instalados. Los proyectos se crearon
        como «Análisis de proyecto …» y se renombraron «FT-…» en es_MX; en en_US (el idioma de
        una migración sin contexto) siguen con el nombre original, así que 57.118.0 y 57.120.2
        no encontraron ninguno."""
        Project = self.sudo().with_context(active_test=False, sgi_dev_migration=True)
        mapping = self._sgi_dev_stage_partner_map()
        touched = Project.browse()
        for project in Project.browse(self._sgi_dev_legacy_ft_projects().ids):
            parsed = project._sgi_dev_parse_legacy_name(self._sgi_dev_legacy_ft_name(project))
            vals = {'sgi_is_ft': True}
            if parsed and not project.sgi_ft_folio:
                folio, rest, revision = parsed
                vals.update({'sgi_ft_folio': folio, 'sgi_dev_revision': revision})
                if rest and not project.sgi_dev_product_name:
                    vals['sgi_dev_product_name'] = rest
            if self._sgi_dev_lang_keys(project.stage_id) & INTERNAL_STAGES:
                vals['sgi_dev_origin'] = 'interno'
            elif not project.partner_id and project.stage_id in mapping and mapping[project.stage_id][0]:
                vals['partner_id'] = mapping[project.stage_id][0].id
            project.write(vals)
            touched |= project
        templates = Project.browse(self._sgi_dev_search_langs('project.project', [
            '|', ('name', '=ilike', 'PLANTILLA - Diseño y Desarrollo%'),
            ('name', '=ilike', 'ANALISIS DE PROYECTO%'), ('sgi_is_ft', '=', False)]).ids)
        templates.write({'sgi_is_ft': True})
        touched |= templates
        touched._sgi_dev_unify_names()
        Project.flush_model()
        return touched

    def _sgi_dev_unify_names(self):
        """Un solo nombre en todos los idiomas. Desarrollos con folio o producto: el nombre que
        arma Odoo. Plantillas y análisis sin producto: el nombre que ve la empresa (es_MX en
        Quimibond), que es el que escribió la persona; así se deshace el «Análisis» que 57.118.0
        dejó en en_US en las plantillas 480 y 481 y en el proyecto 490."""
        langs = self._sgi_dev_langs()
        if len(langs) <= 1:
            self._sgi_dev_names_to_sync()._sgi_dev_sync_name()
            return
        company_lang = self.env.company.partner_id.lang or 'en_US'
        placeholder = "Análisis"
        for project in self.filtered('sgi_is_ft'):
            if project in project._sgi_dev_names_to_sync():
                name = project._sgi_dev_name()
            else:
                values = {lang: project.with_context(lang=lang).name or '' for lang in langs}
                good = [v for v in values.values() if v and v != placeholder and not v.startswith(placeholder + ' ')]
                name = values.get(company_lang) if values.get(company_lang) in good else (good[0] if good else '')
            if name and any(project.with_context(lang=lang).name != name for lang in langs):
                project._sgi_dev_write_name_all_langs(name)

    @api.model
    def _sgi_dev_legacy_stage_target(self, stage, project=None):
        """Clave de la etapa de avance a la que pasa un proyecto FT que está en una etapa vieja.
        El nombre de la etapa se compara en todos los idiomas. Un proyecto sin folio FT en una
        etapa de cliente (un «Análisis de proyecto …» que alguien arrastró a la columna del
        cliente, como el 491 en producción) va a «Análisis», no a «Muestra»: el folio nace al
        llegar a Muestra y ese proyecto nunca lo tuvo."""
        for key in self._sgi_dev_lang_keys(stage):
            if key in LEGACY_STAGE_MAP:
                return LEGACY_STAGE_MAP[key]
        if project is not None and not project.sgi_ft_folio and not self._sgi_dev_legacy_ft_name(project):
            return 'analisis'
        return LEGACY_STAGE_DEFAULT

    @api.model
    def _sgi_dev_migrate_stages(self):
        """Pasa los desarrollos de las etapas con nombre de cliente a las etapas de avance
        (Cancelada → Cerrado sin producto, Hecha → Liberado, ANALISIS DE PROYECTOS → Análisis,
        lo demás → Muestra), abre su reloj de etapa y archiva las etapas viejas que quedan sin
        proyectos. Las plantillas no se mueven. Idempotente."""
        Project = self.sudo().with_context(active_test=False, sgi_dev_migration=True)
        Stage = self.env['project.project.stage'].sudo().with_context(active_test=False)
        dev_stage_ids = set(self._sgi_dev_stage_keys())
        moved = Project.browse()
        old_stages = Stage.browse()
        for project in Project.search([('sgi_is_ft', '=', True), ('is_template', '=', False)]):
            if not project.stage_id or project.stage_id.id in dev_stage_ids:
                continue
            target = self._sgi_dev_stage(self._sgi_dev_legacy_stage_target(project.stage_id, project))
            if not target:
                continue
            old_stages |= project.stage_id
            project.write({'stage_id': target.id})
            moved |= project
        # 57.120.3: lo que 57.120.2 dejó en «Muestra» sin folio ni nombre FT- (un análisis que
        # estaba en la columna de un cliente) regresa a «Análisis». Un desarrollo real recibe su
        # folio al entrar a Muestra, salvo que el resultado del análisis sea línea o no factible.
        muestra, analisis = self._sgi_dev_stage('muestra'), self._sgi_dev_stage('analisis')
        if muestra and analisis:
            for project in Project.search([('sgi_is_ft', '=', True), ('is_template', '=', False),
                                           ('stage_id', '=', muestra.id), ('sgi_ft_folio', '=', False),
                                           ('sgi_dev_analysis_result', 'not in', ('linea', 'no_factible'))]):
                if self._sgi_dev_legacy_ft_name(project):
                    continue
                project.write({'stage_id': analisis.id})
                project.sgi_dev_stage_log_ids.filtered(
                    lambda l: not l.date_end and l.stage_id == muestra).write({'stage_id': analisis.id})
                moved |= project
        moved.with_context(sgi_dev_migration=False)._sgi_dev_open_stage_log()
        archived = Stage.browse()
        if 'active' in Stage._fields:
            for stage in old_stages:
                keys = self._sgi_dev_lang_keys(stage)
                if keys & NON_CUSTOMER_STAGES or keys & INTERNAL_STAGES:
                    if not keys & {'analisis de proyectos', 'análisis de proyectos', 'quimibond'}:
                        continue
                if not Project.search_count([('stage_id', '=', stage.id)]):
                    stage.write({'active': False})
                    archived |= stage
        Project.flush_model()
        return moved, archived

    @api.model
    def _sgi_dev_enable_mcp_models(self):
        """Expone al MCP los modelos nuevos del desarrollo y de las fichas (lectura; escritura en
        los catálogos y renglones). Solo si mcp_server está instalado. Idempotente."""
        if 'mcp.enabled.model' not in self.env:
            return self.env['ir.model']
        Enabled = self.env['mcp.enabled.model'].sudo().with_context(active_test=False)
        IrModel = self.env['ir.model'].sudo()
        done = IrModel
        for model_name, writable in MCP_MODELS.items():
            if model_name not in self.env:
                continue
            model = IrModel.search([('model', '=', model_name)], limit=1)
            if not model or Enabled.search_count([('model_id', '=', model.id)]):
                continue
            Enabled.create({'model_id': model.id, 'active': True, 'allow_read': True,
                            'allow_create': writable, 'allow_write': writable, 'allow_unlink': False,
                            'notes': "quimibond_sgi 57.120.2: modelo del desarrollo de producto (C1)."})
            done |= model
        return done

    @api.model
    def _sgi_dev_migrate_measure_domains(self):
        """Los filtros de medición que decían «nombre empieza con FT-» pasan a la bandera y
        excluyen las plantillas (57.120.2). Idempotente: no duplica la cláusula."""
        changed = 0
        for model, field in (('sgi.deliverable', 'measure_domain'), ('sgi.process.activity', 'measure_domain'),
                             ('sgi.process.activity', 'applies_domain')):
            if model not in self.env or field not in self.env[model]._fields:
                continue
            records = self.env[model].sudo().with_context(active_test=False).search(
                ['|', (field, 'ilike', 'FT-%'), (field, 'ilike', 'sgi_is_ft')])
            for rec in records:
                text = rec[field] or ''
                new = self._sgi_dev_rewrite_domain(text)
                if new != text:
                    rec.write({field: new})
                    changed += 1
        return changed

    @api.model
    def _sgi_dev_rewrite_domain(self, text):
        """Texto de un dominio viejo → nuevo (bandera + sin plantillas), sin duplicar."""
        new = text
        for old, repl in LEGACY_DOMAIN_REPLACEMENTS[:2]:
            new = new.replace(old, repl)
        if 'is_template' not in new:
            for old, repl in LEGACY_DOMAIN_REPLACEMENTS[2:]:
                if old in new:
                    new = new.replace(old, repl, 1)
                    break
        return new

# -*- coding: utf-8 -*-
"""Artículo en desarrollo y generador de código (57.119.0, C1 bloque 3; brief 6.6).

- **Estado del artículo** (`product.template.sgi_dev_state`): en desarrollo
  (no se vende: solo proyecto, cotización y órdenes de muestra) → en
  pilotaje (se vende con aviso; entra cuando el cliente aprueba la muestra)
  → liberado → archivado (automático si el proyecto cierra sin producto;
  archiva también sus listas de materiales). El proyecto lo mueve al cambiar
  de etapa.
- **Generador de código** según la regla de codificación (DAT P-D02-01,
  claves en ``ficha.tecnica.clave.codigo``): con las claves elegidas y la
  masa, la galga y el ancho de la tabla de características, Odoo arma el
  código de 14 a 16 posiciones y crea los artículos de la ruta: crudo (H,
  kg), teñido (I, kg, si lleva teñido) y acabado (J, m). El crudo y el
  teñido llevan el ancho de tela cruda; el crudo va en color natural salvo
  que el hilo sea preteñido o reciclado. Si el código ya existe se liga el
  artículo existente en lugar de duplicarlo.
- **Bloqueo del artículo genérico de muestra** («MUESTRA PILOTO …», 16292 y
  16293): a partir de la fecha del parámetro
  ``quimibond_sgi.dev_block_generic_sample_from`` (vacío = sin bloqueo; la
  fecha la decide Jose, hay órdenes abiertas con ellos) no se crean ni
  confirman órdenes de fabricación con esos artículos.
"""
from odoo import api, fields, models
from odoo.exceptions import UserError, ValidationError

DEV_PRODUCT_STATES = [
    ('desarrollo', "En desarrollo"),
    ('pilotaje', "En pilotaje"),
    ('liberado', "Liberado"),
    ('archivado', "Archivado"),
]
DEV_PRODUCT_ROLES = [('crudo', "Crudo (H)"), ('tenido', "Teñido (I)"), ('acabado', "Acabado (J)")]
OPERATION_CODES = {'crudo': 'H', 'tenido': 'I', 'acabado': 'J'}
NATURAL_COLOR = 'NT'
# Hilos que llegan con color: el crudo conserva el color final.
COLORED_YARN_CODES = ('B', 'R')
GENERIC_SAMPLE_PREFIX = 'MUESTRA PILOTO'
PARAM_BLOCK_GENERIC = 'quimibond_sgi.dev_block_generic_sample_from'
CATEG_NAMES = {
    'crudo': ("Producto en Proceso / Tejido Circular", "Producto en Proceso"),
    'tenido': ("Producto en Proceso / Teñido", "Producto en Proceso"),
    'acabado_industrial': ("Producto Terminado / Tejido Circular / Industrial", "Producto Terminado / Tejido Circular",
                           "Producto Terminado"),
    'acabado_confeccion': ("Producto Terminado / Tejido Circular / Confección", "Producto Terminado / Tejido Circular",
                           "Producto Terminado"),
}


class ProductTemplateDev(models.Model):
    _inherit = 'product.template'

    sgi_dev_state = fields.Selection(DEV_PRODUCT_STATES, string="Estado de desarrollo", tracking=True, copy=False,
                                     help="Solo en artículos nacidos de un desarrollo. En desarrollo: no se vende. "
                                          "En pilotaje: se vende con aviso. Liberado: de línea. Archivado: el "
                                          "desarrollo se perdió o canceló.")
    sgi_dev_project_id = fields.Many2one('project.project', string="Proyecto de desarrollo", copy=False, index=True,
                                         ondelete='set null',
                                         help="Proyecto de desarrollo de producto que creó el artículo.")
    sgi_dev_role = fields.Selection(DEV_PRODUCT_ROLES, string="Papel en la ruta", copy=False,
                                    help="Crudo, teñido o acabado dentro de la ruta del desarrollo.")

    def _sgi_dev_set_state(self, state):
        """Cambia el estado de desarrollo y lo que implica: venta y archivo."""
        for tmpl in self:
            vals = {'sgi_dev_state': state}
            if tmpl.sgi_dev_role == 'acabado' or not tmpl.sgi_dev_role:
                vals['sale_ok'] = state in ('pilotaje', 'liberado')
            if state == 'archivado':
                vals['active'] = False
                tmpl.env['mrp.bom'].sudo().search([('product_tmpl_id', '=', tmpl.id)]).write({'active': False})
            tmpl.write(vals)
        return True


class ProductProductDevCode(models.Model):
    """57.142.0: si alguien corrige el código del artículo acabado (el 499 pasó de Q21 a Q22 a mano),
    el nombre del proyecto se vuelve a armar con el código final."""
    _inherit = 'product.product'

    def write(self, vals):
        res = super().write(vals)
        if 'default_code' in vals and not self.env.context.get('sgi_dev_migration'):
            projects = self.env['project.project'].search([('sgi_dev_product_id', 'in', self.ids)])
            if projects:
                projects._sgi_dev_sync_name()
        return res


class SaleOrderDev(models.Model):
    _inherit = 'sale.order'

    def action_confirm(self):
        for order in self:
            lines = order.order_line.filtered(lambda l: l.product_id.sgi_dev_state)
            blocked = lines.filtered(lambda l: l.product_id.sgi_dev_state in ('desarrollo', 'archivado'))
            if blocked:
                raise UserError(
                    "No se puede confirmar: %s está en desarrollo (o archivado) y aún no se vende. Se vende "
                    "cuando el cliente aprueba la muestra (pilotaje)."
                    % ", ".join(blocked.mapped('product_id.display_name')))
            pilot = lines.filtered(lambda l: l.product_id.sgi_dev_state == 'pilotaje')
            if pilot:
                order.message_post(body="Aviso: %s está en pilotaje; los tres primeros lotes del primer pedido son "
                                        "el pilotaje del desarrollo." % ", ".join(pilot.mapped('product_id.display_name')))
        return super().action_confirm()


class MrpProductionDev(models.Model):
    _inherit = 'mrp.production'

    @api.model
    def _sgi_dev_generic_block_date(self):
        raw = self.env['ir.config_parameter'].sudo().get_param(PARAM_BLOCK_GENERIC, '') or ''
        try:
            return fields.Date.to_date(raw.strip()) if raw.strip() else None
        except ValueError:
            return None

    def _sgi_dev_check_generic_sample(self):
        since = self._sgi_dev_generic_block_date()
        if not since or fields.Date.context_today(self) < since:
            return
        generic = self.filtered(lambda p: (p.product_id.default_code or '').upper().startswith(GENERIC_SAMPLE_PREFIX))
        if generic:
            raise UserError(
                "Desde el %s las muestras de desarrollo no se fabrican con el artículo genérico «%s»: pida la corrida "
                "desde el proyecto de desarrollo, que genera el artículo con su código."
                % (since.strftime('%d/%m/%Y'), generic[:1].product_id.display_name))

    @api.model_create_multi
    def create(self, vals_list):
        productions = super().create(vals_list)
        productions._sgi_dev_check_generic_sample()
        return productions

    def action_confirm(self):
        self._sgi_dev_check_generic_sample()
        return super().action_confirm()


class ProjectProjectDevCode(models.Model):
    _inherit = 'project.project'

    # 57.131.0 (Jose 2026-10-08, punto 3): los artículos del desarrollo desde el
    # proyecto; la entrada «Lista de materiales» de C1.05 se liga con la
    # cotización por «project_id.sgi_dev_product_tmpl_ids.bom_ids».
    sgi_dev_product_tmpl_ids = fields.One2many('product.template', 'sgi_dev_project_id',
                                               string="Artículos del desarrollo")
    sgi_dev_code_composicion_id = fields.Many2one('ficha.tecnica.clave.codigo', string="Composición (1)",
                                                  domain=[('kind', '=', 'composicion')], ondelete='restrict',
                                                  help="Posición 1 del código: la composición de la tela.")
    sgi_dev_code_dibujo_id = fields.Many2one('ficha.tecnica.clave.codigo', string="Dibujo (2)",
                                             domain=[('kind', '=', 'dibujo')], ondelete='restrict',
                                             help="Posición 2 del código: el dibujo o ligamento.")
    sgi_dev_code_peso = fields.Integer(string="Peso (3-5, g/m²)",
                                       help="Posiciones 3 a 5: masa por unidad de área en g/m². Se toma de la tabla.")
    sgi_dev_code_hilo_id = fields.Many2one('ficha.tecnica.clave.codigo', string="Tipo de hilo (6)",
                                           domain=[('kind', '=', 'hilo')], ondelete='restrict',
                                           help="Posición 6: hilo natural, preteñido o reciclado.")
    sgi_dev_code_galga = fields.Integer(string="Galga (7-8)",
                                        help="Galga de la máquina (14, 16, 18…); el código lleva su rango. Se toma de "
                                             "la tabla.")
    # 57.142.0: el DAT P-D02-01 da a cada galga un rango de dos dígitos (galga 18 = 21 a 30); antes
    # siempre se ponía el primero. Sin artículo base se elige el número dentro del rango; con base se
    # toman los dígitos del código del base.
    sgi_dev_code_galga_digits = fields.Integer(string="Dígitos de galga (7-8)", compute='_compute_sgi_dev_code_galga_digits',
                                               store=True, readonly=False, copy=False,
                                               help="Posiciones 7 y 8 del código: un número dentro del rango de la galga "
                                                    "(galga 18 = 21 a 30). Se propone el primero del rango; con artículo "
                                                    "base se usan los dígitos del base.")
    sgi_dev_code_galga_range = fields.Char(string="Rango de la galga", compute='_compute_sgi_dev_code_galga_range')
    sgi_dev_code_color_id = fields.Many2one('ficha.tecnica.clave.codigo', string="Color (10-11)",
                                            domain=[('kind', '=', 'color')], ondelete='restrict',
                                            help="Posiciones 10 y 11: color del producto terminado.")
    sgi_dev_code_ancho = fields.Integer(string="Ancho acabado (12-14, cm)",
                                        help="Posiciones 12 a 14 del acabado: ancho de tela abierta en cm. Se toma "
                                             "de la tabla.")
    sgi_dev_code_ancho_crudo = fields.Integer(string="Ancho crudo (cm)",
                                              help="Ancho de tela cruda en cm: posiciones 12 a 14 del crudo y del "
                                                   "teñido.")
    sgi_dev_code_acabado_id = fields.Many2one('ficha.tecnica.clave.codigo', string="Acabado (15-16)",
                                              domain=[('kind', '=', 'acabado')], ondelete='restrict',
                                              help="Posiciones 15 y 16, opcionales: el acabado especial.")
    sgi_dev_code_tenido = fields.Boolean(string="Lleva teñido", compute='_compute_sgi_dev_code_tenido', store=True,
                                         readonly=False,
                                         help="Si la ruta tiene teñido se crea también el artículo I. Se propone "
                                              "cuando el color no es natural.")
    sgi_dev_code_crudo = fields.Char(string="Código crudo", compute='_compute_sgi_dev_codes',
                                     help="Código propuesto del crudo (H).")
    sgi_dev_code_tenido_code = fields.Char(string="Código teñido", compute='_compute_sgi_dev_codes',
                                           help="Código propuesto del teñido (I).")
    sgi_dev_code_acabado_code = fields.Char(string="Código acabado", compute='_compute_sgi_dev_codes',
                                            help="Código propuesto del acabado (J).")
    sgi_dev_product_crudo_id = fields.Many2one('product.product', string="Artículo crudo", copy=False,
                                               help="Artículo crudo (H) del desarrollo.")
    sgi_dev_product_tenido_id = fields.Many2one('product.product', string="Artículo teñido", copy=False,
                                                help="Artículo teñido (I) del desarrollo.")

    @api.depends('sgi_dev_code_color_id')
    def _compute_sgi_dev_code_tenido(self):
        for project in self:
            if project.sgi_dev_code_color_id:
                project.sgi_dev_code_tenido = project.sgi_dev_code_color_id.code != NATURAL_COLOR
            else:
                project.sgi_dev_code_tenido = project.sgi_dev_code_tenido

    def _sgi_dev_gauge_clave(self):
        self.ensure_one()
        if not self.sgi_dev_code_galga:
            return self.env['ficha.tecnica.clave.codigo']
        return self.env['ficha.tecnica.clave.codigo'].search([('kind', '=', 'galga'), ('gauge', '=', self.sgi_dev_code_galga)], limit=1)

    @api.depends('sgi_dev_code_galga')
    def _compute_sgi_dev_code_galga_digits(self):
        for project in self:
            clave = project._sgi_dev_gauge_clave()
            project.sgi_dev_code_galga_digits = clave.range_from if clave else 0

    @api.depends('sgi_dev_code_galga')
    def _compute_sgi_dev_code_galga_range(self):
        for project in self:
            clave = project._sgi_dev_gauge_clave()
            project.sgi_dev_code_galga_range = ("%02d a %02d" % (clave.range_from, clave.range_to)) if clave else ''

    @api.constrains('sgi_dev_code_galga', 'sgi_dev_code_galga_digits')
    def _check_sgi_dev_code_galga_digits(self):
        for project in self:
            clave = project._sgi_dev_gauge_clave()
            d = project.sgi_dev_code_galga_digits
            if clave and d and not clave.range_from <= d <= clave.range_to:
                raise ValidationError("Los dígitos de galga %02d están fuera del rango de la galga %s (%02d a %02d)."
                                      % (d, clave.gauge, clave.range_from, clave.range_to))

    def _sgi_dev_gauge_digits(self):
        """'22' para la galga del proyecto: los dígitos elegidos dentro del rango, o el primero del
        rango; '' si no hay galga en el catálogo o los dígitos están fuera del rango."""
        self.ensure_one()
        clave = self._sgi_dev_gauge_clave()
        if not clave:
            return ''
        d = self.sgi_dev_code_galga_digits or clave.range_from
        return ("%02d" % d) if clave.range_from <= d <= clave.range_to else ''

    def _sgi_dev_code_parts(self):
        """Partes del código o el nombre de lo que falta."""
        self.ensure_one()
        missing = []
        if not self.sgi_dev_code_composicion_id:
            missing.append("composición")
        if not self.sgi_dev_code_dibujo_id:
            missing.append("dibujo")
        if not 0 < self.sgi_dev_code_peso < 1000:
            missing.append("peso (1 a 999 g/m²)")
        if not self.sgi_dev_code_hilo_id:
            missing.append("tipo de hilo")
        gauge = self._sgi_dev_gauge_digits()
        if not gauge:
            missing.append("galga (con rango en el catálogo) y sus dígitos dentro del rango%s"
                           % ((" %s" % self.sgi_dev_code_galga_range) if self.sgi_dev_code_galga_range else ''))
        if not self.sgi_dev_code_color_id:
            missing.append("color")
        if not 0 < self.sgi_dev_code_ancho < 1000:
            missing.append("ancho acabado (1 a 999 cm)")
        if not 0 < self.sgi_dev_code_ancho_crudo < 1000:
            missing.append("ancho crudo (1 a 999 cm)")
        if missing:
            return None, missing
        return {
            'composicion': self.sgi_dev_code_composicion_id.code.upper(),
            'dibujo': self.sgi_dev_code_dibujo_id.code.upper(),
            'peso': "%03d" % self.sgi_dev_code_peso,
            'hilo': self.sgi_dev_code_hilo_id.code.upper(),
            'galga': gauge,
            'color': self.sgi_dev_code_color_id.code.upper(),
            'ancho': "%03d" % self.sgi_dev_code_ancho,
            'ancho_crudo': "%03d" % self.sgi_dev_code_ancho_crudo,
            'acabado': (self.sgi_dev_code_acabado_id.code or '').upper(),
        }, []

    def _sgi_dev_build_code(self, role, parts=None):
        self.ensure_one()
        if parts is None:
            parts, _missing = self._sgi_dev_code_parts()
        if not parts:
            return ''
        base = parts['composicion'] + parts['dibujo'] + parts['peso'] + parts['hilo'] + parts['galga']
        if role == 'acabado':
            return base + 'J' + parts['color'] + parts['ancho'] + parts['acabado']
        color = parts['color'] if (role == 'tenido' or parts['hilo'] in COLORED_YARN_CODES) else NATURAL_COLOR
        return base + OPERATION_CODES[role] + color + parts['ancho_crudo']

    @api.depends('sgi_dev_code_composicion_id', 'sgi_dev_code_dibujo_id', 'sgi_dev_code_peso', 'sgi_dev_code_hilo_id',
                 'sgi_dev_code_galga', 'sgi_dev_code_galga_digits', 'sgi_dev_code_color_id', 'sgi_dev_code_ancho',
                 'sgi_dev_code_ancho_crudo', 'sgi_dev_code_acabado_id', 'sgi_dev_code_tenido')
    def _compute_sgi_dev_codes(self):
        for project in self:
            parts, _missing = project._sgi_dev_code_parts()
            project.sgi_dev_code_crudo = project._sgi_dev_build_code('crudo', parts)
            project.sgi_dev_code_tenido_code = project._sgi_dev_build_code('tenido', parts) if project.sgi_dev_code_tenido else ''
            project.sgi_dev_code_acabado_code = project._sgi_dev_build_code('acabado', parts)

    def action_sgi_dev_code_from_table(self):
        """Toma peso, galga y ancho de la tabla de características (nominal del cliente)."""
        for project in self:
            lines = project.sgi_dev_line_ids
            vals = {}
            masa = project._sgi_dev_pick(lines, 'masa')
            if masa and masa.spec_nominal and not project.sgi_dev_code_peso:
                vals['sgi_dev_code_peso'] = int(round(masa.spec_nominal))
            galga = project._sgi_dev_pick(lines, 'galga')
            if galga and galga.spec_nominal and not project.sgi_dev_code_galga:
                vals['sgi_dev_code_galga'] = int(round(galga.spec_nominal))
            ancho = project._sgi_dev_pick(lines, 'ancho')
            if ancho and ancho.spec_nominal and not project.sgi_dev_code_ancho:
                # La tabla va en metros; el código en centímetros.
                vals['sgi_dev_code_ancho'] = int(round(ancho.spec_nominal * 100 if ancho.spec_nominal < 10 else ancho.spec_nominal))
            if vals:
                project.write(vals)
        return True

    @api.model
    def _sgi_dev_category(self, names):
        Categ = self.env['product.category']
        for name in names:
            categ = Categ.search([('complete_name', '=', name)], limit=1)
            if categ:
                return categ
        return Categ.search([], limit=1)

    def _sgi_dev_product_name(self):
        self.ensure_one()
        return "%s DE %d G/M2" % ((self.sgi_dev_code_dibujo_id.name or 'TEJIDO').upper(), self.sgi_dev_code_peso)

    def action_sgi_dev_generate_products(self):
        """Crea (o liga, si el código ya existe) los artículos de la ruta: crudo, teñido y acabado."""
        Product = self.env['product.product']
        kg = self.env.ref('uom.product_uom_kgm')
        meter = self.env.ref('uom.product_uom_meter')
        for project in self.filtered('sgi_is_ft'):
            project.action_sgi_dev_code_from_table()
            parts, missing = project._sgi_dev_code_parts()
            if missing:
                raise UserError("Falta para armar el código: %s." % ", ".join(missing))
            confeccion = 'confec' in (project.sgi_dev_team_id.name or '').lower()
            plan = [
                ('crudo', 'sgi_dev_product_crudo_id', CATEG_NAMES['crudo'], kg, False),
                ('tenido', 'sgi_dev_product_tenido_id', CATEG_NAMES['tenido'], kg, False),
                ('acabado', 'sgi_dev_product_id',
                 CATEG_NAMES['acabado_confeccion' if confeccion else 'acabado_industrial'], meter, True),
            ]
            name = project._sgi_dev_product_name()
            for role, field, categ_names, uom, saleable in plan:
                if role == 'tenido' and not project.sgi_dev_code_tenido:
                    continue
                if project[field]:
                    continue
                code = project._sgi_dev_build_code(role, parts)
                product = Product.with_context(active_test=False).search([('default_code', '=', code)], limit=1)
                if not product:
                    product = Product.create({
                        'name': name, 'default_code': code, 'type': 'consu', 'is_storable': True,
                        'uom_id': uom.id, 'categ_id': project._sgi_dev_category(categ_names).id,
                        'sale_ok': False, 'purchase_ok': False,
                        'company_id': project.company_id.id or False,
                        'sgi_dev_state': 'desarrollo', 'sgi_dev_project_id': project.id, 'sgi_dev_role': role,
                    })
                    project.message_post(body="Artículo %s creado: %s (%s)." % (role, code, name))
                elif not product.sgi_dev_state and not product.sgi_dev_project_id:
                    project.message_post(body="El código %s ya existía (%s): se liga el artículo existente, sin "
                                              "cambiar su estado." % (code, product.display_name))
                project.write({field: product.id})
        return True

    # Estados del artículo al cambiar de etapa el proyecto.
    def _sgi_dev_products(self):
        self.ensure_one()
        return (self.sgi_dev_product_crudo_id | self.sgi_dev_product_tenido_id | self.sgi_dev_product_id).filtered(
            lambda p: p.sgi_dev_project_id == self).mapped('product_tmpl_id')

    def _sgi_dev_sync_product_states(self):
        for project in self.filtered('sgi_is_ft'):
            templates = project._sgi_dev_products()
            if not templates:
                continue
            key = project.sgi_dev_stage_key
            if key == 'cerrado_sin_producto':
                templates.filtered(lambda t: t.sgi_dev_state not in ('liberado',))._sgi_dev_set_state('archivado')
            elif key == 'liberado':
                templates.filtered(lambda t: t.sgi_dev_state != 'liberado')._sgi_dev_set_state('liberado')
            elif key == 'pilotaje':
                templates.filtered(lambda t: t.sgi_dev_state == 'desarrollo')._sgi_dev_set_state('pilotaje')

    def write(self, vals):
        res = super().write(vals)
        if 'stage_id' in vals and not self.env.context.get('sgi_dev_migration'):
            self._sgi_dev_sync_product_states()
        return res

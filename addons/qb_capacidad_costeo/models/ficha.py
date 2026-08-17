# -*- coding: utf-8 -*-
"""Ficha técnica por producto — la nomenclatura de Quimibond, en Odoo.

La referencia interna codifica la spec del producto:
    WR135Q46JNT165
    │ │  │  ││  └─ ancho de rama: 165 → 1.65 m
    │ │  │  │└─── color (NT natural, BL blanco, NG negro, GO oro, OW...)
    │ │  │  └──── estado: H crudo · I teñido · J terminado
    │ │  └─────── calidad (Q46)
    │ └────────── gramaje: 135 g/m² (EXACTAMENTE 3 dígitos;
    │             4 dígitos = código de resina 4032/9032, NO gramaje)
    └──────────── familia (WR, WJ, WC, X, XJ, ZN, WM, ...)
    sufijo ' I' = importado (solo inspección/reempaque)

Este modelo PARSEA esa nomenclatura a campos estructurados, los combina
con el maestro de pesos y los gramajes/anchos curados, y produce una
ficha editable + PDF para clientes. Tabla propia del módulo — no toca
product.product.
"""
import re

from odoo import api, fields, models

FICHA_REGEX = re.compile(
    r'^(?P<familia>[A-Z]+?)(?P<numero>\d+)(?P<medio>[A-Z][A-Z0-9]*?)?'
    r'(?P<ancho>\d{2,3})$')


class QbProductoFicha(models.Model):
    _name = 'qb.producto.ficha'
    _description = 'Ficha técnica de producto'
    _order = 'product_id'
    _rec_name = 'product_id'

    product_id = fields.Many2one(
        'product.product', required=True, index=True, ondelete='cascade')
    default_code = fields.Char(
        related='product_id.default_code', store=True, string='Referencia')
    active = fields.Boolean(default=True)

    familia = fields.Char(
        help='Prefijo de la referencia: WR, WJ, WC, X, XJ, ZN, WM...')
    gramaje_g_m2 = fields.Float(string='Gramaje (g/m²)', digits=(16, 1))
    ancho_m = fields.Float(string='Ancho de rama (m)', digits=(16, 2))
    calidad = fields.Char(help='Segmento Q## de la referencia (ej. Q46).')
    estado = fields.Selection([
        ('crudo', 'Crudo (H)'),
        ('tenido', 'Teñido (I)'),
        ('terminado', 'Terminado (J)'),
    ], help='Sufijo H/I/J de la nomenclatura.')
    color = fields.Char(help='Código de color: NT, BL, NG, GO, OW...')
    resina_code = fields.Char(
        string='Código de resina',
        help='Bloque de 4 dígitos (4032/9032): producto con resina — el '
             'número NO es gramaje.')
    es_importado = fields.Boolean(
        string='Importado', help="Sufijo ' I': solo inspección/reempaque.")
    composicion = fields.Char(help='Ej. 100% PES (tomada del nombre).')
    presentacion = fields.Selection(
        [('m', 'Metros'), ('kg', 'Kilogramos')], string='Se vende en')
    peso_kg_unidad = fields.Float(
        string='Peso (kg por unidad de venta)', digits=(16, 4),
        help='Del maestro de pesos (qb.producto.peso).')
    rendimiento_m_kg = fields.Float(
        string='Rendimiento (m/kg)', digits=(16, 2))
    source = fields.Selection([
        ('parser', 'Nomenclatura (parseada)'),
        ('supabase', 'Curado (Supabase)'),
        ('manual', 'Manual'),
    ], default='parser', required=True,
        help='Manual gana: el generador no pisa fichas editadas a mano.')
    parse_warning = fields.Char(
        string='Advertencia del parser',
        help='Qué no se pudo interpretar de la referencia.')
    notas = fields.Text()

    _sql_constraints = [
        ('product_uniq', 'unique(product_id)',
         'Solo una ficha por producto (edítala en lugar de duplicar).'),
    ]

    # ------------------------------------------------------------------
    # Parser de nomenclatura
    # ------------------------------------------------------------------
    @api.model
    def parse_ref(self, ref):
        """Referencia → dict de campos de la ficha. Conservador: solo llena
        lo que matchea sin ambigüedad; lo demás queda en parse_warning."""
        vals = {'parse_warning': False}
        if not ref:
            return dict(vals, parse_warning='Producto sin referencia interna.')
        ref = ref.strip()
        if ref.endswith(' I'):
            vals['es_importado'] = True
            ref = ref[:-2].strip()
        if re.match(r'^(SALDO|DESPERDICIO)', ref, re.I):
            return dict(vals, familia='SUBPRODUCTO',
                        parse_warning='Subproducto/desperdicio: sin spec propia.')
        m = FICHA_REGEX.match(ref.replace(' ', ''))
        if not m:
            return dict(vals, parse_warning='La referencia no sigue la '
                                            'nomenclatura estándar.')
        vals['familia'] = m.group('familia')
        numero = m.group('numero')
        if len(numero) == 3:
            vals['gramaje_g_m2'] = float(numero)
        elif len(numero) == 4:
            # Código de resina (4032/9032) — NO es gramaje
            vals['resina_code'] = numero
        elif len(numero) == 5:
            # Gramaje (3) + calidad sin Q (2): XJ14021... = 140 g/m², cal 21
            vals['gramaje_g_m2'] = float(numero[:3])
            vals['calidad'] = numero[3:]
        else:
            vals['parse_warning'] = ('Bloque numérico de %s dígitos (%s): '
                                     'la norma son 3 (gramaje), 4 (resina) '
                                     'o 5 (gramaje+calidad).'
                                     % (len(numero), numero))
        ancho = int(m.group('ancho')) / 100.0
        if 0.6 <= ancho <= 3.5:
            vals['ancho_m'] = ancho
        medio = m.group('medio') or ''
        q = re.search(r'Q(\d+)', medio)
        if q:
            vals['calidad'] = 'Q%s' % q.group(1)
            medio = medio.replace(q.group(0), '', 1)
        estado = {'H': 'crudo', 'I': 'tenido', 'J': 'terminado'}
        if medio[:1] in estado:
            vals['estado'] = estado[medio[0]]
            medio = medio[1:]
        if medio and medio.isalpha() and len(medio) <= 3:
            vals['color'] = medio
        elif medio:
            vals['parse_warning'] = ('Segmento "%s" no interpretado.' % medio)
        return vals

    @api.model
    def _build_vals(self, product):
        Peso = self.env['qb.producto.peso']
        vals = self.parse_ref(product.default_code)
        vals['product_id'] = product.id
        name = product.name or ''
        comp = re.search(r'(\d{1,3}\s*%[^,;]*)', name)
        if comp:
            vals['composicion'] = comp.group(1).strip()
        uom = (product.uom_id.name or '').lower()
        vals['presentacion'] = 'kg' if uom.startswith('k') else 'm'
        kg = Peso.resolve_kg_per_unit(product)
        if kg:
            vals['peso_kg_unidad'] = kg
        m_per_kg = Peso.resolve_m_per_kg(product)
        if m_per_kg:
            vals['rendimiento_m_kg'] = m_per_kg
        return vals

    # ------------------------------------------------------------------
    # Generación masiva (acción de menú + parte del import semanal)
    # ------------------------------------------------------------------
    @api.model
    def action_generar_fichas(self):
        """Crea/actualiza la ficha de TODOS los productos vendibles desde la
        nomenclatura + maestro de pesos. Las fichas manuales no se pisan."""
        products = self.env['product.product'].search(
            [('sale_ok', '=', True), ('default_code', '!=', False)])
        existing = {f.product_id.id: f
                    for f in self.with_context(active_test=False).search([])}
        to_create = []
        updated = skipped = 0
        for product in products:
            ficha = existing.get(product.id)
            if ficha and ficha.source == 'manual':
                skipped += 1
                continue
            vals = self._build_vals(product)
            if ficha:
                ficha.write(vals)
                updated += 1
            else:
                to_create.append(vals)
        if to_create:
            self.create(to_create)
        return {
            'type': 'ir.actions.act_window',
            'name': 'Fichas técnicas',
            'res_model': self._name,
            'view_mode': 'list,form',
            'context': {},
            'help': ('%s creadas, %s actualizadas, %s manuales respetadas'
                     % (len(to_create), updated, skipped)),
        }

    @api.model
    def action_abrir_de_producto(self):
        """Botón en la ficha del producto: abre (o crea) su ficha técnica."""
        active_model = self.env.context.get('active_model')
        active_id = self.env.context.get('active_id')
        record = self.env[active_model].browse(active_id).exists()
        product = record if active_model == 'product.product' \
            else record.product_variant_id
        ficha = self.search([('product_id', '=', product.id)], limit=1)
        if not ficha:
            ficha = self.create(self._build_vals(product))
        return {
            'type': 'ir.actions.act_window',
            'res_model': self._name,
            'res_id': ficha.id,
            'view_mode': 'form',
            'target': 'current',
        }

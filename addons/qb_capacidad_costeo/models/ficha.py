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

    _product_uniq = models.Constraint(
        'unique(product_id)',
        "Solo una ficha por producto (edítala en lugar de duplicar).",
    )

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

    # ------------------------------------------------------------------
    # Resolución por etapa: qué corre en CADA centro
    # ------------------------------------------------------------------
    @api.model
    def insumos_de_etapa(self, product, estado, max_depth=8):
        """Artículos de una etapa que consume UNA unidad vendida de `product`.

        La letra H/I/J no es decorativa: dice qué centro produjo el artículo
        (H tejido, I tintorería, J acabado). El terminado que se vende sale
        de ACABADO, así que preguntarle a la tejedora por el código del
        terminado es preguntarle por algo que esa máquina nunca hizo —
        `WJ038Q22JNT160` se teje como `WJ035Q22HNT200`, dos BOMs abajo y con
        otro gramaje y otro ancho. Ese era el motivo real de que el catálogo
        de familias de máquinas no cruzara con NADA de lo que se vende.

        No se puede deducir de la nomenclatura: entre etapas cambian el
        gramaje y el ancho (38 g/m² a 1.60 m ← 35 g/m² a 2.00 m), así que
        cruzar por familia+gramaje+ancho falla. El BOM es el único enlace
        real, y esta ficha es la que reconoce la etapa al llegar.

        Devuelve {product: cantidad por unidad vendida} — vacío si la cadena
        nunca llega a esa etapa. La cantidad va SIEMPRE en la unidad propia
        de cada artículo devuelto: las dos divisiones del camino convierten
        unidades, igual que `_explode_bom` en el motor de costeo. Sin eso, un
        BOM declarado en otra unidad que su producto (el `WJ038Q22JNT160M2`
        está en m², el `WB038Q46IBE096` en kg) mete la razón de conversión
        entera como factor — en el cruce m↔kg, un error de ~16× en la carga
        del telar, y silencioso.
        """
        out = {}
        if not product:
            return out
        BOM = self.env['mrp.bom']
        fichas = {}

        def etapa_de(prod):
            # La ficha manda cuando existe (puede estar editada a mano), pero
            # no se puede depender de ella: `action_generar_fichas` solo cubre
            # productos vendibles, y los crudos y teñidos rara vez lo son —
            # de 1,839 fichas solo 10 son de teñido. Sin este respaldo, la
            # cadena se cortaría justo en los intermedios que hay que cruzar.
            if prod.id not in fichas:
                ficha = self.search([('product_id', '=', prod.id)], limit=1)
                fichas[prod.id] = ficha.estado if ficha else \
                    self.parse_ref(prod.default_code).get('estado')
            return fichas[prod.id]

        def walk(prod, factor, depth, vistos):
            if depth > max_depth or factor <= 0 or prod.id in vistos:
                return
            if etapa_de(prod) == estado:
                # Llegamos: no se sigue bajando. El crudo se teje, no se
                # compone de otro crudo.
                out[prod] = out.get(prod, 0.0) + factor
                return
            bom = BOM._bom_find(prod).get(prod)
            if not bom or not bom.product_qty:
                return
            # Las dos conversiones son las mismas que hace `_explode_bom`:
            # el encabezado del BOM a la unidad del producto que produce, y
            # cada línea a la unidad del componente que consume.
            bom_qty = bom.product_uom_id._compute_quantity(
                bom.product_qty, prod.uom_id, round=False,
                raise_if_failure=False) or bom.product_qty
            if not bom_qty:
                return
            for line in bom.bom_line_ids:
                comp = line.product_id
                if not comp:
                    continue
                qty = line.product_uom_id._compute_quantity(
                    line.product_qty, comp.uom_id, round=False,
                    raise_if_failure=False) or line.product_qty
                walk(comp, factor * qty / bom_qty,
                     depth + 1, vistos | {prod.id})

        walk(product, 1.0, 0, frozenset())
        return out

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
    # El peso de la ficha es una COPIA del maestro
    # ------------------------------------------------------------------
    @api.model
    def sync_pesos(self, products=None, tolerancia=0.0):
        """Refresca `peso_kg_unidad` y `rendimiento_m_kg` desde el maestro.

        Los dos campos son copias de `qb.producto.peso`, no campos
        relacionados: se llenan al generar la ficha y ahí se quedan. Cada vez
        que se mide un peso, la copia envejece en silencio — y esta ficha es
        la hoja técnica que se le manda al cliente. El WJ032Q22JNT160 tenía
        0.0512 kg/m (gramaje × ancho, la adivinanza del código) contra
        0.059114 medidos de báscula: 13% abajo durante 16 días.

        Respeta la misma regla que el generador: una ficha `manual` no se
        pisa. Y toca SOLO estos dos campos — regenerar la ficha completa
        reescribiría además gramaje, ancho, estado y color desde el parser,
        que es un martillo más grande que el clavo.

        Devuelve cuántas fichas se actualizaron.
        """
        Peso = self.env['qb.producto.peso']
        dominio = [('source', '!=', 'manual')]
        if products is not None:
            if not products:
                return 0
            dominio.append(('product_id', 'in', products.ids))
        cache = {}
        tocadas = 0
        for ficha in self.with_context(active_test=False).search(dominio):
            vals = {}
            kg = Peso.resolve_kg_per_unit(ficha.product_id, cache)
            if kg and abs(kg - ficha.peso_kg_unidad) > max(
                    tolerancia * kg, 1e-9):
                vals['peso_kg_unidad'] = kg
            m_kg = Peso.resolve_m_per_kg(ficha.product_id, cache)
            if m_kg and abs(m_kg - ficha.rendimiento_m_kg) > max(
                    tolerancia * m_kg, 1e-9):
                vals['rendimiento_m_kg'] = m_kg
            if vals:
                ficha.write(vals)
                tocadas += 1
        return tocadas

    @api.model
    def fichas_con_peso_desfasado(self, tol_pct=2.0):
        """Fichas cuyo peso guardado se alejó del maestro más de `tol_pct`.

        El guard del sync: cubre las fichas `manual` (que a propósito no se
        refrescan), las que se escribieron por una vía que no pasó por el
        maestro, y cualquier hueco futuro. Misma regla que ya cubre pesos
        (5.11), AVCO de importados (5.13) y consumo de BOM (5.14): un
        parámetro que duplica un dato vivo se contrasta contra su fuente.

        Devuelve [(ficha, guardado, maestro, desviación)] de mayor a menor.

        Recorre el MAESTRO, no las fichas: son dos consultas en vez de una
        por ficha, y este check vive en el panel, que es la pantalla de
        entrada. No se pierde cobertura — un producto sin registro en el
        maestro resuelve por la misma cadena de respaldo con que se llenó su
        ficha, así que no puede divergir de sí mismo.
        """
        pesos = self.env['qb.producto.peso'].search(
            [('kg_per_unit', '>', 0)])
        if not pesos:
            return []
        por_producto = {p.product_id.id: p.kg_per_unit for p in pesos}
        fuera = []
        for ficha in self.search([('peso_kg_unidad', '>', 0),
                                  ('product_id', 'in', list(por_producto))]):
            kg = por_producto.get(ficha.product_id.id)
            if not kg:
                continue
            desv = (ficha.peso_kg_unidad - kg) / kg
            if abs(desv) * 100.0 > tol_pct:
                fuera.append((ficha, ficha.peso_kg_unidad, kg, desv))
        fuera.sort(key=lambda f: -abs(f[3]))
        return fuera

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

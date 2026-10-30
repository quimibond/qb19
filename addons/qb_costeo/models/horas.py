# -*- coding: utf-8 -*-
"""Horas por producto y centro, con la fuente de cada dato.

Para cada producto fabricado y cada centro directo de su ruta: cuántas horas
de ese centro consume UNA unidad del producto, y de dónde salió el número.
En orden de preferencia:

    pesaje    ritmo medido rollo por rollo desde el pesaje (módulo
              `qb_tejido_ritmo`, si está instalado): manda sobre las órdenes
              de trabajo porque el cronómetro de las órdenes se queda abierto
    medido    órdenes de trabajo reales del producto en la ventana (12 meses),
              dentro de la banda de velocidad del centro
    estandar  operación de la receta vigente (tiempo estándar que capturó
              Ingeniería)
    ficha     velocidad del artículo en cada máquina del centro según la ficha
              técnica (procesos por artículo de `quimibond_ficha_tecnica_tela`,
              si está instalado): p. ej. los m/min de cada tela en cada rama
              que da Producción
    hermano   mismo código salvo color y ancho (primeros 9 caracteres de la
              nomenclatura DAT P-D02-01: composición, dibujo, peso, hilo,
              galga y operación)
    galga     promedio de lo tejido en las máquinas de la misma galga
              (posiciones 7-8 del código → etiqueta «GALGA nn» de la máquina)
    estimado  promedio del centro
    driver    lo que dice la configuración del centro (velocidad de rama,
              horas por carga, horas fijas por unidad) cuando no hay órdenes
              ni receta que midan ese centro
    manual    capturado con motivo y vigencia; manda sobre todo lo demás

Los semielaborados heredan hacia arriba por la receta: el terminado J carga
las horas de tejido de su crudo H y las de teñido de su I, más las propias.
`horas_unidad` es el total (propias + heredadas) y `calidad` la peor fuente
que intervino.
"""
import logging
import re
from datetime import date

from dateutil.relativedelta import relativedelta

from odoo import api, fields, models
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

FUENTES = [
    ('manual', 'Capturado con motivo'),
    ('pesaje', 'Pesaje de rollos'),
    ('medido', 'Órdenes de trabajo'),
    ('estandar', 'Estándar de la receta'),
    ('ficha', 'Velocidad del artículo (ficha técnica)'),
    ('hermano', 'Crudo hermano'),
    ('galga', 'Máquinas de la misma galga'),
    ('estimado', 'Promedio del centro'),
    ('driver', 'Configuración del centro'),
    ('ninguna', 'Sin dato'),
]
# Orden de calidad: menor = mejor.
RANGO = {'manual': 0, 'pesaje': 1, 'medido': 1, 'estandar': 1, 'ficha': 1,
         'hermano': 2, 'galga': 2, 'estimado': 3, 'driver': 3, 'ninguna': 4}
CALIDAD = {0: 'alta', 1: 'alta', 2: 'media', 3: 'baja', 4: 'ninguna'}

# Nomenclatura DAT P-D02-01: 14 caracteres sin el prefijo de unidad.
NOMEN_RE = re.compile(r'^I?([A-Z])([A-Z])(\d{3})([QBR])(\d{2})([HIJ])([A-Z]{2})(\d{3})')
GALGA_RANGOS = [(1, 10, 14), (11, 20, 16), (21, 30, 18), (31, 45, 22),
                (46, 55, 24), (56, 65, 26), (66, 75, 28), (76, 85, 30),
                (86, 95, 32)]


def raiz_hermano(ref):
    """Primeros 9 caracteres de la nomenclatura (hasta la operación), sin
    el prefijo de unidad. None si la referencia no sigue la norma."""
    m = NOMEN_RE.match((ref or '').replace(' ', ''))
    if not m:
        return None
    return ''.join(m.groups()[:6])


def codigo_metros(articulo):
    """1.6.4: el código en metros de un proceso con código en kg: «I» +
    tela acabada (DAT P-D02-01; IWJ053Q22JNT160 → WJ053Q22JNT160). Vacío
    si el código no es de esa forma (14 o 16 posiciones con J en la 9)."""
    code = (articulo or '').strip()
    base = code[1:]
    if code[:1] == 'I' and len(base) in (14, 16) and base[8:9] == 'J':
        return base
    return ''


def es_proceso_kg(product):
    """1.7.0: ¿Es la tela en proceso del flujo en dos niveles? «I» + código
    de la tela acabada y unidad de peso (IWJ053Q22JNT160 en kg). Con otra
    unidad, el prefijo «I» es de un importado."""
    if not product or not codigo_metros(product.default_code):
        return False
    kg = product.env.ref('uom.product_uom_kgm', raise_if_not_found=False)
    return bool(kg and product.uom_id and product.uom_id._has_common_reference(kg))


def es_tintoreria(op):
    """La operación de tintorería de una lista (el jet), como la reconoce
    `quimibond_tintoreria_dosificacion`: tipo de máquina, nombre de la
    operación o del centro de trabajo con «tintorer»."""
    wc = op.workcenter_id
    tipo = wc.ft_tipo_id.name if 'ft_tipo_id' in wc._fields else ''
    return any('tintorer' in (t or '').lower() for t in (tipo, op.name, wc.name))


def galga_de(ref):
    m = NOMEN_RE.match((ref or '').replace(' ', ''))
    if not m:
        return None
    n = int(m.group(5))
    for a, b, g in GALGA_RANGOS:
        if a <= n <= b:
            return g
    return None


class QbProductoHoras(models.Model):
    _name = 'qb.producto.horas'
    _description = 'Horas por unidad de producto y centro'
    _order = 'product_id, centro_id'
    _rec_name = 'product_id'

    product_id = fields.Many2one('product.product', required=True,
                                 index=True, ondelete='cascade')
    default_code = fields.Char(related='product_id.default_code', store=True)
    centro_id = fields.Many2one('qb.centro', required=True, index=True,
                                ondelete='cascade')
    company_id = fields.Many2one(
        'res.company', required=True,
        default=lambda self: self.env.company)
    horas_propias = fields.Float(
        digits=(16, 6), string='Horas propias / u',
        help='Horas del centro que consume la unidad en su propia receta '
             '(sin lo que heredan sus componentes).')
    fuente = fields.Selection(FUENTES, default='ninguna', required=True,
                              string='Fuente de las propias')
    horas_heredadas = fields.Float(
        digits=(16, 6), string='Horas heredadas / u',
        help='Horas de este centro que traen los componentes de la receta '
             '(el crudo trae el tejido; el teñido, la tintorería).')
    horas_unidad = fields.Float(
        digits=(16, 6), string='Horas / u', help='Propias + heredadas.')
    calidad = fields.Selection([
        ('alta', 'Alta: pesaje, medido, estándar o ficha técnica'),
        ('media', 'Media: hermano o galga'),
        ('baja', 'Baja: promedio o configuración del centro'),
        ('ninguna', 'Sin dato')], default='ninguna',
        help='La peor fuente que intervino, propias o heredadas.')
    detalle = fields.Char(help='De dónde salió el número, en una línea.')
    n_ordenes = fields.Integer(string='Órdenes usadas')
    unidades_hora = fields.Float(
        digits=(16, 2), string='Unidades / hora',
        help='1 ÷ horas propias; la velocidad que implica el dato.')
    calculado_el = fields.Datetime(readonly=True)
    # Captura manual
    horas_manual = fields.Float(
        digits=(16, 6), string='Horas manuales / u',
        help='Capturado por Ingeniería. Manda sobre lo medido y lo estimado '
             'mientras esté vigente.')
    manual_motivo = fields.Char(string='Motivo')
    manual_vigente_hasta = fields.Date(
        string='Vigente hasta',
        help='Vacío = sin caducidad. Al vencer, el recálculo vuelve a la '
             'mejor fuente disponible y deja el dato manual como referencia.')

    _product_centro_uniq = models.Constraint(
        'unique(product_id, centro_id, company_id)',
        'Ya hay horas de ese producto en ese centro.')

    @api.constrains('horas_manual', 'manual_motivo')
    def _check_manual(self):
        for rec in self:
            if rec.horas_manual and not (rec.manual_motivo or '').strip():
                raise UserError('Escriba el motivo de las horas manuales.')

    # ------------------------------------------------------------------
    # Ventanas y recetas
    # ------------------------------------------------------------------
    @api.model
    def _ventana(self, hasta=None):
        P = self.env['qb.parametro']
        meses = int(P.get_float('horas_historia_meses', 12)) or 12
        hasta = hasta or date.today()
        return hasta - relativedelta(months=meses), hasta

    @api.model
    def _tipos(self, key, default):
        """Ids de tipos de operación de un parámetro («409,410,411»)."""
        return {int(x) for x in self.env['qb.parametro'].get_list(key, default)
                if x.strip().isdigit()}

    @api.model
    def _bom_vigente(self, product, hasta=None):
        """La receta con la que hoy se fabrica el producto. 1.7.0: la del
        flujo en dos niveles (tipo de operación en `tipos_flujo_nuevo`:
        Clasificación final, Tela en kg, Tela en m) y, entre las demás, la de
        menor secuencia, como la escoge Odoo al crear la orden. Antes era la
        de más cantidad producida en la ventana, que seguía siendo la vieja
        en metros: la historia sirve para las horas medidas, no para escoger
        la receta. `hasta` se conserva por compatibilidad."""
        Bom = self.env['mrp.bom']
        if not product or product.type == 'service':
            return Bom
        boms = Bom.search(Bom._bom_find_domain(
            product, company_id=self.env.company.id))
        if not boms:
            return Bom
        nuevos = self._tipos('tipos_flujo_nuevo', '409,410,411')
        return boms.sorted(lambda b: (b.picking_type_id.id not in nuevos,
                                      b.sequence, not b.product_id, b.id))[:1]

    @api.model
    def _es_clasificacion(self, product, bom):
        """1.7.0: ¿La lista es la clasificación final de la tela en metros?
        Su tipo de operación es de clasificación (409/411) o consume la tela
        en proceso en kg del mismo código («I» + código)."""
        if not bom:
            return False
        if bom.picking_type_id.id in self._tipos('tipos_clasificacion', '409,411'):
            return True
        code = (product.default_code or '').strip()
        return bool(code) and any(
            codigo_metros(l.product_id.default_code) == code and es_proceso_kg(l.product_id)
            for l in bom.bom_line_ids)

    @api.model
    def _ficha_acabado(self, product):
        """La ficha técnica de acabado de la tela en metros (o de su tela en
        proceso en kg), si `quimibond_ficha_tecnica_tela` está instalado."""
        if 'ficha.tecnica.acabado' not in self.env or not product:
            return None
        code = (product.default_code or '').strip()
        dom = [('product_acabado_id', '=', product.id)]
        if es_proceso_kg(product):
            dom = ['|', ('product_proceso_id', '=', product.id)] + dom
            code = codigo_metros(code)
        if code:
            dom = ['|', ('articulo', '=', code)] + dom
        return self.env['ficha.tecnica.acabado'].search(dom, order='id desc', limit=1) or None

    @api.model
    def kg_por_unidad_ficha(self, product):
        """1.7.0: (kg por unidad del producto, ficha) con la conversión de
        la ficha de acabado: kg por metro = 1 ÷ rendimiento de la tela
        acabada (1000 ÷ (g/m² × ancho m)), decisión de Dirección del
        2026-10-30. Convertido a la unidad del producto (yardas…). (0, None)
        sin ficha, sin rendimiento o si la unidad no es de longitud."""
        ficha = self._ficha_acabado(product)
        rend = getattr(ficha, 'rendimiento_tela_acabada', 0.0) if ficha else 0.0
        metro = self.env.ref('uom.product_uom_meter', raise_if_not_found=False)
        if not rend or not metro or not product.uom_id._has_common_reference(metro):
            return 0.0, None
        return product.uom_id._compute_quantity(1.0, metro, round=False) / rend, ficha

    @api.model
    def _qty_linea(self, bom, line, product):
        """(cantidad del componente por unidad del producto, en la unidad
        del componente; nota). 1.7.0: en la clasificación final, la tela en
        proceso en kg se consume a 1 ÷ rendimiento de la ficha aunque la
        lista diga otra cosa (la validación `conversion_clasificacion` avisa
        de la diferencia): la conversión de kg a metros es una sola y es la
        de la ficha."""
        comp = line.product_id
        qty = line.product_uom_id._compute_quantity(
            line.product_qty, comp.uom_id, round=False,
            raise_if_failure=False) / self._qty_bom_en_uom_producto(bom, product)
        if es_proceso_kg(comp) and codigo_metros(comp.default_code) == \
                (product.default_code or '').strip():
            kg_u, ficha = self.kg_por_unidad_ficha(product)
            kg = self.env.ref('uom.product_uom_kgm')
            if kg_u:
                return (kg._compute_quantity(kg_u, comp.uom_id, round=False),
                        'ficha %s: %.4f kg por %s' % (ficha.display_name, kg_u,
                                                      product.uom_id.name))
        return qty, ''

    @api.model
    def _qty_bom_en_uom_producto(self, bom, product):
        qty = bom.product_qty or 1.0
        if bom.product_uom_id and product.uom_id and \
                bom.product_uom_id != product.uom_id and \
                bom.product_uom_id._has_common_reference(product.uom_id):
            qty = bom.product_uom_id._compute_quantity(qty, product.uom_id,
                                                       round=False)
        return qty or 1.0

    # ------------------------------------------------------------------
    # Fuentes
    # ------------------------------------------------------------------
    @api.model
    def _medido(self, centros, desde, hasta):
        """{(product_id, centro_id): (horas_por_unidad, n_ordenes, unidades)}
        desde las órdenes de trabajo terminadas en la ventana, por orden de
        fabricación (horas de la orden ÷ unidades terminadas), filtradas por
        la banda de velocidad del centro, y promediadas ponderando por
        unidades."""
        out = {}
        self.env.flush_all()
        for centro in centros:
            wcs = centro.workcenter_ids.ids
            if not wcs:
                continue
            # Leer los campos del centro ANTES de la consulta: un acceso al
            # ORM entre execute() y fetchall() dispara su propio SELECT en el
            # mismo cursor y se pierde el resultado (en producción los
            # centros llegan de un search sin caché y nada salía «medido»).
            # 1.7.0: banda por unidad del producto de la orden (kg/h, m/h
            # u horas por baño en tintorería).
            bandas = centro._bandas()
            fuera = centro.fuera_de_banda
            self.env.cr.execute("""
                WITH horas AS (
                    SELECT wo.production_id, SUM(wo.duration) / 60.0 AS h
                    FROM mrp_workorder wo
                    WHERE wo.workcenter_id IN %s AND wo.state = 'done'
                      AND wo.date_finished >= %s AND wo.date_finished < %s
                    GROUP BY wo.production_id
                ), producido AS (
                    SELECT sm.production_id, SUM(sm.quantity) AS qty
                    FROM stock_move sm
                    JOIN mrp_production mo ON mo.id = sm.production_id
                    WHERE sm.state = 'done' AND sm.product_id = mo.product_id
                    GROUP BY sm.production_id
                )
                SELECT mo.product_id, h.h, p.qty, pt.uom_id
                FROM horas h
                JOIN mrp_production mo ON mo.id = h.production_id
                JOIN producido p ON p.production_id = mo.id
                JOIN product_product pp ON pp.id = mo.product_id
                JOIN product_template pt ON pt.id = pp.product_tmpl_id
                WHERE mo.state = 'done' AND h.h > 0 AND p.qty > 0
            """, (tuple(wcs), desde, hasta))
            acum = {}
            for pid, h, qty, uom_id in self.env.cr.fetchall():
                if fuera(bandas, uom_id, qty, h):
                    continue
                a = acum.setdefault(pid, [0.0, 0.0, 0])
                a[0] += h
                a[1] += qty
                a[2] += 1
            for pid, (h, qty, n) in acum.items():
                out[(pid, centro.id)] = (h / qty, n, qty)
        return out

    @api.model
    def _fuentes_externas(self, products, centros, desde, hasta):
        """{(product_id, centro_id): (horas_por_unidad, fuente, detalle, n)}
        que otro módulo mide mejor que las órdenes de trabajo (el pesaje de
        rollos, `qb_tejido_ritmo`). Aquí vacío; quien lo extienda manda
        sobre `medido` y por debajo de `manual`."""
        return {}

    @api.model
    def _ficha_procesos(self, centros):
        """1.5.0: velocidades por artículo de la ficha técnica en las
        máquinas de `centros` → {(clave, centro_id): [(máquina, m/h)]}, con
        clave = id del producto ligado o código del artículo (y, si el
        código es en kg, el de la tela acabada en metros). m/h con merma
        (`metros_hora_neto`: velocidad m/min × 60 × (1 − merma %)), porque
        lo que sale de la máquina es el metro que se vende. Un proceso sin
        velocidad no cuenta (dato que la planta no ha dado). Vacío si la
        ficha técnica no está instalada."""
        out = {}
        if 'ficha.tecnica.proceso' not in self.env:
            return out
        por_wc = {}
        for c in centros:
            for wc in c.workcenter_ids:
                por_wc.setdefault(wc.id, []).append(c.id)
        if not por_wc:
            return out
        procesos = self.env['ficha.tecnica.proceso'].search(
            [('workcenter_id', 'in', list(por_wc))])
        for pr in procesos:
            mh = pr.metros_hora_neto
            if mh <= 0:
                continue
            claves = {pr.articulo}
            if pr.product_id:
                claves.add(pr.product_id.id)
            # 1.6.4: los procesos van con el código en kg («I…») desde que
            # la producción va en kg hasta la Clasificación final; la
            # velocidad vale igual para la tela acabada en metros.
            if codigo_metros(pr.articulo):
                claves.add(codigo_metros(pr.articulo))
            for cid in por_wc[pr.workcenter_id.id]:
                for k in claves:
                    out.setdefault((k, cid), []).append((pr.workcenter_id.name, mh))
        return out

    @api.model
    def _ficha(self, product, centro, fichas):
        """1.5.0: horas por unidad desde la velocidad del artículo en las
        máquinas del centro (`_ficha_procesos`). Si corre en varias (rama 1
        y rama 2), el promedio de sus horas por metro: la ficha no dice
        cuánto va a cada una. Solo productos en unidades de longitud.
        (horas, detalle) o None."""
        if not fichas:
            return None
        maquinas = fichas.get((product.id, centro.id)) or \
            fichas.get(((product.default_code or '').strip(), centro.id))
        if not maquinas:
            return None
        metro = self.env.ref('uom.product_uom_meter', raise_if_not_found=False)
        uom = product.uom_id
        if not metro or not uom._has_common_reference(metro):
            return None
        factor = uom._compute_quantity(1.0, metro, round=False)
        maquinas = sorted(set(maquinas))
        h_m = sum(1.0 / mh for _n, mh in maquinas) / len(maquinas)
        return (factor * h_m, 'Ficha técnica: %s m/h con merma' % ', '.join(
            '%s %s' % (n, '{:,.0f}'.format(mh)) for n, mh in maquinas))

    @api.model
    def _driver(self, product, centro):
        """Horas por unidad según la configuración del centro, para los
        centros que todavía no tienen órdenes de trabajo ni operaciones en
        las recetas: `m_velocidad` (1 ÷ velocidad de rama, productos en
        metros), `kg_ciclo` (horas por carga ÷ kg por carga, productos en
        kg) y `fijo_unidad` (horas fijas por unidad). (horas, detalle) o
        None."""
        uom = product.uom_id
        metro = self.env.ref('uom.product_uom_meter', raise_if_not_found=False)
        kg = self.env.ref('uom.product_uom_kgm', raise_if_not_found=False)
        if centro.driver == 'm_velocidad' and centro.velocidad_m_h > 0 and \
                metro and uom._has_common_reference(metro):
            factor = uom._compute_quantity(1.0, metro, round=False)
            return (factor / centro.velocidad_m_h,
                    'Velocidad del centro: %.0f m/h' % centro.velocidad_m_h)
        if centro.driver == 'kg_ciclo' and kg and uom._has_common_reference(kg):
            # 1.7.0: por producto y jet (ficha > fórmula de la lista) entre
            # los kg por baño; si corre en varios jets, el promedio de sus
            # horas por kg. La carga del centro, solo como último recurso.
            bom = self._bom_vigente(product)
            jets = []
            for wc in centro.workcenter_ids:
                bano = self._minutos_bano(product, bom, wc)
                cap = self._kg_bano(product, wc, kg) if bano else None
                if bano and cap and cap[0] > 0:
                    jets.append((wc.name, bano[0], cap[0]))
            if jets:
                h_kg = sum(m / 60.0 / c for _n, m, c in jets) / len(jets)
                factor = uom._compute_quantity(1.0, kg, round=False)
                return (factor * h_kg, 'Baño por jet: %s' % ', '.join(
                    '%s %.0f min ÷ %.0f kg' % j for j in jets))
        if centro.driver == 'kg_ciclo' and centro.kg_por_carga > 0 and \
                centro.horas_por_carga > 0 and kg and uom._has_common_reference(kg):
            factor = uom._compute_quantity(1.0, kg, round=False)
            return (factor * centro.horas_por_carga / centro.kg_por_carga,
                    'Carga del centro: %.0f kg en %.1f h'
                    % (centro.kg_por_carga, centro.horas_por_carga))
        if centro.driver == 'fijo_unidad' and centro.horas_por_unidad > 0:
            return (centro.horas_por_unidad,
                    'Horas fijas del centro: %.4f h/u' % centro.horas_por_unidad)
        return None

    @api.model
    def diagnostico_medido(self, product_id=None, centro_id=None):
        """Qué ve `_medido` en esta base: ventana, máquinas del centro,
        filas que devuelve la consulta y el dato del producto pedido. Para
        revisar por MCP por qué un producto con órdenes no sale «medido»."""
        Centro = self.env['qb.centro']
        centros = Centro.browse(centro_id) if centro_id else Centro.search(
            [('company_id', '=', self.env.company.id), ('nature', '=', 'directo')])
        desde, hasta = self._ventana()
        medido = self._medido(centros, desde, hasta)
        out = {'desde': str(desde), 'hasta': str(hasta),
               'company_id': self.env.company.id,
               'centros': [{'id': c.id, 'code': c.code, 'maquinas': len(c.workcenter_ids),
                            'banda': [c.velocidad_min, c.velocidad_max],
                            'bandas': {str(k): list(v) for k, v in c._bandas().items()},
                            'productos_medidos': sum(1 for k in medido if k[1] == c.id)}
                           for c in centros]}
        if product_id:
            out['producto'] = {
                '%s' % c.code: list(medido.get((product_id, c.id), ())) for c in centros}
            self.env.flush_all()
            self.env.cr.execute("""
                SELECT COUNT(*), COALESCE(SUM(wo.duration), 0) / 60.0
                FROM mrp_workorder wo
                JOIN mrp_production mo ON mo.id = wo.production_id
                WHERE mo.product_id = %s AND wo.state = 'done' AND mo.state = 'done'
                  AND wo.date_finished >= %s AND wo.date_finished < %s
            """, (product_id, desde, hasta))
            n, h = self.env.cr.fetchone()
            self.env.cr.execute("""
                SELECT COUNT(*), COALESCE(SUM(sm.quantity), 0)
                FROM stock_move sm JOIN mrp_production mo ON mo.id = sm.production_id
                WHERE mo.product_id = %s AND sm.product_id = mo.product_id
                  AND sm.state = 'done' AND mo.state = 'done'
            """, (product_id,))
            nm, q = self.env.cr.fetchone()
            out['producto'].update(ordenes_trabajo=n, horas_wo=float(h or 0),
                                   movimientos_terminados=nm, qty_terminada=float(q or 0))
        return out

    @api.model
    def _minutos_bano(self, product, bom, wc):
        """1.7.0: (minutos por baño, origen) del artículo en el jet, con la
        prioridad de «Programar Tintorería»: tiempo de ciclo del artículo en
        ese jet en la ficha técnica (horas) > minutos de la fórmula de la
        lista en ese jet (`tintoreria.formula.tiempo`). None si no hay."""
        if 'ficha.tecnica.proceso.valor' in self.env:
            productos = product
            ficha = self._ficha_acabado(product)
            if ficha:
                productos |= ficha.product_acabado_id | ficha.product_proceso_id
            codigos = [c for c in productos.mapped('default_code') if c]
            valor = self.env['ficha.tecnica.proceso.valor'].search([
                ('code', '=', 'tiempo_ciclo'), ('workcenter_id', '=', wc.id),
                ('valor', '>', 0),
                '|', ('proceso_id.product_id', 'in', productos.ids),
                ('proceso_id.articulo', 'in', codigos)], limit=1)
            if valor:
                factor = 1.0 if (valor.unit or 'h').lower() in ('min', 'mins', 'minutos') else 60.0
                return valor.valor * factor, 'tiempo de ciclo de la ficha'
        formula = bom.formula_tintoreria_id if bom and 'formula_tintoreria_id' in bom._fields else None
        if formula and 'tintoreria.formula.tiempo' in self.env:
            tiempo = self.env['tintoreria.formula.tiempo'].search([
                ('formula_id', '=', formula.id), ('workcenter_id', '=', wc.id)], limit=1)
            if tiempo:
                return tiempo.minutos, 'fórmula %s' % formula.display_name
        return None

    @api.model
    def _kg_bano(self, product, wc, uom):
        """1.7.0: (unidades de `uom` por baño, origen) del artículo en el
        jet: capacidad capturada en el centro de trabajo para el producto
        (`mrp.workcenter.capacity`) > kg por carga de la ficha técnica del
        jet. None si no hay."""
        caps = wc.capacity_ids.filtered(
            lambda c: c.product_id == product and c.capacity > 0)
        if not caps:
            caps = wc.capacity_ids.filtered(
                lambda c: not c.product_id and c.capacity > 0
                and c.product_uom_id in (uom, product.uom_id))
        if caps:
            c = caps[0]
            return (c.product_uom_id._compute_quantity(c.capacity, uom, round=False,
                                                       raise_if_failure=False),
                    'capacidad del centro de trabajo')
        if hasattr(wc, '_ft_capacidad_carga') and hasattr(product, '_ft_rendimiento'):
            kg, motivo = wc._ft_capacidad_carga(product._ft_rendimiento()[0], product=product)
            kg_uom = self.env.ref('uom.product_uom_kgm')
            if kg and uom._has_common_reference(kg_uom):
                return kg_uom._compute_quantity(kg, uom, round=False), 'ficha: %s' % motivo
        return None

    @api.model
    def _minutos_operacion(self, product, bom, op, centro):
        """1.7.0: (minutos por unidad del producto, detalle) de una operación
        de la lista, como los calcula Odoo (`_get_duration_expected`):
        (preparación + limpieza + tiempo del ciclo) ÷ unidades por ciclo
        (capacidad del centro de trabajo para el producto; sin capacidad, la
        cantidad de la lista) ÷ eficiencia. Antes: tiempo ÷ cantidad de la
        lista, y una tintorería de 210 min por baño en una lista de 1 kg
        salía de 210 min por kg.

        En la tintorería (centro `kg_ciclo`) el tiempo es por baño: tiempo
        de ciclo de la ficha > fórmula de la lista en ese jet > operación,
        entre los kg por baño (capacidad del centro de trabajo, ficha del
        jet o kg por carga del centro de costo, en ese orden)."""
        wc = op.workcenter_id
        uom = bom.product_uom_id or product.uom_id
        qty_bom = bom.product_qty or 1.0
        cap, inicio, fin = wc._get_capacity(product, uom, qty_bom)
        minutos, origen = op.time_cycle, 'operación'
        if centro.driver == 'kg_ciclo' and es_tintoreria(op):
            bano = self._minutos_bano(product, bom, wc)
            if bano:
                (minutos, origen), inicio, fin = bano, 0.0, 0.0
            kg = self._kg_bano(product, wc, uom)
            if kg and kg[0] > 0:
                cap = kg[0]
            elif centro.kg_por_carga > 0:
                kg_uom = self.env.ref('uom.product_uom_kgm')
                if uom._has_common_reference(kg_uom):
                    cap = kg_uom._compute_quantity(centro.kg_por_carga, uom, round=False)
        if not minutos and not (inicio or fin):
            return 0.0, ''
        cap = cap or 1.0
        eff = (wc.time_efficiency or 100.0) / 100.0
        por_u_bom = (inicio + fin + minutos) / cap / eff
        por_u = por_u_bom * qty_bom / self._qty_bom_en_uom_producto(bom, product)
        det = '%s %s: %s min (%s) ÷ %s %s por ciclo' % (
            op.name, wc.name, '{:,.4g}'.format(inicio + fin + minutos), origen,
            '{:,.4g}'.format(cap), uom.name)
        if eff != 1.0:
            det += ' ÷ %.0f %% de eficiencia' % (eff * 100)
        return por_u, det

    @api.model
    def _estandar_detalle(self, product, bom, centro):
        """(horas por unidad, detalle) desde las operaciones de la receta en
        los centros de trabajo del centro (`_minutos_operacion`)."""
        if not bom:
            return 0.0, ''
        wcs = set(centro.workcenter_ids.ids)
        minutos, dets = 0.0, []
        for op in bom.operation_ids:
            if op.workcenter_id.id in wcs:
                m, d = self._minutos_operacion(product, bom, op, centro)
                if m:
                    minutos += m
                    dets.append(d)
        return minutos / 60.0, '; '.join(dets)

    @api.model
    def _estandar(self, product, bom, centro):
        """Horas por unidad desde las operaciones de la receta en los
        centros de trabajo del centro."""
        return self._estandar_detalle(product, bom, centro)[0]

    @api.model
    def _promedios_centro(self, centros, desde, hasta):
        """{centro_id: {uom_id: horas_por_unidad}} y, por galga,
        {(centro_id, galga): {uom_id: horas_por_unidad}}: promedio de todo lo
        producido en el centro (y en sus máquinas de cada galga) en la
        ventana, dentro de la banda."""
        prom, prom_galga = {}, {}
        self.env.flush_all()
        Tag = self.env['mrp.workcenter.tag']
        for centro in centros:
            wcs = centro.workcenter_ids
            if not wcs:
                continue
            galga_de_wc = {}
            for wc in wcs:
                for tag in wc.tag_ids:
                    m = re.match(r'GALGA\s*(\d+)', tag.name or '', re.I)
                    if m:
                        galga_de_wc[wc.id] = int(m.group(1))
            bandas = centro._bandas()
            fuera = centro.fuera_de_banda
            self.env.cr.execute("""
                WITH horas AS (
                    SELECT wo.production_id, wo.workcenter_id,
                           SUM(wo.duration) / 60.0 AS h
                    FROM mrp_workorder wo
                    WHERE wo.workcenter_id IN %s AND wo.state = 'done'
                      AND wo.date_finished >= %s AND wo.date_finished < %s
                    GROUP BY wo.production_id, wo.workcenter_id
                ), producido AS (
                    SELECT sm.production_id, SUM(sm.quantity) AS qty
                    FROM stock_move sm
                    JOIN mrp_production mo ON mo.id = sm.production_id
                    WHERE sm.state = 'done' AND sm.product_id = mo.product_id
                    GROUP BY sm.production_id
                ), tot AS (
                    SELECT production_id, SUM(h) AS h FROM horas GROUP BY 1
                )
                SELECT h.workcenter_id, pp_t.uom_id, h.h, p.qty * h.h / t.h
                FROM horas h
                JOIN tot t ON t.production_id = h.production_id
                JOIN mrp_production mo ON mo.id = h.production_id
                JOIN product_product pp ON pp.id = mo.product_id
                JOIN product_template pp_t ON pp_t.id = pp.product_tmpl_id
                JOIN producido p ON p.production_id = mo.id
                WHERE mo.state = 'done' AND t.h > 0 AND p.qty > 0
            """, (tuple(wcs.ids), desde, hasta))
            acum = {}
            acum_g = {}
            for wc_id, uom_id, h, qty in self.env.cr.fetchall():
                if fuera(bandas, uom_id, qty, h):
                    continue
                a = acum.setdefault(uom_id, [0.0, 0.0])
                a[0] += h
                a[1] += qty
                g = galga_de_wc.get(wc_id)
                if g:
                    ag = acum_g.setdefault((g, uom_id), [0.0, 0.0])
                    ag[0] += h
                    ag[1] += qty
            prom[centro.id] = {u: h / q for u, (h, q) in acum.items() if q}
            for (g, u), (h, q) in acum_g.items():
                if q:
                    prom_galga.setdefault((centro.id, g), {})[u] = h / q
        _ = Tag
        return prom, prom_galga

    # ------------------------------------------------------------------
    # Recálculo
    # ------------------------------------------------------------------
    @api.model
    def _productos_a_calcular(self):
        Bom = self.env['mrp.bom']
        company = self.env.company
        boms = Bom.search([('active', '=', True),
                           ('company_id', 'in', (False, company.id)),
                           ('type', '=', 'normal')])
        prods = boms.mapped('product_id') | \
            boms.filtered(lambda b: not b.product_id).mapped(
                'product_tmpl_id.product_variant_ids')
        desde, hasta = self._ventana()
        self.env.cr.execute("""
            SELECT DISTINCT product_id FROM mrp_production
            WHERE state = 'done' AND company_id = %s
              AND date_finished >= %s AND date_finished < %s
        """, (company.id, desde, hasta))
        prods |= self.env['product.product'].browse(
            [r[0] for r in self.env.cr.fetchall()])
        return prods.filtered(lambda p: p.active)

    @api.model
    def recalcular(self, products=None):
        """Recalcula las horas por unidad de `products` (default: todo lo
        fabricado) en todos los centros directos. Devuelve las filas."""
        company = self.env.company
        Centro = self.env['qb.centro']
        centros = Centro.search([('company_id', '=', company.id),
                                 ('nature', '=', 'directo')])
        if not centros:
            return self.browse()
        if products is None:
            products = self._productos_a_calcular()
        desde, hasta = self._ventana()
        medido = self._medido(centros, desde, hasta)
        externas = self._fuentes_externas(products, centros, desde, hasta)
        prom, prom_galga = self._promedios_centro(centros, desde, hasta)
        fichas = self._ficha_procesos(centros)
        hoy = fields.Date.today()

        # Hermanos: por raíz de nomenclatura, los productos con dato medido.
        hermanos = {}  # (raiz, centro_id) → [(horas, n, uom_id)]
        Prod = self.env['product.product']
        for (pid, cid), (h, n, qty) in medido.items():
            p = Prod.browse(pid)
            raiz = raiz_hermano(p.default_code)
            if raiz:
                hermanos.setdefault((raiz, cid), []).append((h, qty, p.uom_id.id))

        existentes = {(r.product_id.id, r.centro_id.id): r for r in self.search(
            [('product_id', 'in', products.ids),
             ('company_id', '=', company.id)])}
        boms = {p.id: self._bom_vigente(p, hasta) for p in products}
        propias = {}   # (pid, cid) → (horas, fuente, detalle, n)
        for p in products:
            bom = boms[p.id]
            for c in centros:
                rec = existentes.get((p.id, c.id))
                manual = rec and rec.horas_manual and (
                    not rec.manual_vigente_hasta or rec.manual_vigente_hasta >= hoy)
                if manual:
                    propias[(p.id, c.id)] = (
                        rec.horas_manual, 'manual',
                        'Manual: %s' % (rec.manual_motivo or ''), 0)
                    continue
                # 1.7.0: la operación real de la lista manda. Si la receta
                # vigente tiene operaciones y ninguna en este centro, el
                # producto no pasa por aquí aunque la historia (la ruta vieja)
                # o la ficha digan otra cosa: en el flujo en dos niveles la
                # rama la carga la tela en proceso en kg, no la de metros.
                if self._ops_fuera(bom, c):
                    continue
                ext = externas.get((p.id, c.id))
                if ext:
                    propias[(p.id, c.id)] = ext
                    continue
                m = medido.get((p.id, c.id))
                if m:
                    h, n, qty = m
                    propias[(p.id, c.id)] = (
                        h, 'medido', '%d órdenes, %s %s, %.2f %s/h'
                        % (n, '{:,.0f}'.format(qty), p.uom_id.name,
                           1 / h if h else 0, p.uom_id.name), n)
                    continue
                e, e_det = self._estandar_detalle(p, bom, c)
                if e:
                    propias[(p.id, c.id)] = (
                        e, 'estandar', ('Receta %s: %s' % (bom.display_name, e_det))[:250], 0)
                    continue
                # Que la ficha diga a qué velocidad corre el artículo en una
                # máquina del centro también dice que pasa por ahí.
                f = self._ficha(p, c, fichas)
                if f:
                    propias[(p.id, c.id)] = (f[0], 'ficha', f[1], 0)
                    continue
                # Sin receta en este centro y sin órdenes: el centro no es
                # de este producto, salvo que la receta vigente lo pase por
                # ahí (operación sin tiempo) o haya hermanos/galga que digan
                # que sí.
                raiz = raiz_hermano(p.default_code)
                hs = hermanos.get((raiz, c.id)) if raiz else None
                hs = [x for x in (hs or []) if x[2] == p.uom_id.id]
                if hs:
                    tot_h = sum(h * q for h, q, _u in hs)
                    tot_q = sum(q for _h, q, _u in hs)
                    propias[(p.id, c.id)] = (
                        tot_h / tot_q, 'hermano',
                        'Hermanos %s…: %d productos' % (raiz, len(hs)), len(hs))
                    continue
                if not self._centro_aplica(p, bom, c):
                    continue
                g = galga_de(p.default_code)
                pg = prom_galga.get((c.id, g), {}).get(p.uom_id.id) if g else None
                if pg:
                    propias[(p.id, c.id)] = (
                        pg, 'galga', 'Máquinas galga %d: %.2f %s/h'
                        % (g, 1 / pg, p.uom_id.name), 0)
                    continue
                pc = prom.get(c.id, {}).get(p.uom_id.id)
                if pc:
                    propias[(p.id, c.id)] = (
                        pc, 'estimado', 'Promedio del centro: %.2f %s/h'
                        % (1 / pc, p.uom_id.name), 0)
                    continue
                dr = self._driver(p, c)
                if dr:
                    propias[(p.id, c.id)] = (dr[0], 'driver', dr[1], 0)

        # Herencia por receta (memo sobre todos los productos conocidos).
        memo = {}

        def total(pid, cid, pila=()):
            """(horas, peor_rango) propias + heredadas."""
            key = (pid, cid)
            if key in memo:
                return memo[key]
            if pid in pila:
                return 0.0, 4
            prod = Prod.browse(pid)
            bom = boms.get(pid)
            if bom is None:
                bom = self._bom_vigente(prod, hasta)
                boms[pid] = bom
            own = propias.get(key)
            if own is None and pid not in {p.id for p in products} and \
                    (pid, cid) not in medido:
                # Producto fuera del lote (componente): calcula propias al vuelo.
                own = self._propias_al_vuelo(prod, bom, Centro.browse(cid),
                                             medido, hermanos, prom, prom_galga,
                                             fichas)
                propias[key] = own
            h = own[0] if own else 0.0
            rango = RANGO[own[1]] if own else 4
            heredado = 0.0
            if bom:
                for line in bom.bom_line_ids:
                    comp = line.product_id
                    if comp.type == 'service' or line._skip_bom_line(prod):
                        continue
                    hc, rc = total(comp.id, cid, pila + (pid,))
                    if hc:
                        heredado += hc * self._qty_linea(bom, line, prod)[0]
                        rango = max(rango, rc) if rango < 4 else rc
            if not h and not heredado:
                memo[key] = (0.0, 4)
            else:
                memo[key] = (h + heredado, rango if (h or heredado) else 4)
            return memo[key]

        ahora = fields.Datetime.now()
        filas = self.browse()
        for p in products:
            for c in centros:
                own = propias.get((p.id, c.id))
                tot, rango = total(p.id, c.id)
                heredadas = tot - (own[0] if own else 0.0)
                if not own and not heredadas:
                    rec = existentes.get((p.id, c.id))
                    if rec and not rec.horas_manual:
                        rec.unlink()
                    continue
                vals = {
                    'horas_propias': own[0] if own else 0.0,
                    'fuente': own[1] if own else 'ninguna',
                    'detalle': own[2] if own else 'Solo heredadas de la receta',
                    'n_ordenes': own[3] if own else 0,
                    'horas_heredadas': heredadas,
                    'horas_unidad': tot,
                    'unidades_hora': (1 / own[0]) if own and own[0] else 0.0,
                    'calidad': CALIDAD[rango],
                    'calculado_el': ahora,
                }
                rec = existentes.get((p.id, c.id))
                if rec:
                    rec.write(vals)
                else:
                    rec = self.create(dict(vals, product_id=p.id, centro_id=c.id,
                                           company_id=company.id))
                filas |= rec
        return filas

    ETAPA_DRIVER = {'H': 'workorder', 'I': 'kg_ciclo', 'J': 'm_velocidad'}
    # 1.7.0: flujo en dos niveles. La tela en proceso en kg («I» + código)
    # pasa por tintorería (con devanado), acabado e inspección; la tela en
    # metros que sale de la clasificación final, solo por inspección y
    # empaque (lo demás lo hereda de la de kg).
    DRIVERS_PROCESO_KG = {'kg_ciclo', 'm_velocidad', 'fijo_unidad'}
    DRIVERS_CLASIFICACION = {'fijo_unidad'}

    @api.model
    def _ops_fuera(self, bom, centro):
        """True si la receta tiene operaciones y ninguna en el centro."""
        return bool(bom and bom.operation_ids) and not any(
            op.workcenter_id in centro.workcenter_ids for op in bom.operation_ids)

    @api.model
    def _drivers_por_codigo(self, product, bom):
        """Drivers de los centros por los que pasa el producto según su
        código (DAT P-D02-01) y el tipo de su receta. None si el código no
        dice la ruta (fuera de la norma o importado con prefijo)."""
        ref = (product.default_code or '').replace(' ', '')
        if es_proceso_kg(product):
            return set(self.DRIVERS_PROCESO_KG)
        m = NOMEN_RE.match(ref)
        if not m or ref.startswith('I'):
            return None
        etapa = m.group(6)
        if etapa == 'J' and self._es_clasificacion(product, bom):
            return set(self.DRIVERS_CLASIFICACION)
        return {self.ETAPA_DRIVER[etapa]} if etapa in self.ETAPA_DRIVER else set()

    @api.model
    def centros_esperados(self, product, centros, hasta=None):
        """Centros directos por los que pasa el producto según su receta,
        receta abajo. Con operaciones en la receta vigente, sus centros de
        trabajo (1.7.0: la operación real manda sobre el código); sin
        operaciones, la etapa del código (H tejido, I tintorería, J acabado;
        la tela en proceso en kg: tintorería, acabado e inspección; la tela
        en metros de la clasificación final: inspección y empaque). Un código
        fuera de la norma o un importado (prefijo de unidad) no aporta etapa:
        la ruta no se conoce por el código. Un terminado natural cuya receta
        va directo del crudo no espera tintorería."""
        esperados = centros.browse()
        vistos = set()

        def recorrer(prod):
            nonlocal esperados
            if prod.id in vistos:
                return
            vistos.add(prod.id)
            bom = self._bom_vigente(prod, hasta)
            drivers = self._drivers_por_codigo(prod, bom)
            if drivers is None:
                return
            if bom and bom.operation_ids:
                wcs = bom.operation_ids.workcenter_id
                esperados |= centros.filtered(lambda c: c.workcenter_ids & wcs)
            else:
                esperados |= centros.filtered(lambda c: c.driver in drivers)
            for line in (bom.bom_line_ids if bom else ()):
                recorrer(line.product_id)

        recorrer(product)
        return esperados

    @api.model
    def _centro_aplica(self, product, bom, centro):
        """¿Este producto pasa por este centro? Con operaciones en la receta
        vigente, solo si alguna está en el centro (aunque sin tiempo); sin
        operaciones, si la etapa de su código corresponde al driver del
        centro (`_drivers_por_codigo`). Sin nomenclatura ni operación, no."""
        if bom and bom.operation_ids:
            return not self._ops_fuera(bom, centro)
        drivers = self._drivers_por_codigo(product, bom)
        return bool(drivers) and centro.driver in drivers

    @api.model
    def _propias_al_vuelo(self, product, bom, centro, medido, hermanos, prom,
                          prom_galga, fichas=None):
        if self._ops_fuera(bom, centro):
            return None
        m = medido.get((product.id, centro.id))
        if m:
            h, n, qty = m
            return (h, 'medido', '%d órdenes' % n, n)
        e, e_det = self._estandar_detalle(product, bom, centro)
        if e:
            return (e, 'estandar', e_det[:250], 0)
        f = self._ficha(product, centro, fichas)
        if f:
            return (f[0], 'ficha', f[1], 0)
        raiz = raiz_hermano(product.default_code)
        hs = [x for x in hermanos.get((raiz, centro.id), [])
              if x[2] == product.uom_id.id] if raiz else []
        if hs:
            tot_h = sum(h * q for h, q, _u in hs)
            tot_q = sum(q for _h, q, _u in hs)
            return (tot_h / tot_q, 'hermano', 'Hermanos %s…' % raiz, len(hs))
        if not self._centro_aplica(product, bom, centro):
            return None
        g = galga_de(product.default_code)
        pg = prom_galga.get((centro.id, g), {}).get(product.uom_id.id) if g else None
        if pg:
            return (pg, 'galga', 'Máquinas galga %d' % g, 0)
        pc = prom.get(centro.id, {}).get(product.uom_id.id)
        if pc:
            return (pc, 'estimado', 'Promedio del centro', 0)
        dr = self._driver(product, centro)
        if dr:
            return (dr[0], 'driver', dr[1], 0)
        return None

    # ------------------------------------------------------------------
    @api.model
    def cron_recalcular(self):
        for company in self.env['res.company'].search([]):
            self.with_company(company).recalcular()

    def action_recalcular(self):
        products = self.mapped('product_id') or None
        self.recalcular(products)
        return True

    def action_recalcular_todo(self):
        self.recalcular()
        return {
            'type': 'ir.actions.act_window',
            'name': 'Horas por centro',
            'res_model': 'qb.producto.horas',
            'view_mode': 'list,form',
        }

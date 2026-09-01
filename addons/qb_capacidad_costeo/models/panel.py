# -*- coding: utf-8 -*-
"""Panel: puerta de entrada del módulo.

Está armado alrededor de las decisiones que se toman con él, en este orden:

1. ¿Cómo va el AÑO? — ventas, margen de productos, ociosidad y resultado,
   acumulados de enero al último mes calculado.
2. ¿Le puedo creer al número? — la brecha contra la contabilidad, contra el
   ±2% que el propio módulo fija como umbral para decidir precios. Va antes
   de los márgenes a propósito: enseñarlos sin decir de qué lado de esa raya
   estamos es invitar a decidir con lo que el modelo declara que no cuadra.
3. ¿Dónde está el techo? — utilización por MÁQUINA, no por centro. Un centro
   promedia la saturada con las vacías: acabado lee 88% y su rama UNITECH va
   al 94%; tintorería lee 48% y su HTJ-1 al 81%.
4. ¿Dónde se gana y dónde se pierde? — clientes y productos, 12 meses.
5. ¿Qué hago hoy? — ordenado por el dinero en juego.
6. Configuración, colapsada: es de la puesta a punto, no del día a día.

Las ventanas se dicen SIEMPRE en la etiqueta. Mezclarlas en silencio —unas
tarjetas del mes y unas tablas de doce— es lo que tenía antes, y no hay
forma de que el que lee lo note.
"""
import logging

from dateutil.relativedelta import relativedelta

from odoo import fields, models

from .producto_reportes import MESES_ES, mes_es, money

_logger = logging.getLogger(__name__)

OK = '✅'
WARN = '⚠️'
BAD = '❌'


class QbCosteoPanel(models.TransientModel):
    _name = 'qb.costeo.panel'
    _description = 'Panel de capacidad y costeo'

    negocio_html = fields.Html(compute='_compute_panel', sanitize=False)
    estado_html = fields.Html(compute='_compute_panel', sanitize=False)
    kpi_html = fields.Html(compute='_compute_panel', sanitize=False)

    def _compute_panel(self):
        for rec in self:
            rec.negocio_html = rec._build_negocio()
            rec.estado_html = rec._build_estado()
            rec.kpi_html = rec._build_kpis()

    # ------------------------------------------------------------------
    # ¿Cómo va el negocio? — lo primero que se ve al abrir: el mes en
    # curso en tarjetas, quién gana y quién pierde en 12 meses, y qué
    # necesita acción. La configuración vive abajo, colapsada: es de la
    # puesta a punto, no del día a día.
    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    # Piezas de presentación
    #
    # Paleta de ESTADO, no de series (ver la guía de visualización): cuatro
    # roles fijos y validados contra el fondo claro. Dos de ellos no llegan a
    # 3:1 de contraste a propósito, así que la regla es que el color NUNCA va
    # solo: cada uno viaja con su icono y su palabra. Quien no distinga los
    # tonos lee exactamente lo mismo.
    # ------------------------------------------------------------------
    BIEN = '#0ca30c'
    OJO = '#fab219'
    SERIO = '#ec835a'
    MAL = '#d03b3b'
    TINTA = '#1f1f1c'
    TENUE = '#6b6b66'
    RIEL = '#e9e7e2'
    # Divergente para valores con signo: azul y rojo, polos que se leen
    # opuestos, con el cero en gris. Nunca un arcoíris ni un tono en el medio.
    POS = '#2f6fb5'
    NEG = '#d03b3b'

    @staticmethod
    def _compacto(v):
        """$108.6M — un titular no se lee con nueve dígitos.

        La cifra exacta viaja en el `title`, que es donde se va a mirar
        cuando alguien la necesite para cuadrar contra algo.
        """
        signo = '-' if v < 0 else ''
        a = abs(v)
        if a >= 1e6:
            return '%s$%.1fM' % (signo, a / 1e6)
        if a >= 1e3:
            return '%s$%.0fK' % (signo, a / 1e3)
        return money(v)

    @classmethod
    def _tile(cls, titulo, valor, sub='', color=None, icono='', exacto=None):
        color = color or cls.TINTA
        return (
            '<div title="%s" style="display:inline-block;min-width:190px;'
            'margin:0 8px 8px 0;padding:12px 14px;border:1px solid #dee2e6;'
            'border-radius:10px;border-left:4px solid %s;vertical-align:top;">'
            '<div style="font-size:11px;text-transform:uppercase;'
            'letter-spacing:.03em;color:%s;">%s</div>'
            '<div style="font-size:26px;font-weight:600;line-height:1.15;'
            'color:%s;">%s %s</div>'
            '<div style="font-size:11px;color:%s;margin-top:2px;">%s</div>'
            '</div>'
            % (exacto or '', color, cls.TENUE, titulo, cls.TINTA, icono,
               valor, cls.TENUE, sub))

    @classmethod
    def _franja(cls, icono, titulo, texto, color):
        return (
            '<div style="border:1px solid %s;border-left:5px solid %s;'
            'border-radius:8px;padding:10px 14px;margin:4px 0 12px 0;'
            'background:#fbfbfa;">'
            '<div style="font-weight:600;color:%s;">%s %s</div>'
            '<div style="font-size:13px;color:%s;margin-top:3px;">%s</div>'
            '</div>' % (color, color, cls.TINTA, icono, titulo, cls.TENUE,
                        texto))

    @classmethod
    def _barra(cls, pct, color, ancho=180):
        """Barra de magnitud contra una referencia de 100.

        El valor va FUERA, en tinta de texto: dentro tendría que competir
        con el color de la barra y dos de los cuatro estados no dan
        contraste suficiente. La marca lleva la identidad, el texto el dato.
        """
        lleno = max(0.0, min(pct, 100.0))
        exceso = max(0.0, min(pct - 100.0, 100.0))
        return (
            '<span style="display:inline-block;width:%spx;height:10px;'
            'background:%s;border-radius:5px;vertical-align:middle;'
            'overflow:hidden;">'
            '<span style="display:block;height:10px;width:%.1f%%;'
            'background:%s;border-radius:5px;"></span></span>'
            '%s'
            % (ancho, cls.RIEL, lleno, color,
               '<span style="color:%s;font-size:11px;"> ▸%.0f</span>'
               % (cls.MAL, exceso) if exceso else ''))

    @staticmethod
    def _mini_tabla(titulo, filas, nota=''):
        """filas = [(icono, nombre, monto, pct)]"""
        if not filas:
            return ''
        cuerpo = ''.join(
            '<tr><td style="padding:2px 6px;">%s %s</td>'
            '<td style="padding:2px 6px;text-align:right;">%s</td>'
            '<td style="padding:2px 6px;text-align:right;">%+.1f&#37;</td>'
            '</tr>' % (icono, nombre, money(monto), pct)
            for icono, nombre, monto, pct in filas)
        return (
            '<div style="display:inline-block;vertical-align:top;'
            'min-width:300px;margin:4px 12px 4px 0;">'
            '<h6 style="margin-bottom:2px;">%s</h6>'
            '<div style="font-size:11px;color:#6b6b66;margin-bottom:2px;">'
            '%s</div>'
            '<table class="table table-sm" style="font-size:12px;">'
            '<tbody>%s</tbody></table></div>' % (titulo, nota, cuerpo))

    # ------------------------------------------------------------------
    # La ventana: el AÑO EN CURSO
    # ------------------------------------------------------------------
    def _periodos_del_anio(self):
        """Períodos del año en curso que YA están calculados.

        No basta con filtrar por año: septiembre existe en la conciliación
        desde su primer día, con ventas del mayor y el costeo todavía sin
        correr. Sumarlo mete ingresos sin su costo y el resultado del año
        sale inflado justo el día que alguien lo abre. La lista la manda
        `qb.costo.factores`: si un mes no tiene factores, no tiene modelo, y
        no entra.
        """
        ene = fields.Date.today().replace(month=1, day=1)
        return self.env['qb.costo.factores'].search(
            [('period', '>=', ene)], order='period')

    def _bloque_anio(self, periodos):
        conc = self.env['qb.costo.conciliacion'].search(
            [('period', 'in', periodos.mapped('period'))])
        if not conc:
            return ''
        ventas = sum(conc.mapped('gl_ventas'))
        margen = sum(conc.mapped('resultado_modelo'))
        ocioso = sum(conc.mapped('ociosidad_ias2'))
        resultado = sum(conc.mapped('resultado_par'))
        pct = 100.0 * resultado / ventas if ventas else 0.0
        rango = '%s–%s %s' % (
            MESES_ES[min(periodos.mapped('period')).month - 1][:3].lower(),
            MESES_ES[max(periodos.mapped('period')).month - 1][:3].lower(),
            max(periodos.mapped('period')).year)
        sub = '%s · %s meses cerrados' % (rango, len(conc))
        color = self.BIEN if pct >= 5 else self.OJO if pct >= 0 else self.MAL
        icono = '🟢' if pct >= 5 else '🟡' if pct >= 0 else '🔴'
        return (
            '<h5>El año</h5>'
            + self._tile('Ventas del año', self._compacto(ventas), sub,
                         self.TINTA, exacto=money(ventas))
            + self._tile(
                'Margen de productos', self._compacto(margen),
                'lo que dejan los vendidos sobre la capacidad que SÍ usan',
                self.TINTA, exacto=money(margen))
            + self._tile(
                'Ociosidad acumulada', self._compacto(-ocioso),
                'la planta parada; la paga el período, no el producto',
                self.SERIO, '🟠', money(-ocioso))
            + self._tile(
                'Resultado del año', self._compacto(resultado),
                'margen − ociosidad · %+.1f&#37; sobre venta' % pct,
                color, icono, money(resultado)))

    def _bloque_confianza(self, periodos):
        """La pregunta previa a todas: ¿le puedo creer a estos números?

        El módulo fija su propio criterio en `brecha_pct`: bajo ±2% sirve
        para decidir precios, arriba de ahí primero hay que cerrar la
        brecha. Enseñar márgenes por cliente y producto sin decir en qué
        lado de esa raya estamos es invitar a decidir con lo que el propio
        modelo declara que todavía no cuadra.
        """
        conc = self.env['qb.costo.conciliacion'].search(
            [('period', 'in', periodos.mapped('period'))])
        if not conc:
            return ''
        ventas = sum(conc.mapped('gl_ventas'))
        brecha = sum(conc.mapped('brecha_neta'))
        if not ventas:
            return ''
        pct = abs(100.0 * brecha / ventas)
        if pct <= 2.0:
            return self._franja(
                '✅', 'El modelo cuadra con la contabilidad',
                'Quedan %s sin explicar, el %.1f&#37; de la venta del año. '
                'Debajo de 2&#37; los márgenes de abajo se pueden usar como '
                'cifra, no solo para comparar.' % (money(brecha), pct),
                self.BIEN)
        color = self.MAL if pct > 10 else self.SERIO
        return self._franja(
            '⛔' if pct > 10 else '🟠',
            'Todavía no decidas precios con estos márgenes',
            'Entre el modelo y el mayor quedan <b>%s sin explicar</b> — el '
            '<b>%.1f&#37;</b> de la venta del año, contra el ±2&#37; que el '
            'propio módulo pide para fijar precios. Los márgenes de abajo '
            'sirven para <b>comparar entre sí</b> (quién deja más y quién '
            'menos), no como cifra absoluta. Cerrar la brecha se trabaja en '
            'Conciliación.' % (money(brecha), pct),
            color)

    def _bloque_meses(self, periodos):
        """Qué meses dejaron y cuáles no.

        Una sola serie con signo, así que va con la pareja divergente (azul
        arriba, rojo abajo) y el cero en gris: los polos tienen que leerse
        opuestos. Sin leyenda —el título nombra la serie— y con etiqueta
        directa solo en el mejor y el peor mes, no en los doce.
        """
        conc = self.env['qb.costo.conciliacion'].search(
            [('period', 'in', periodos.mapped('period'))], order='period')
        if len(conc) < 2:
            return ''
        vals = [(c.period, c.resultado_par) for c in conc]
        tope = max(abs(v) for _, v in vals) or 1.0
        mejor = max(vals, key=lambda x: x[1])[0]
        peor = min(vals, key=lambda x: x[1])[0]
        cols = ''
        for period, v in vals:
            alto = min(abs(v) / tope * 34.0, 34.0)
            pos = v >= 0
            etiqueta = ''
            if period in (mejor, peor):
                etiqueta = ('<div style="font-size:10px;color:%s;'
                            'white-space:nowrap;">%s</div>'
                            % (self.TENUE, self._compacto(v)))
            barra = (
                '<div style="height:%.1fpx;background:%s;'
                'border-radius:%s;" title="%s: %s"></div>'
                % (alto, self.POS if pos else self.NEG,
                   '3px 3px 0 0' if pos else '0 0 3px 3px',
                   mes_es(period), money(v)))
            cols += (
                '<div style="display:inline-block;width:38px;'
                'vertical-align:bottom;text-align:center;margin-right:2px;">'
                '<div style="height:46px;display:flex;flex-direction:column;'
                'justify-content:flex-end;">%s%s</div>'
                '<div style="height:1px;background:#cfcdc7;"></div>'
                '<div style="height:40px;">%s</div>'
                '<div style="font-size:10px;color:%s;">%s</div></div>'
                % (etiqueta if pos else '', barra if pos else '',
                   barra if not pos else '',
                   self.TENUE, MESES_ES[period.month - 1][:3].lower()))
        return (
            '<h6 style="margin-top:14px;">Resultado por mes '
            '<span style="font-weight:400;color:%s;font-size:12px;">'
            '(margen de productos − ociosidad)</span></h6>'
            '<div style="white-space:nowrap;overflow-x:auto;padding:2px 0;">'
            '%s</div>' % (self.TENUE, cols))

    def _bloque_gana_pierde(self):
        env = self.env
        html = ''
        sem = lambda pct: ('🔴' if pct < 0 else '🟡' if pct < 5 else '🟢')
        nota = '12 meses móviles — ventana distinta a las tarjetas de arriba'
        clientes = env['qb.cliente.rentabilidad'].search([])
        if clientes:
            orden = clientes.sorted('margen_neto_12m')
            html += '<div>'
            html += self._mini_tabla(
                'Clientes que más DEJAN',
                [(sem(c.margen_neto_pct), c.partner_id.name or '',
                  c.margen_neto_12m, c.margen_neto_pct)
                 for c in reversed(orden[-5:])], nota)
            html += self._mini_tabla(
                'Clientes que más CUESTAN',
                [(sem(c.margen_neto_pct), c.partner_id.name or '',
                  c.margen_neto_12m, c.margen_neto_pct)
                 for c in orden[:5] if c.margen_neto_12m < 0], nota)
            html += '</div>'
        productos = env['qb.producto.rentabilidad'].search([])
        if productos:
            orden = productos.sorted('margen_neto_12m')
            html += '<div>'
            html += self._mini_tabla(
                'Productos que más DEJAN',
                [(sem(p.margen_neto_pct),
                  p.product_id.default_code or p.product_id.name or '',
                  p.margen_neto_12m, p.margen_neto_pct)
                 for p in reversed(orden[-5:])], nota)
            html += self._mini_tabla(
                'Productos que más CUESTAN',
                [(sem(p.margen_neto_pct),
                  p.product_id.default_code or p.product_id.name or '',
                  p.margen_neto_12m, p.margen_neto_pct)
                 for p in orden[:5] if p.margen_neto_12m < 0], nota)
            html += '</div>'
        if html:
            html = ('<h5 style="margin-top:16px;">Dónde se gana y dónde se '
                    'pierde</h5>' + html)
        return html

    def _bloque_cobertura(self, periodos):
        """¿La contribución del año ya cubrió los fijos del año?"""
        if not periodos:
            return ''
        contrib = sum(self.env['qb.costo.producto'].search([
            ('period', 'in', periodos.mapped('period'))
        ]).mapped('contrib_total'))
        fijos = sum(f.fab_pool_month + f.entretela_pool_month
                    + f.op_pool_month for f in periodos)
        if not fijos:
            return ''
        pct = 100.0 * contrib / fijos
        color = self.BIEN if pct >= 100 else (
            self.SERIO if pct >= 80 else self.MAL)
        icono = '🟢' if pct >= 100 else '🟠' if pct >= 80 else '🔴'
        cola = ('arriba de aquí todo es utilidad'
                if pct >= 100 else 'faltan %s' % money(max(fijos - contrib, 0)))
        return (
            '<h6 style="margin-top:16px;">Cobertura de fijos del año</h6>'
            '<div style="font-size:13px;color:%s;margin-bottom:4px;">'
            'Contribución %s contra fijos %s '
            '(fabricación + entretelas + operación)</div>'
            '%s <b style="color:%s;">%s %.0f&#37;</b> '
            '<span style="font-size:12px;color:%s;">— %s</span>'
            % (self.TENUE, money(contrib), money(fijos),
               self._barra(pct, color, 260), self.TINTA, icono, pct,
               self.TENUE, cola))

    def _bloque_acciones(self):
        """Lo que hay que hacer, ordenado por el dinero que está en juego."""
        env = self.env
        acciones = []
        clientes = env['qb.cliente.rentabilidad'].search([])
        rojos = clientes.filtered(lambda c: c.margen_neto_12m < 0)
        if rojos:
            monto = abs(sum(rojos.mapped('margen_neto_12m')))
            acciones.append((monto, '🔴',
                             '<b>%s clientes con margen neto negativo</b> '
                             'que suman %s al año — ábrelos en «Rentabilidad '
                             'por cliente» y recotiza.'
                             % (len(rojos), money(-monto))))
        saturadas = env['qb.familia.carga'].search(
            [('utilization_pct', '>=', 90.0)])
        if saturadas:
            acciones.append((
                10 ** 9, '🏭',
                '<b>%s familia(s) de máquinas arriba del 90&#37;</b>: %s. '
                'Antes de prometer volumen, revisa que la máquina que hace '
                'ESE artículo tenga lugar — el promedio del centro no lo ve.'
                % (len(saturadas),
                   ', '.join('%s (%.0f&#37;)' % (f.familia_id.code,
                                                 f.utilization_pct)
                             for f in saturadas[:4]))))
        n_pesos_mal = env['qb.peso.auditoria'].search_count(
            [('estado', 'in', ('critico', 'revisar'))])
        if n_pesos_mal:
            acciones.append((
                10 ** 8, '⚖️',
                '<b>%s productos con peso dudoso</b> en la Auditoría de '
                'pesos — un peso malo infla o esconde su costo.'
                % n_pesos_mal))
        hoy = fields.Date.today()
        por_vencer = env['qb.cotizacion'].search_count([
            ('state', 'in', ('draft', 'done')),
            ('validez_hasta', '!=', False),
            ('validez_hasta', '<=', hoy + relativedelta(days=15))])
        if por_vencer:
            acciones.append((
                10 ** 7, '⏳',
                '<b>%s cotizaciones vivas vencen en 15 días</b> — revísalas '
                'en «Cotizaciones guardadas».' % por_vencer))
        if not acciones:
            return ''
        acciones.sort(reverse=True)
        return ('<h5 style="margin-top:16px;">Qué necesita acción</h5>'
                '<ul style="font-size:13px;margin-bottom:0;">%s</ul>'
                % ''.join('<li>%s %s</li>' % (ic, txt)
                          for _, ic, txt in acciones))

    def _build_negocio(self):
        periodos = self._periodos_del_anio()
        if not periodos:
            return ('<p style="color:%s;">Todavía no hay ningún mes de este '
                    'año con el costeo calculado — corre «Recalcular '
                    'costeo».</p>' % self.TENUE)
        return (self._bloque_anio(periodos)
                + self._bloque_confianza(periodos)
                + self._bloque_meses(periodos)
                + self._bloque_gana_pierde()
                + self._bloque_cobertura(periodos)
                + self._bloque_acciones())

    # ------------------------------------------------------------------
    # El techo de la planta
    # ------------------------------------------------------------------
    # Rampa de UN tono para descomponer la capacidad instalada en sus
    # tres tramos: usada, disponible y parada. Es una rampa y no tres
    # colores porque los tramos son un ORDEN —de lo que ya trabaja a lo
    # que ni siquiera está dotado—, no tres categorías; y porque una
    # rampa se lee igual con daltonismo: el verde y el ámbar que pedía
    # el instinto quedan a ΔE 1.1 en deuteranopía, o sea el mismo color.
    # El estado de cada máquina no viaja en la barra: lo carga su icono
    # con su palabra, al lado.
    CAP_USADA = '#1c4f82'
    CAP_LIBRE = '#5093ce'
    CAP_PARADA = '#9cc4e4'

    @classmethod
    def _barra_capacidad(cls, usada, libre, parada, escala=1.0, ancho=210):
        """Barra apilada de capacidad instalada, a escala dentro del centro.

        El ancho es proporcional a la capacidad INSTALADA de la máquina
        contra la mayor de su centro, no un 100 por ciento para todas:
        así se ve de un golpe que la HTJ-1 es tres veces la HTJ-4 y no
        solo que las dos van medio llenas. Entre centros no se compara —
        tejido y tintorería miden en kg y acabado en metros—, y por eso
        la tabla va agrupada por centro y cada grupo tiene su escala.
        """
        total = usada + libre + parada
        if total <= 0 or escala <= 0:
            return ''
        tramos = ((usada, cls.CAP_USADA), (libre, cls.CAP_LIBRE),
                  (parada, cls.CAP_PARADA))
        piezas = ''.join(
            '<span style="display:block;flex:%.4f 0 0;background:%s;'
            'height:11px;"></span>' % (v, color)
            for v, color in tramos if v > 0)
        return (
            '<span style="display:inline-flex;gap:2px;width:%.1fpx;'
            'height:11px;border-radius:3px;overflow:hidden;'
            'vertical-align:middle;background:%s;">%s</span>'
            % (ancho * min(escala, 1.0), cls.RIEL, piezas))

    @classmethod
    def _leyenda_capacidad(cls):
        def sw(color, palabra, glosa):
            return (
                '<span style="margin-right:16px;white-space:nowrap;">'
                '<span style="display:inline-block;width:11px;height:11px;'
                'border-radius:3px;background:%s;vertical-align:middle;">'
                '</span> <b style="color:%s;">%s</b> '
                '<span style="color:%s;">%s</span></span>'
                % (color, cls.TINTA, palabra, cls.TENUE, glosa))
        return (
            '<div style="font-size:11px;margin:2px 0 6px 0;">%s%s%s</div>'
            % (sw(cls.CAP_USADA, 'usada', 'lo que corrió'),
               sw(cls.CAP_LIBRE, 'disponible', 'dotada y libre — se vende hoy'),
               sw(cls.CAP_PARADA, 'parada',
                  'instalada sin dotar — se libera con gente')))

    # ------------------------------------------------------------------
    # El techo de la planta, máquina por máquina
    # ------------------------------------------------------------------
    def _build_kpis(self):
        """Capacidad INSTALADA vs DOTADA vs USADA, por máquina.

        Son tres niveles y no dos, y confundirlos cuesta dinero de dos
        maneras opuestas:

        * **Instalada** es lo que la planta compró. No absorbe costo por
          existir: meter una máquina parada en el denominador de la
          absorción inventaría ociosidad que nadie decidió tener.
        * **Dotada** (la capacidad normal de la NIC 2) es lo que de
          verdad se corre. Es la que absorbe, y su parte no usada sí es
          ociosidad con costo.
        * **Usada** es la carga. Es una asignación, no una medición.

        La diferencia entre instalada y dotada es una decisión pendiente
        —contratar o mover gente—, no un residuo, y hasta ahora no se
        veía en ningún lado: la HTJ-5 son 91,000 kg/mes que existen,
        están pagados y no aparecían.

        Y se lee por MÁQUINA, no por centro. Un centro promedia la
        saturada con las vacías y contesta que sí a un pedido que la
        planta no puede correr: acabado lee 88 y su rama UNITECH va al
        94; tintorería lee 48 y su HTJ-1 al 81.
        """
        env = self.env
        filas = env['qb.familia.carga'].search([])
        if not filas:
            return self._build_kpis_por_centro()
        util_centro = {
            o.centro_id.id: o.utilization_pct
            for o in env['qb.ociosidad'].search([])}
        ocioso = sum(env['qb.ociosidad'].search([]).mapped('idle_cost_month'))

        # Agrupado por centro: dentro de un centro las unidades son
        # comparables (kg en tejido y tintorería, m en acabado) y entre
        # centros no. Sumar los cuatro sería sumar kilos con metros.
        centros = filas.mapped('centro_id').sorted(
            lambda c: (c.sequence, c.code or ''))
        cuerpo = ''
        for centro in centros:
            del_centro = filas.filtered(lambda r: r.centro_id == centro)
            cuerpo += self._grupo_de_centro(centro, del_centro, util_centro)

        return (self._encabezado_techo(filas, ocioso)
                + self._leyenda_capacidad()
                + '<table class="table table-sm" style="font-size:13px;'
                  'max-width:900px;"><tbody>%s</tbody></table>' % cuerpo
                + '<p style="font-size:11px;color:%s;margin-top:-6px;">'
                  'Todo en unidades por MES —kg en tejido y tintorería, '
                  'metros en acabado— y por eso la tabla va agrupada por '
                  'centro: entre centros no se suma. El costeo trabaja por '
                  'período; multiplicar por doce solo invitaría a comparar '
                  'contra las ventas del año, que es otra cosa. La carga es '
                  'una asignación, no una medición: Odoo no registra en qué '
                  'máquina corrió cada orden.</p>' % self.TENUE)

    def _encabezado_techo(self, filas, ocioso):
        """Las dos frases que hay que leer aunque no se mire la tabla."""
        peor = filas.sorted(lambda r: -r.utilization_pct)[:1]
        if not peor:
            return ''
        if peor.utilization_pct < 1.0:
            # Todas en cero: no hay «la más apretada» que nombrar, y
            # decirlo igual sería señalar una máquina al azar. Pasa en una
            # base recién instalada y cuando la ventana de producción se
            # queda sin OPs.
            cabeza = (
                '<p style="margin-bottom:2px;color:%s;">Ninguna máquina '
                'registra carga en la ventana de producción — o no hay '
                'órdenes terminadas todavía, o sus artículos no están '
                'catalogados en ninguna familia.</p>' % self.SERIO)
        else:
            cabeza = (
                '<p style="margin-bottom:2px;">La máquina más apretada de '
                'la planta es <b>%s</b> al <b>%.0f&#37;</b> de su capacidad '
                'dotada, con %s %s/mes disponibles.</p>'
                % (peor.familia_id.code, peor.utilization_pct,
                   '{:,.0f}'.format(peor.free_month_units),
                   self._unidad(peor.centro_id)))

        # Lo parado, por centro y con nombre: es lo que se puede decidir.
        paradas = filas.filtered(lambda r: r.capacity_parked_units > 0)
        if paradas:
            por_centro = []
            for centro in paradas.mapped('centro_id').sorted(
                    lambda c: (c.sequence, c.code or '')):
                del_centro = paradas.filtered(lambda r: r.centro_id == centro)
                total = sum(del_centro.mapped('capacity_parked_units'))
                quienes = ', '.join(
                    f.familia_id.code
                    for f in del_centro.sorted(
                        lambda r: -r.capacity_parked_units)[:3])
                por_centro.append(
                    '<b>%s %s/mes</b> en %s (%s)'
                    % ('{:,.0f}'.format(total), self._unidad(centro),
                       centro.code or centro.name, quienes))
            cabeza += (
                '<p style="margin-bottom:2px;">Instalado y sin dotar: %s. '
                'No cuesta ociosidad —una máquina parada no absorbe— pero '
                'es capacidad ya pagada que se libera con gente, no con '
                'inversión.</p>' % '; '.join(por_centro))

        sin_velocidad = filas.filtered(
            lambda r: r.capacity_installed_units <= 0)
        if sin_velocidad:
            cabeza += (
                '<p style="margin-bottom:2px;color:%s;">Sin velocidad '
                'capturada, así que su capacidad instalada no se puede '
                'leer: %s. Mientras siga así, el techo de su centro está '
                'subestimado.</p>'
                % (self.SERIO,
                   ', '.join(sorted(sin_velocidad.mapped('familia_id.code')))))

        return cabeza + (
            '<p style="margin-bottom:6px;color:%s;">Costo de la capacidad '
            '<b>dotada y ociosa</b>: <b>%s/mes</b>. La parada no entra aquí: '
            'no absorbe.</p>' % (self.TENUE, money(ocioso)))

    @staticmethod
    def _unidad(centro):
        return 'kg' if centro.driver_principal == 'peso' else 'm'

    def _grupo_de_centro(self, centro, del_centro, util_centro):
        """Encabezado del centro con sus totales, y sus máquinas debajo."""
        unidad = self._unidad(centro)
        num = '{:,.0f}'.format
        instalada = sum(del_centro.mapped('capacity_installed_units'))
        usada = sum(del_centro.mapped('load_month_units'))
        libre = sum(del_centro.mapped('free_month_units'))
        parada = sum(del_centro.mapped('capacity_parked_units'))
        tope = max(del_centro.mapped('capacity_installed_units') or [0.0])

        # El promedio del centro se dice UNA vez y aquí, no colgado de
        # cada máquina: pegado a siete renglones dejaba de leerse. Y va
        # con el rango de sus máquinas al lado, que es lo que enseña por
        # qué el promedio engaña — no que sea 48, sino que adentro hay
        # una en 84 y otra en 17.
        activas = del_centro.filtered('activa')
        uc = util_centro.get(centro.id)
        aviso = ''
        if uc is not None and activas:
            utils = activas.mapped('utilization_pct')
            aviso = (' · el promedio del centro lee %.0f&#37;, y sus '
                     'máquinas van de %.0f&#37; a %.0f&#37;'
                     % (uc, min(utils), max(utils)))
        col = ('<td style="padding:3px 8px;text-align:right;'
               'white-space:nowrap;%s">%s</td>')
        fila = (
            '<tr style="background:#f6f5f2;">'
            '<td style="padding:5px 8px;"><b>%s</b> '
            '<span style="font-size:11px;color:%s;">%s · %s</span></td>'
            '<td style="padding:5px 8px;font-size:11px;color:%s;">'
            'instalada / usada / disponible / parada%s</td>'
            % (centro.code or centro.name, self.TENUE, centro.name, unidad,
               self.TENUE, aviso))
        for v in (instalada, usada, libre, parada):
            fila += col % ('font-weight:600;', num(v))
        fila += '</tr>'

        for f in del_centro.sorted(lambda r: -r.utilization_pct):
            fila += self._fila_de_maquina(f, tope)
        return fila

    def _fila_de_maquina(self, f, tope):
        num = '{:,.0f}'.format
        u = f.utilization_pct
        if not f.activa:
            icono, banda, color = '⚪', 'parada — no dotada', self.TENUE
        elif u >= 95:
            icono, banda, color = '🔴', 'saturada', self.MAL
        elif u >= 85:
            icono, banda, color = '🟠', 'ajustada', self.SERIO
        elif u >= 55:
            icono, banda, color = '🟢', 'sana', self.BIEN
        else:
            icono, banda, color = '🟡', 'ociosa', self.OJO

        sub = banda if not f.activa else '%.0f&#37; · %s' % (u, banda)

        escala = (f.capacity_installed_units / tope) if tope > 0 else 0.0
        barra = self._barra_capacidad(
            f.load_month_units, f.free_month_units,
            f.capacity_parked_units, escala=escala)
        if not barra:
            barra = ('<span style="font-size:11px;color:%s;">sin velocidad '
                     'capturada</span>' % self.SERIO)

        col = ('<td style="padding:3px 8px;text-align:right;'
               'white-space:nowrap;color:%s;">%s</td>')
        celdas = ''
        for v, tinta in ((f.capacity_installed_units, self.TINTA),
                         (f.load_month_units, self.TENUE),
                         (f.free_month_units, self.TENUE),
                         (f.capacity_parked_units,
                          self.SERIO if f.capacity_parked_units > 0
                          else self.TENUE)):
            celdas += col % (tinta, num(v))
        return (
            '<tr>'
            '<td style="padding:3px 8px;white-space:nowrap;">%s <b>%s</b>'
            '<div style="font-size:11px;color:%s;">%s</div></td>'
            '<td style="padding:3px 8px;">%s</td>%s</tr>'
            % (icono, f.familia_id.code, color, sub, barra, celdas))

    def _build_kpis_por_centro(self):
        """Sin familias dadas de alta no queda más que el promedio del
        centro, que es justo lo que engaña. Se dice."""
        env = self.env
        balance = env['qb.balance'].search([])
        if not balance or not any(b.capacity_equiv_m for b in balance):
            return ('<p style="color:%s;">Sin datos de capacidad todavía — '
                    'completa el semáforo de abajo.</p>' % self.TENUE)
        cuerpo = ''
        for b in balance.sorted(lambda r: -r.utilization_pct):
            color = (self.MAL if b.is_bottleneck else self.BIEN
                     if b.utilization_pct < 70 else self.SERIO)
            icono = '🔴' if b.is_bottleneck else (
                '🟢' if b.utilization_pct < 70 else '🟠')
            cuerpo += (
                '<tr><td style="padding:3px 8px;">%s <b>%s</b>%s</td>'
                '<td style="padding:3px 8px;">%s</td>'
                '<td style="padding:3px 8px;text-align:right;">'
                '<b>%.0f&#37;</b></td></tr>'
                % (icono, b.centro_id.code,
                   ' — cuello' if b.is_bottleneck else '',
                   self._barra(b.utilization_pct, color), b.utilization_pct))
        return (
            '<p style="margin-bottom:6px;color:%s;">Sin familias de máquinas '
            'dadas de alta, esto es el promedio del centro — y un centro '
            'promedia la máquina saturada con las vacías. Dalas de alta en '
            'Configuración para leer el techo real.</p>'
            '<table class="table table-sm" style="font-size:13px;'
            'max-width:600px;"><tbody>%s</tbody></table>'
            % (self.SERIO, cuerpo))

    # ------------------------------------------------------------------
    # Semáforo de configuración
    # ------------------------------------------------------------------
    # Prefijo `_estado_` y no `_check_`: Odoo usa `_check_*` para sus
    # propios hooks del ORM (`_check_company`, `_check_access`,
    # `_check_recursion`…) y un check del panel que cayera en uno de
    # esos nombres lo sobrescribiría en silencio.
    # Orden de presentación del estado de configuración. Agregar un
    # check es escribir su método y ponerlo aquí: el cuerpo de
    # `_build_estado` no se toca, y así no vuelve a crecer a 677
    # líneas como lo hizo entre la 1.10 y la 1.58.
    _CHECKS_ESTADO = (
        '_estado_pesos_medidos',
        '_estado_workcenters_ligados',
        '_estado_capacidad_por_centro',
        '_estado_maestro_de_pesos',
        '_estado_clasificacion_de_cuentas',
        '_estado_absorcion_por_workcenter',
        '_estado_renta_contractual_vs_gl',
        '_estado_aduana_landed_costs',
        '_estado_mp_conciliable',
        '_estado_capacidad_normal',
        '_estado_cuello_de_botella',
        '_estado_ventana_fabril_tras_corte',
        '_estado_ajuste_de_metros',
        '_estado_periodos_vs_maestro_de_pesos',
        '_estado_cola_de_recalculo',
        '_estado_avco_de_importados',
        '_estado_bom_vs_consumo_real',
        '_estado_produccion_arriba_de_capacidad',
        '_estado_capacidad_capturada_vs_horario',
        '_estado_familias_de_maquinas',
        '_estado_capacidad_instalada',
        '_estado_peso_de_la_ficha',
        '_estado_factores_calculados',
        '_estado_costos_por_producto',
    )

    def _build_estado(self):
        """Corre los checks del registro y arma la tabla colapsable.

        Cada check se aísla: uno que truene sale como renglón rojo con su
        error en vez de tumbar el panel entero. El panel es la pantalla de
        entrada del módulo, así que perder los otros veintitantos checks
        —y el resto del tablero— por un dato roto en uno solo era el peor
        canje posible. El error sigue visible y sigue en el log; lo que
        cambia es que no se lleva la pantalla por delante.
        """
        checks = []
        for nombre in self._CHECKS_ESTADO:
            try:
                checks.extend(getattr(self, nombre)())
            except Exception as exc:
                _logger.exception(
                    'qb_capacidad_costeo: el check %s del panel falló',
                    nombre)
                checks.append((
                    BAD, nombre.replace('_estado_', '').replace('_', ' '),
                    'el check falló y no se pudo evaluar: %s' % exc))
        return self._render_estado(checks)

    @staticmethod
    def _render_estado(checks):
        rows = ''.join(
            '<tr><td style="padding:4px 8px;">%s</td>'
            '<td style="padding:4px 8px;"><b>%s</b></td>'
            '<td style="padding:4px 8px;">%s</td></tr>' % c for c in checks)
        table = '<table class="table table-sm"><tbody>%s</tbody></table>' % rows
        # Siempre colapsado: la configuración es de la puesta a punto, no
        # del día a día. El resumen dice si hay algo que atender.
        pendientes = [c for c in checks if c[0] != OK]
        if not pendientes:
            resumen = '%s <b>Configuración completa</b>' % OK
        else:
            resumen = ('%s <b>Configuración: %s punto(s) por revisar</b>'
                       % (WARN if all(c[0] == WARN for c in pendientes)
                          else BAD, len(pendientes)))
        return ('<details><summary style="cursor:pointer;">%s — clic para '
                'el detalle</summary>%s</details>' % (resumen, table))

    def _estado_pesos_medidos(self):
        env = self.env
        checks = []
        # 1. Maestro de pesos medidos/ingeniería cargado (nativo, sin Supabase)
        n_pesos = env['qb.producto.peso'].search_count(
            [('source', 'in', ('manual', 'cvu'))])
        checks.append((OK if n_pesos else WARN,
                       'Pesos medidos capturados',
                       '%s productos con peso medido/ingeniería' % n_pesos
                       if n_pesos else
                       'corre "Cargar maestro de pesos" en Configuración '
                       '(sin esto el peso se estima del código)'))
        return checks

    def _estado_workcenters_ligados(self):
        env = self.env
        checks = []
        # 2. Workcenters ligados a centros
        total_wc = env['mrp.workcenter'].search_count([])
        linked_wc = len(env['qb.costeo.centro'].search([]).mapped('workcenter_ids'))
        icon = OK if linked_wc else (WARN if total_wc else BAD)
        checks.append((icon, 'Workcenters ligados a centros',
                       '%s de %s máquinas — sin esto el factor $/kg y la '
                       'capacidad de TEJIDO salen en 0. Se liga solo con '
                       '"Importar desde Supabase".' % (linked_wc, total_wc)))
        return checks

    def _estado_capacidad_por_centro(self):
        env = self.env
        checks = []
        # 3. Centros fabriles sin fuente de capacidad
        sin_capacidad = env['qb.costeo.centro'].search([
            ('nature', '!=', 'admin'),
            ('workcenter_ids', '=', False),
        ]).filtered(lambda c: not env['qb.turno.config'].search_count(
            [('centro_id', '=', c.id)]))
        checks.append((OK if not sin_capacidad else WARN,
                       'Capacidad por centro',
                       'todos los centros tienen workcenters o turnos'
                       if not sin_capacidad else
                       'sin workcenters NI turnos: %s — captúralos en '
                       '"Turnos / capacidad manual"'
                       % ', '.join(sin_capacidad.mapped('code'))))
        return checks

    def _estado_maestro_de_pesos(self):
        env = self.env
        checks = []
        # 4. Pesos por producto
        n_pesos = env['qb.producto.peso'].search_count([])
        checks.append((OK if n_pesos > 100 else WARN,
                       'Maestro de pesos',
                       '%s productos con peso — el import de Supabase trae '
                       '~2,758' % n_pesos))
        return checks

    def _estado_clasificacion_de_cuentas(self):
        env = self.env
        checks = []
        # 5. Cuentas sin clasificar
        mapped_accounts = env['qb.costeo.cuenta.map'].search([]).mapped('account_id')
        pending = env['account.account'].search([]).filtered(
            lambda a: a.code and a.code[:1] in '4567' and a not in mapped_accounts)
        checks.append((OK if len(pending) < 10 else WARN,
                       'Clasificación de cuentas',
                       '%s cuentas de resultados sin clasificar' % len(pending)))
        return checks

    def _estado_absorcion_por_workcenter(self):
        env = self.env
        checks = []
        # 5.4 Régimen híbrido: un centro que Odoo ya capitaliza NO puede
        # seguir en el pool del módulo, y su cuenta de costos aplicados tiene
        # que estar clasificada para poder restarla. Las dos mitades del
        # doble conteo son silenciosas.
        Centro = env['qb.costeo.centro']
        Clase = env['qb.costeo.cuenta.class']
        hoy = fields.Date.today()
        absorbidos = Centro.absorbidos_en(hoy)
        n_absorcion = Clase.search_count([('bucket', '=', 'absorcion_odoo')])
        ultimo = env['qb.costo.factores'].search(
            [], order='period DESC', limit=1)
        if absorbidos and not n_absorcion:
            checks.append((
                BAD, 'Absorción por workcenter',
                '%s ya capitaliza por workcenter, pero NINGUNA cuenta está '
                'clasificada como «Absorbido por Odoo». Sin ella el módulo no '
                'puede restar lo capitalizado y ese costo se cuenta DOS '
                'veces: una en el AVCO del producto y otra en el pool.'
                % ', '.join(absorbidos.mapped('code'))))
        elif absorbidos and ultimo and not ultimo.absorcion_bruta_month:
            checks.append((
                WARN, 'Absorción por workcenter',
                '%s está marcado como absorbido y la cuenta está clasificada, '
                'pero el período %s no registra nada capitalizado. O la '
                'tarifa por hora sigue en 0, o la fecha de corte se adelantó.'
                % (', '.join(absorbidos.mapped('code')), ultimo.period)))
        elif (absorbidos and ultimo and ultimo.absorcion_bruta_month
                and not ultimo.absorcion_pool_month):
            checks.append((
                WARN, 'Absorción por workcenter',
                'Odoo capitalizó $%s/mes de %s, pero sus cuentas etiquetadas '
                'y su renta contractual ya sumaban $%s/mes fuera del pool: la '
                'resta neta queda en 0. La tarifa por hora absorbe menos que '
                'el costo que el centro ya tenía identificado — revisa la '
                'tarifa, o que sus cuentas no estén etiquetadas de más.'
                % (f'{ultimo.absorcion_bruta_month:,.0f}',
                   ', '.join(absorbidos.mapped('code')),
                   f'{ultimo.absorcion_ya_fuera_month:,.0f}')))
        elif absorbidos:
            checks.append((
                OK, 'Absorción por workcenter',
                '%s fuera del pool desde %s; Odoo capitalizó $%s/mes, de los '
                'que $%s ya estaban excluidos por centro y renta → se restan '
                '$%s/mes'
                % (', '.join(absorbidos.mapped('code')),
                   min(absorbidos.mapped('fecha_absorcion')),
                   f'{ultimo.absorcion_bruta_month:,.0f}' if ultimo else '0',
                   f'{ultimo.absorcion_ya_fuera_month:,.0f}' if ultimo else '0',
                   f'{ultimo.absorcion_pool_month:,.0f}' if ultimo else '0')))
        elif ultimo and ultimo.absorcion_bruta_month:
            checks.append((
                BAD, 'Absorción por workcenter',
                'Odoo capitalizó $%s/mes de costos fabriles aplicados pero '
                'ningún centro está marcado como absorbido. Marca su modo de '
                'costeo y su fecha de corte, o el pool seguirá arrastrando un '
                'gasto que ya viaja dentro del inventario.'
                % f'{ultimo.absorcion_bruta_month:,.0f}'))
        return checks

    def _estado_renta_contractual_vs_gl(self):
        env = self.env
        checks = []
        # Se rederiva aquí: antes venía de un check anterior por vivir
        # todo en el mismo método, y ese orden implícito era una
        # dependencia que nadie declaró.
        Clase = env['qb.costeo.cuenta.class']
        # 5.5 Renta: contractual vs. GL — el doble conteo es silencioso
        renta_contractual = sum(env['qb.costeo.centro'].search([
            ('nature', 'in', ('fabril_directo', 'fabril_indirecto')),
        ]).mapped('renta_contractual_mxn'))
        if renta_contractual:
            # Una clase de PATRÓN que abarca una cuenta de renta junto a
            # cuarenta que no lo son no se arregla marcándola: marcarla saca
            # las cuarenta del pool. Se arregla separando la cuenta de renta
            # en su propia clasificación, que gana por más específica.
            sin_marcar = Clase.clases_con_renta_mezclada()
            if sin_marcar:
                detalle = (
                    'la renta se está contando DOS VECES: $%s/mes por '
                    'contrato y además por una cuenta de renta que vive '
                    'dentro de %s, en un bucket fabril. NO marques esas '
                    'clases: abarcan mucho más que la renta y marcarlas '
                    'sacaría todo del pool. Dale a la cuenta de renta su '
                    'propia clasificación específica y marca esa.'
                    % (f'{renta_contractual:,.0f}',
                       ', '.join(sin_marcar.mapped('name'))))
            else:
                detalle = ('renta contractual $%s/mes en el pool; las cuentas '
                           'de renta del GL están marcadas y fuera'
                           % f'{renta_contractual:,.0f}')
            checks.append((BAD if sin_marcar else OK,
                           'Renta sin doble conteo', detalle))
        return checks

    def _estado_aduana_landed_costs(self):
        env = self.env
        checks = []
        # Se rederiva aquí: antes venía de un check anterior por vivir
        # todo en el mismo método, y ese orden implícito era una
        # dependencia que nadie declaró.
        Clase = env['qb.costeo.cuenta.class']
        # 5.6 Aduana: ¿se está capitalizando con landed costs, o se queda en
        # resultados? El pedimento sabe a qué embarque pertenece; prorratearlo
        # con una fórmula le cobra al hilo el pedimento de una máquina.
        mal_ubicadas = Clase.cuentas_de_importacion_mal_ubicadas()
        n_import = Clase.search_count([('bucket', '=', 'importacion')])
        if mal_ubicadas:
            checks.append((
                WARN, 'Cuentas de aduana',
                'estas cuentas de aduana están en un bucket que las reparte '
                'por el driver equivocado (sobre TODAS las ventas, o fuera de '
                'costeo): %s. Muévelas al bucket «Gastos e impuestos de '
                'importación» para al menos poder medirlas.'
                % ', '.join(mal_ubicadas.mapped('name'))))
        elif not n_import:
            checks.append((
                WARN, 'Cuentas de aduana',
                'ninguna cuenta en el bucket «Gastos e impuestos de '
                'importación» — si importas, no hay forma de saber cuánta '
                'aduana se está quedando en resultados'))
        else:
            checks.append((OK, 'Cuentas de aduana',
                           '%s cuentas clasificadas' % n_import))

        if n_import:
            desde = fields.Date.today() - relativedelta(months=12)
            aduana = env['qb.costo.factores'].search(
                [], order='period DESC', limit=1).importacion_pool_month or 0.0
            capitalizado = sum(env['stock.landed.cost'].search([
                ('state', '=', 'done'), ('date', '>=', desde),
            ]).mapped('amount_total')) / 12.0
            if aduana and capitalizado < 0.25 * aduana:
                checks.append((
                    BAD, 'Aduana capitalizada (landed cost)',
                    '$%s/mes de aduana en resultados contra solo $%s/mes '
                    'capitalizado con landed costs. El pedimento sabe a qué '
                    'embarque pertenece: captúralo en la recepción y caerá en '
                    'los productos que lo causaron. Prorratearlo con una '
                    'fórmula le cobra al hilo el pedimento de una máquina.'
                    % (f'{aduana:,.0f}', f'{capitalizado:,.0f}')))
            else:
                checks.append((
                    OK, 'Aduana capitalizada (landed cost)',
                    '$%s/mes capitalizado con landed costs contra $%s/mes en '
                    'resultados' % (f'{capitalizado:,.0f}', f'{aduana:,.0f}')))
        return checks

    def _estado_mp_conciliable(self):
        env = self.env
        checks = []
        # Se rederiva aquí: antes venía de un check anterior por vivir
        # todo en el mismo método, y ese orden implícito era una
        # dependencia que nadie declaró.
        Clase = env['qb.costeo.cuenta.class']
        # 5.7 MP: ¿hay contra qué conciliar la receta?
        n_mp = Clase.search_count([('bucket', '=', 'mp')])
        if not n_mp:
            checks.append((
                WARN, 'Conciliación de materia prima',
                'ninguna cuenta en el bucket «Materia prima» — la MP de '
                'receta no se está comparando contra el costo primo del '
                'mayor, así que la merma y la variación de precio no entran '
                'al costo'))
        else:
            ajuste = env['qb.costo.factores'].search(
                [], order='period DESC', limit=1).mp_ajuste or 1.0
            checks.append((
                OK, 'Conciliación de materia prima',
                '%s cuentas de costo primo; ajuste vigente ×%.3f '
                '(receta → consumo real)' % (n_mp, ajuste)))
        return checks

    def _estado_capacidad_normal(self):
        env = self.env
        checks = []
        # 5.8 Capacidad normal: sin ella el producto carga la ociosidad
        denominadores = env['qb.costeo.centro'].search(
            ['|', ('es_denominador_kg', '=', True),
             ('es_denominador_m', '=', True)])
        caps = {o.centro_id.id: o.capacity_month_units
                for o in env['qb.ociosidad'].search(
                    [('centro_id', 'in', denominadores.ids)])}
        sin_normal = denominadores.filtered(lambda c: caps.get(c.id, 0.0) <= 0)
        if sin_normal:
            checks.append((
                WARN, 'Capacidad normal (IAS 2)',
                'estos centros definen el denominador y NO tienen capacidad '
                'normal derivable: %s. Su pool fijo se divide entre la '
                'producción real, así que un mes flojo encarece el producto. '
                'Captura su throughput nominal (o su capacidad normal) en '
                'Centros de costo.' % ', '.join(sin_normal.mapped('code'))))
        else:
            checks.append((
                OK, 'Capacidad normal (IAS 2)',
                'el pool fijo se divide entre capacidad normal; la ociosidad '
                'va al resultado del período, no al producto'))
        return checks

    def _estado_cuello_de_botella(self):
        env = self.env
        checks = []
        # 5.9 Cuello de botella: sin throughput el ranking apunta al centro
        # equivocado
        sin_throughput = env['qb.costeo.centro'].search([
            ('nature', '=', 'fabril_directo'),
            ('std_output_per_hour', '=', 0),
        ])
        if sin_throughput:
            checks.append((
                WARN, 'Throughput por centro',
                'sin throughput nominal: %s. La contribución por '
                'hora-máquina los ignora, así que el ranking mide el centro '
                'equivocado cuando el cuello real está en uno de ellos.'
                % ', '.join(sin_throughput.mapped('code'))))
        return checks

    def _estado_ventana_fabril_tras_corte(self):
        env = self.env
        checks = []
        # Se rederiva aquí: antes venía de un check anterior por vivir
        # todo en el mismo método, y ese orden implícito era una
        # dependencia que nadie declaró.
        absorbidos = env['qb.costeo.centro'].absorbidos_en(
            fields.Date.today())
        ultimo = env['qb.costo.factores'].search(
            [], order='period DESC', limit=1)
        # 5.9.5 Ventana fabril corta tras un corte de absorción
        if ultimo and ultimo.fab_ventana_meses and \
                ultimo.fab_ventana_meses < 3 and absorbidos:
            checks.append((
                WARN, 'Ventana del pool fabril',
                'solo %s mes(es) desde el corte de %s. El pool no se puede '
                'promediar con meses del régimen anterior —llevaban el gasto '
                'del centro completo— así que arranca corto y ruidoso. Se '
                'estabiliza solo; mientras tanto, lee los factores del mes '
                'con esa reserva.'
                % (ultimo.fab_ventana_meses,
                   ', '.join(absorbidos.mapped('code')))))
        return checks

    def _estado_ajuste_de_metros(self):
        env = self.env
        checks = []
        # 5.10 El ajuste de metros pierde su contrapeso si el estiramiento
        # se detiene: resta encogimiento y suma estiramiento, y se compensan.
        desde = fields.Date.today().replace(day=1) - relativedelta(months=6)
        sin_est = env['qb.costo.producto']._meses_sin_estiramiento(
            desde, fields.Date.today() + relativedelta(months=1))
        if sin_est:
            checks.append((
                WARN, 'Ajuste de metros (encogimiento/estiramiento)',
                'estos meses tienen encogimiento pero CERO estiramiento: %s. '
                'El ajuste resta metros sin su contrapeso, así que el costo '
                'por metro sale alto por una operación que dejó de hacerse, '
                'no porque la planta gaste más. Si el estiramiento se '
                'suspendió de verdad, revisa si el encogimiento debe seguir '
                'descontándose.'
                % ', '.join(str(m) for m in sin_est)))
        return checks

    def _estado_periodos_vs_maestro_de_pesos(self):
        env = self.env
        checks = []
        # 5.11 Períodos calculados ANTES del último cambio de pesos. El
        # caso WD3846NT163m2: se corrigió su peso y los meses recalculados
        # antes de la captura se quedaron con el criterio viejo — la misma
        # ventana de 12 meses mezclaba dos pesos y el producto "perdía"
        # $1M que no perdía. Un período abierto más viejo que el maestro
        # de pesos es un reporte mintiendo en silencio.
        ultimo_peso = env['qb.producto.peso'].search(
            [], order='write_date desc', limit=1)
        if ultimo_peso:
            desfasados = env['qb.costo.factores'].search([
                ('state', '=', 'borrador'),
                ('write_date', '<', ultimo_peso.write_date)])
            if desfasados:
                periodos = ', '.join(
                    str(p) for p in sorted(desfasados.mapped('period'))[:8])
                if len(desfasados) > 8:
                    periodos += '…'
                checks.append((
                    WARN, 'Períodos vs maestro de pesos',
                    '%s período(s) abiertos calculados ANTES del último '
                    'cambio de pesos (%s): su costo usa el peso viejo y la '
                    'ventana de 12 meses mezcla criterios. Corre '
                    '«Recalcular costeo (rango de meses)» sobre: %s'
                    % (len(desfasados),
                       ultimo_peso.write_date.strftime('%Y-%m-%d %H:%M'),
                       periodos)))
            else:
                checks.append((
                    OK, 'Períodos vs maestro de pesos',
                    'todos los períodos abiertos son posteriores al último '
                    'cambio de pesos'))
        return checks

    def _estado_cola_de_recalculo(self):
        env = self.env
        checks = []
        # 5.12 Cola de recálculo atorada: el cron diferido murió una vez
        # dejando 6 meses de 2024 pendientes por 2 días — nadie lo vio
        # porque el cron simplemente dejó de correr.
        Config = env['qb.costeo.factor.config']
        cola = Config.search([('key', '=', 'recalculo_pendiente')], limit=1)
        pendientes_cola = [p for p in (cola.value_text or '').split(',') if p]
        if pendientes_cola:
            cron = env.ref('qb_capacidad_costeo.cron_recalculo_pendientes',
                           raise_if_not_found=False)
            if not cron or not cron.active:
                checks.append((
                    BAD, 'Cola de recálculo diferido',
                    '%s período(s) esperando en la cola y el cron está '
                    'APAGADO — la cola quedó atorada. Actívalo en Ajustes '
                    '→ Técnico → Acciones planificadas («Recálculo '
                    'diferido de históricos») o corre el recálculo por '
                    'rango.' % len(pendientes_cola)))
            else:
                checks.append((
                    OK, 'Cola de recálculo diferido',
                    '%s período(s) en cola, cron activo — convergiendo'
                    % len(pendientes_cola)))
        return checks

    def _estado_avco_de_importados(self):
        env = self.env
        checks = []
        # Se rederiva aquí: antes venía de un check anterior por vivir
        # todo en el mismo método, y ese orden implícito era una
        # dependencia que nadie declaró.
        ultimo = env['qb.costo.factores'].search(
            [], order='period DESC', limit=1)
        # 5.13 AVCO de importados vs su compra real. El costo del ' I' es su
        # AVCO (compra del IT + gastos de la OP de conversión) y nadie lo
        # validaba contra la fuente: el KP2032T11GO152 I traía 9.39 —
        # calcado del gemelo nacional — cuando su IT real (KP4032T11GO152
        # IT) se compró a ~6.10. +54% de MP fantasma en $511K de venta.
        if ultimo:
            Costo = env['qb.costo.producto']
            imp_rows = Costo.search([
                ('period', '=', ultimo.period),
                ('product_bucket', '=', 'importado'),
                ('qty_vendida', '>', 0)])
            divergentes = []
            for row in imp_rows:
                it = Costo._it_twin(row.product_id)
                if not it:
                    continue
                compra = Costo._last_purchase_cost(it)
                if compra <= 0 or row.mp_unit <= 0:
                    continue
                delta = (row.mp_unit - compra) / compra
                if abs(delta) > 0.35:
                    divergentes.append((abs(delta), delta, row, compra))
            if divergentes:
                divergentes.sort(key=lambda d: d[0], reverse=True)
                det = '; '.join(
                    '%s: modelo $%.2f vs compra IT $%.2f (%+.0f%%)'
                    % (row.default_code, row.mp_unit, compra, delta * 100)
                    for _absd, delta, row, compra in divergentes[:6])
                if len(divergentes) > 6:
                    det += '…'
                checks.append((
                    WARN, 'AVCO de importados vs compra IT',
                    '%s importado(s) vendidos con MP a más de ±35%% de la '
                    'última compra de su gemelo IT: %s. Puede ser AVCO '
                    'seteado a mano, capas viejas de inventario o un precio '
                    'nuevo — revisa el costo del producto en Odoo.'
                    % (len(divergentes), det)))
            elif imp_rows:
                checks.append((
                    OK, 'AVCO de importados vs compra IT',
                    'los %s importados vendidos del período están a ±35%% '
                    'de su última compra IT' % len(imp_rows)))
        return checks

    def _estado_bom_vs_consumo_real(self):
        env = self.env
        checks = []
        # 5.14 Consumo de BOM vs consumo real de las OPs. El caso X140
        # (ago-2026): la BOM de acabado decía 0.2674 kg de tejido por
        # metro y las OPs done consumían 0.2474 — 8% de hilo fantasma
        # que le cargaba $1/m de MP a un producto que "perdía" $72K/mes
        # sin perderlos (y +13% en los SCRIM). La receta duplica un dato
        # vivo (lo que las OPs consumieron) y nadie los comparaba.
        uom_m = env.ref('uom.product_uom_meter', raise_if_not_found=False)
        uom_kg = env.ref('uom.product_uom_kgm', raise_if_not_found=False)
        if uom_m and uom_kg:
            hace_12m = fields.Date.today() - relativedelta(months=12)
            env.flush_all()   # el SQL crudo no ve el buffer del ORM
            env.cr.execute("""
                WITH cons AS (
                    SELECT sm.raw_material_production_id AS mo_id,
                           sm.product_id AS comp_id,
                           SUM(sm.quantity) AS consumed
                      FROM stock_move sm
                     WHERE sm.state = 'done'
                       AND sm.raw_material_production_id IS NOT NULL
                       AND sm.date >= %s
                     GROUP BY 1, 2
                ),
                reales AS (
                    SELECT mp.product_id AS out_id, c.comp_id,
                           SUM(c.consumed) AS consumed,
                           SUM(mp.product_qty) AS produced
                      FROM cons c
                      JOIN mrp_production mp ON mp.id = c.mo_id
                     WHERE mp.state = 'done'
                       AND mp.product_uom_id = %s
                       AND mp.company_id = %s
                     GROUP BY 1, 2
                    HAVING SUM(mp.product_qty) >= %s
                ),
                lineas AS (
                    SELECT pp.id AS out_id, bl.product_id AS comp_id,
                           MIN(bl.product_qty) AS q_min,
                           MAX(bl.product_qty) AS q_max
                      FROM mrp_bom_line bl
                      JOIN mrp_bom b ON b.id = bl.bom_id
                      JOIN product_product pp
                        ON pp.product_tmpl_id = b.product_tmpl_id
                     WHERE b.active
                       AND bl.product_uom_id = %s
                       AND bl.product_qty >= 0.02
                     GROUP BY 1, 2
                )
                SELECT r.out_id, r.comp_id, r.consumed, r.produced,
                       l.q_min, l.q_max
                  FROM reales r
                  JOIN lineas l ON l.out_id = r.out_id
                              AND l.comp_id = r.comp_id
            """, (hace_12m, uom_m.id, env.company.id, 50000.0, uom_kg.id))
            desviadas = []
            for out_id, comp_id, _cons, produced, q_min, q_max in \
                    env.cr.fetchall():
                real = _cons / produced if produced else 0.0
                if real <= 0:
                    continue
                # Con recetas alternativas basta que UNA esté calibrada:
                # manda la BOM más cercana al consumo real.
                cerca = min((q_min, q_max), key=lambda q: abs(q - real))
                delta = (cerca - real) / real
                if abs(delta) > 0.05:
                    desviadas.append(
                        (abs(delta), delta, out_id, comp_id, cerca, real))
            if desviadas:
                desviadas.sort(reverse=True)
                Prod = env['product.product']
                det = '; '.join(
                    '%s ← %s: BOM %.4f vs real %.4f (%+.0f%%)'
                    % (Prod.browse(o).default_code or Prod.browse(o).name,
                       Prod.browse(c).default_code or Prod.browse(c).name,
                       q, real, delta * 100)
                    for _a, delta, o, c, q, real in desviadas[:6])
                if len(desviadas) > 6:
                    det += '…'
                checks.append((
                    WARN, 'Consumo de BOM vs OPs reales',
                    '%s receta(s) kg→m con consumo a más de ±5%% de lo '
                    'que las OPs terminadas consumieron en 12 meses: %s. '
                    'La MP del costeo sale de la receta — valida el factor '
                    'con producción, corrige la BOM y recalcula.'
                    % (len(desviadas), det)))
            else:
                checks.append((
                    OK, 'Consumo de BOM vs OPs reales',
                    'las recetas kg→m con volumen (≥50,000 m/12m) están a '
                    '±5% del consumo real de las OPs'))
        return checks

    def _estado_produccion_arriba_de_capacidad(self):
        env = self.env
        checks = []
        # Se rederiva aquí: antes venía de un check anterior por vivir
        # todo en el mismo método, y ese orden implícito era una
        # dependencia que nadie declaró.
        Config = env['qb.costeo.factor.config']
        # 5.15 Producción arriba de la capacidad normal: el caso Acabado
        # (952K m reales vs 915,733 capturados, una rama nueva sin
        # reflejar). El modelo topaba la utilización en 100 y ponía el
        # ocioso en cero — escondía el error en vez de señalarlo. Ahora el
        # período usa la producción real como denominador (IAS 2) y este
        # check exige actualizar la capacidad del centro.
        #
        # Mira una VENTANA, no el último período: con capacidad de Acabado
        # en 915,733, enero–mayo de 2026 la superaron y junio–agosto no, así
        # que el check se pintaba verde mirando agosto mientras cinco meses
        # del mismo año seguían rojos. Un mes flojo no arregla la capacidad
        # mal capturada; solo la esconde.
        meses_cap = int(Config.get_param('capacidad_superada_meses', 12.0))
        recientes = env['qb.costo.factores'].search(
            [('company_id', '=', env.company.id)],
            order='period DESC', limit=max(meses_cap, 1))
        superados = recientes.filtered(
            lambda f: f.capacidad_superada_m or f.capacidad_superada_kg)
        if superados:
            en_m = superados.filtered('capacidad_superada_m')
            en_kg = superados.filtered('capacidad_superada_kg')
            lados = []
            if en_m:
                lados.append('METROS (hasta %s m/mes producidos — la '
                             'capacidad capturada de los centros '
                             'es_denominador_m quedó abajo)'
                             % '{:,.0f}'.format(
                                 max(en_m.mapped('m_produccion_month'))))
            if en_kg:
                lados.append('KG (hasta %s kg/mes producidos)'
                             % '{:,.0f}'.format(
                                 max(en_kg.mapped('kg_produccion_month'))))
            periodos = ', '.join(
                str(p) for p in sorted(superados.mapped('period'))[:6])
            if len(superados) > 6:
                periodos += '…'
            checks.append((
                BAD, 'Capacidad normal vs producción real',
                '%s de los últimos %s períodos SUPERAN la capacidad normal '
                'capturada en %s: hay máquinas/ramas sin reflejar (%s). El '
                'costeo ya usa la producción real como denominador (IAS 2) '
                'para no sobre-absorber, pero la ociosidad de ese lado '
                'sale en cero por construcción — actualiza la capacidad '
                'del centro en Configuración → Centros de costo.'
                % (len(superados), len(recientes), ' y '.join(lados),
                   periodos)))
        elif recientes:
            checks.append((
                OK, 'Capacidad normal vs producción real',
                'la producción de los últimos %s períodos cabe dentro de la '
                'capacidad normal capturada en ambos lados (kg y m)'
                % len(recientes)))
        return checks

    def _estado_capacidad_capturada_vs_horario(self):
        env = self.env
        checks = []
        # Se rederiva aquí: antes venía de un check anterior por vivir
        # todo en el mismo método, y ese orden implícito era una
        # dependencia que nadie declaró.
        Config = env['qb.costeo.factor.config']
        # 5.16 La capacidad capturada duplica un dato vivo — horario de
        # planta × velocidad de máquina — y hasta hoy nadie los comparaba:
        # `capacidad_normal`, cuando está capturada, GANA sobre el cálculo
        # de turnos y lo deja mudo. Así fue como Acabado se quedó en
        # 915,733 m/mes mientras sus dos ramas daban 1.18M: el número vivió
        # dos años sin que nada lo contradijera. Misma regla que ya cubre
        # pesos (5.11), AVCO de importados (5.13) y consumo de BOM (5.14).
        tol_cap = Config.get_param('capacidad_capturada_tol_pct', 10.0)
        Turno = env['qb.turno.config']
        divergentes, sin_base = [], []
        for centro in env['qb.costeo.centro'].search(
                [('nature', '!=', 'admin'), ('capacidad_normal', '>', 0),
                 ('company_id', '=', env.company.id)]):
            turnos = Turno.search([('centro_id', '=', centro.id),
                                   ('active', '=', True)])
            horas = sum(t.hours_per_month() for t in turnos)
            derivada = horas * (centro.std_output_per_hour or 0.0)
            if not derivada:
                sin_base.append(centro.code)
                continue
            delta = (centro.capacidad_normal - derivada) / derivada
            if abs(delta) * 100.0 > tol_cap:
                divergentes.append((abs(delta), centro.code,
                                    centro.capacidad_normal, derivada, delta))
        if divergentes:
            divergentes.sort(reverse=True)
            det = '; '.join(
                '%s: capturada %s vs horario×velocidad %s (%+.0f%%)'
                % (code, '{:,.0f}'.format(cap), '{:,.0f}'.format(der),
                   delta * 100)
                for _a, code, cap, der, delta in divergentes)
            checks.append((
                WARN, 'Capacidad capturada vs horario × velocidad',
                '%s centro(s) con la capacidad capturada a más de ±%.0f%% de '
                'lo que dan sus turnos por su throughput nominal: %s. Uno de '
                'los dos está viejo — el horario, la velocidad o el número '
                'capturado.' % (len(divergentes), tol_cap, det)))
        elif sin_base:
            checks.append((
                WARN, 'Capacidad capturada vs horario × velocidad',
                'sin turnos o sin throughput nominal para contrastar: %s. '
                'Captúralos en "Turnos / capacidad manual" y en el centro; '
                'sin ellos la capacidad capturada no tiene contra qué '
                'validarse.' % ', '.join(sorted(sin_base))))
        else:
            checks.append((
                OK, 'Capacidad capturada vs horario × velocidad',
                'la capacidad capturada de cada centro cuadra a ±%.0f%% con '
                'sus turnos por su throughput nominal' % tol_cap))
        return checks

    def _estado_familias_de_maquinas(self):
        env = self.env
        checks = []
        # 5.17 La capacidad de un centro NO es fungible. Tejido tiene 27
        # circulares y 197,529 kg/mes, pero el WJ044 de 235 cm solo sale en
        # las diez galga 18 Ø32 — y esas van al 79% mientras la planta va al
        # 44%. Dos cosas que se pueden validar solas: que la suma de las
        # familias cuadre con su centro, y que la producción del centro esté
        # catalogada en alguna familia (lo que no está, no se puede rutear).
        Familia = env['qb.costeo.familia']
        familias = Familia.search([('company_id', '=', env.company.id)])
        if familias:
            descuadre, saturadas = [], []
            for centro in familias.mapped('centro_id'):
                suma = sum(familias.filtered(
                    lambda f: f.centro_id == centro).mapped('capacidad_normal'))
                if not (centro.capacidad_normal and suma):
                    continue
                delta = (suma - centro.capacidad_normal) / centro.capacidad_normal
                if abs(delta) > 0.10:
                    descuadre.append((centro.code, suma,
                                      centro.capacidad_normal, delta))
            for fila in env['qb.familia.carga'].search([]):
                if fila.utilization_pct >= 75.0:
                    saturadas.append((fila.utilization_pct,
                                      fila.familia_id.code))
            if descuadre:
                det = '; '.join(
                    '%s: familias %s vs centro %s (%+.0f%%)'
                    % (code, '{:,.0f}'.format(suma),
                       '{:,.0f}'.format(centro_cap), delta * 100)
                    for code, suma, centro_cap, delta in descuadre)
                checks.append((
                    WARN, 'Familias de máquinas vs capacidad del centro',
                    'la capacidad de las familias no suma la del centro: %s. '
                    'Una de las dos está vieja — o falta dar de alta una '
                    'familia.' % det))
            elif saturadas:
                saturadas.sort(reverse=True)
                checks.append((
                    WARN, 'Familias de máquinas',
                    'la capacidad del centro cuadra con sus familias, pero '
                    '%s de ellas van arriba del 75%%: %s. El agregado del '
                    'centro NO las ve — para decidir si cabe un pedido, mira '
                    'la familia que lo puede hacer.'
                    % (len(saturadas),
                       ', '.join('%s (%.0f%%)' % (c, u) for u, c in saturadas[:5]))))
            else:
                checks.append((
                    OK, 'Familias de máquinas',
                    '%s familias dadas de alta, su capacidad cuadra con la de '
                    'sus centros y ninguna pasa del 75%% de carga'
                    % len(familias)))
        else:
            checks.append((
                WARN, 'Familias de máquinas',
                'ninguna familia dada de alta: la capacidad de cada centro se '
                'lee como si cualquier máquina hiciera cualquier producto, y '
                'un centro medio vacío puede tener su familia clave '
                'saturada.'))
        return checks

    def _estado_capacidad_instalada(self):
        """5.18 El techo FÍSICO: se puede leer, o no se puede.

        `machines_installed` sale contando `machine_names`, así que no es
        un segundo número capturado que se pueda quedar viejo — pero
        `machine_names` sí duplica un dato vivo: las máquinas que Odoo
        conoce como `mrp.workcenter`. Se contrasta contra esa fuente
        donde existe (tejido); en acabado y tintorería las ramas y los
        jets no están dados de alta como workcenter y no hay contra qué
        contrastar, y eso también se dice en vez de callarlo.

        Los dos huecos que dejan el techo subestimado —sin lista de
        máquinas y sin velocidad— salen como aviso: una familia que se
        lee en cero instalada no es una familia sin capacidad, es una
        que no se puede leer, y las dos cosas se ven igual en la tabla
        si nadie lo dice.
        """
        env = self.env
        checks = []
        Familia = env['qb.costeo.familia']
        familias = Familia.with_context(active_test=False).search(
            [('company_id', '=', env.company.id)])
        if not familias:
            return checks

        sin_lista = familias.filtered(lambda f: not f._maquinas_listadas())
        if sin_lista:
            checks.append((
                WARN, 'Máquinas instaladas por familia',
                '%s familia(s) sin lista de máquinas (%s): sus instaladas '
                'se cuentan como las dotadas, así que su capacidad '
                'instalada queda igual a la normal y el techo del centro '
                'sale subestimado.'
                % (len(sin_lista), ', '.join(sorted(sin_lista.mapped('code'))))))

        # Contra la fuente viva: los `mrp.workcenter` que Odoo conoce.
        desconocidas, sin_workcenter = [], []
        for fam in familias:
            listadas = fam._maquinas_listadas()
            if not listadas:
                continue
            encontradas = len(fam.workcenters())
            if not encontradas:
                sin_workcenter.append(fam.code)
            elif encontradas != listadas:
                desconocidas.append(
                    '%s: %s listadas, %s en Odoo'
                    % (fam.code, listadas, encontradas))
        if desconocidas:
            checks.append((
                BAD, 'Máquinas de la familia vs Odoo',
                'la lista de máquinas no cuadra con los centros de trabajo '
                'de Odoo: %s. Una de las dos está vieja, y de la lista sale '
                'la capacidad instalada.' % '; '.join(desconocidas)))
        elif not sin_lista:
            checks.append((
                OK, 'Máquinas de la familia vs Odoo',
                'las %s familias con lista cuadran contra los centros de '
                'trabajo que Odoo conoce%s.'
                % (len(familias) - len(sin_lista),
                   '; %s no tienen workcenter con qué contrastar (%s)'
                   % (len(sin_workcenter), ', '.join(sorted(sin_workcenter)))
                   if sin_workcenter else '')))

        sin_velocidad = familias.filtered(
            lambda f: not f.std_output_per_hour and f.capacidad_normal <= 0)
        if sin_velocidad:
            checks.append((
                WARN, 'Capacidad instalada sin velocidad',
                '%s familia(s) sin velocidad capturada (%s): existen, están '
                'pagadas y su capacidad instalada se lee en cero. Capturar '
                'la velocidad no las activa —siguen sin dotar y sin '
                'absorber— pero le pone número a la decisión de arrancarlas.'
                % (len(sin_velocidad),
                   ', '.join(sorted(sin_velocidad.mapped('code'))))))

        parada = env['qb.familia.carga'].search(
            [('capacity_parked_units', '>', 0)])
        if parada:
            trozos = []
            for centro in parada.mapped('centro_id').sorted(
                    lambda c: (c.sequence, c.code or '')):
                total = sum(parada.filtered(
                    lambda r: r.centro_id == centro
                ).mapped('capacity_parked_units'))
                trozos.append(
                    '%s %s/mes en %s'
                    % ('{:,.0f}'.format(total), self._unidad(centro),
                       centro.code or centro.name))
            det = '; '.join(trozos)
            checks.append((
                OK, 'Capacidad instalada sin dotar',
                '%s. No absorbe costo —una máquina parada no absorbe— pero '
                'se libera con gente, no con inversión.' % det))
        return checks

    def _estado_peso_de_la_ficha(self):
        env = self.env
        checks = []
        # Se rederiva aquí: antes venía de un check anterior por vivir
        # todo en el mismo método, y ese orden implícito era una
        # dependencia que nadie declaró.
        Config = env['qb.costeo.factor.config']
        # 5.18 El peso de la FICHA es una copia del maestro, y esa ficha es
        # la hoja técnica que se le manda al cliente. Al medir un peso, la
        # copia envejecía sin que nada avisara: el WJ032Q22JNT160 quedó en
        # 0.0512 kg/m (gramaje × ancho) contra 0.059114 de báscula, 13%
        # abajo. Ahora el maestro refresca las fichas al escribirse; este
        # check cubre lo que ese refresco no toca — las fichas manuales y
        # cualquier vía que no pase por el maestro.
        tol_ficha = Config.get_param('ficha_peso_tol_pct', 2.0)
        desfasadas = env['qb.producto.ficha'].fichas_con_peso_desfasado(
            tol_pct=tol_ficha)
        if desfasadas:
            det = '; '.join(
                '%s: ficha %.4f vs maestro %.4f (%+.0f%%)'
                % (f.default_code or f.product_id.display_name,
                   guardado, maestro, desv * 100)
                for f, guardado, maestro, desv in desfasadas[:6])
            if len(desfasadas) > 6:
                det += '…'
            checks.append((
                WARN, 'Peso de la ficha vs maestro de pesos',
                '%s ficha(s) con el peso a más de ±%.0f%% del maestro: %s. '
                'Esa hoja va al cliente — si el maestro tiene razón, '
                'refréscalas; si la ficha tiene razón, corrige el maestro.'
                % (len(desfasadas), tol_ficha, det)))
        else:
            checks.append((
                OK, 'Peso de la ficha vs maestro de pesos',
                'el peso de cada ficha cuadra a ±%.0f%% con el maestro'
                % tol_ficha))
        return checks

    def _estado_factores_calculados(self):
        env = self.env
        checks = []
        # 6. Factores calculados
        factores = env['qb.costo.factores'].search([], order='period DESC', limit=1)
        if not factores:
            checks.append((BAD, 'Factores de costeo',
                           'nunca calculados — corre "Recalcular costeo"'))
        else:
            detail = ('período %s: fab $%.2f/kg + $%.2f/m, energía $%.2f/kg, '
                      'op %.1f%%, cobertura %.0f%%'
                      % (factores.period, factores.factor_fab_kg,
                         factores.factor_fab_m, factores.energia_por_kg,
                         factores.op_pct * 100, factores.cobertura_fab_pct))
            icon = OK if factores.factor_fab_kg and factores.op_pct else WARN
            checks.append((icon, 'Factores de costeo', detail))
        return checks

    def _estado_costos_por_producto(self):
        env = self.env
        checks = []
        # 7. Costos por producto
        # El período sale del último `factores`, el mismo que mira el check
        # 6: se vuelve a buscar aquí en vez de heredar la variable para que
        # cada check corra solo y el orden del registro no sea una
        # dependencia escondida.
        factores = env['qb.costo.factores'].search(
            [], order='period DESC', limit=1)
        n_costos = env['qb.costo.producto'].search_count(
            [('period', '=', factores.period)]) if factores else 0
        checks.append((OK if n_costos else WARN, 'Costo por producto',
                       '%s productos costeados en el último período' % n_costos))
        return checks

    # ------------------------------------------------------------------
    # Botones de acción directa
    # ------------------------------------------------------------------
    def _open(self, xmlid):
        action = self.env['ir.actions.act_window']._for_xml_id(
            'qb_capacidad_costeo.%s' % xmlid)
        return action

    def action_cargar_pesos(self):
        """Carga el maestro de pesos nativo (sin Supabase) y abre la lista."""
        self.env['qb.producto.peso'].load_weight_master()
        return self._open('producto_peso_action')

    def action_recalcular(self):
        self.env['qb.costo.producto'].action_recompute_period()
        return self._open('costo_factores_action')

    def action_cuentas_pendientes(self):
        return self.env['qb.costeo.cuenta.class'].action_unclassified_accounts()

    def action_turnos(self):
        return self._open('turno_config_action')

    def action_balance(self):
        return self._open('balance_action')

    def action_cotizar(self):
        return self._open('cotizador_wizard_action')

    def action_ranking(self):
        return self._open('costo_ranking_action')

    def action_rentabilidad_clientes(self):
        return self._open('cliente_rentabilidad_action')

    def action_rentabilidad_productos(self):
        return self._open('producto_rentabilidad_action')

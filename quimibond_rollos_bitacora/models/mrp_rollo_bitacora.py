# -*- coding: utf-8 -*-
from odoo import api, fields, models, _
from odoo.exceptions import UserError

# 3.15.0: calidades que salen como la tela en metros; el resto (Desperdicio) es saldo en kg.
CALIDADES_EN_METROS = ('PQ', 'FE', 'SEGUNDA')
CLASIFICACIONES = [('PQ', 'PQ'), ('FE', 'FE'), ('SEGUNDA', 'Segunda'), ('DESP', 'Desperdicio')]

# DAT P-D02-01 rev. 05, posiciones 10-11 del código (sin el prefijo «I»).
COLORES = {
    'AC': 'AZUL CIELO', 'AM': 'AMARILLO', 'AZ': 'AZUL MARINO', 'BL': 'BLANCO', 'CF': 'CAFÉ',
    'GO': 'GRIS OXFORD', 'HU': 'HUESO', 'NA': 'NARANJA', 'NG': 'NEGRO', 'NT': 'NATURAL', 'RO': 'ROJO',
    'CJ': 'CAFÉ JASPE', 'AJ': 'AZUL JASPE', 'GJ': 'GRIS JASPE', 'GR': 'GRIS RATA', 'GC': 'GRIS CLARO',
    'MO': 'MORADO', 'MZ': 'MOSTAZA', 'AN': 'AMARILLO NEÓN', 'NN': 'NATURAL NEGRO',
}
# Signo al final del código del producto según la clasificación (etiqueta de producto con defectiva).
SUFIJO_CODIGO = {'PQ': '', 'FE': '..', 'SEGUNDA': '.', 'DESP': '*'}
YARDAS_POR_METRO = 1.0936133
LIBRAS_POR_KG = 2.2046226

PARAM_ALERTA_METROS_INSP = 'quimibond_rollos_bitacora.alerta_metros_insp'
PARAM_ALERTA_DIFERENCIA_PCT = 'quimibond_rollos_bitacora.alerta_diferencia_pct'
# Etiqueta final (3.3.7, Sistemas 2026-11-05): 76 mm de ancho x 102 mm de alto = 608 x 816 puntos a 203 dpi.
PARAM_FINAL_ANCHO = 'quimibond_rollos_bitacora.etiqueta_final_ancho'
PARAM_FINAL_ALTO = 'quimibond_rollos_bitacora.etiqueta_final_alto'
FINAL_ANCHO, FINAL_ALTO = 608, 816
# Etiquetas preliminares de Rama e Inspección (3.4.0, Sistemas 2026-11-05): 68 mm de ancho x 38 mm de alto
# = 544 x 304 puntos a 203 dpi. Inspección imprime al confirmar la preliminar o la final (parámetro).
PARAM_RAMA_ANCHO = 'quimibond_rollos_bitacora.etiqueta_rama_ancho'
PARAM_RAMA_ALTO = 'quimibond_rollos_bitacora.etiqueta_rama_alto'
PARAM_PRELIM_ANCHO = 'quimibond_rollos_bitacora.etiqueta_prelim_ancho'
PARAM_PRELIM_ALTO = 'quimibond_rollos_bitacora.etiqueta_prelim_alto'
PRELIM_ANCHO, PRELIM_ALTO = 544, 304
PARAM_INSPECCION_IMPRIME = 'quimibond_rollos_bitacora.inspeccion_imprime'
CALIDAD_CORTA = {'PQ': 'PQ', 'FE': 'FE', 'SEGUNDA': '2DA', 'DESP': 'DESP'}
EMPRESA_ETIQUETA = 'PRODUCTORA DE NO TEJIDOS QUIMIBOND S.A. DE C.V.'
LEYENDA_PNC = {'FE': 'FUERA DE ESPECIFICACIÓN', 'SEGUNDA': 'SEGUNDA', 'DESP': 'DESPERDICIO'}


class MrpRolloBitacora(models.Model):
    """Un registro por rollo de la MO en kg (diseño §9.1). Nace en Rama y lo siguen llenando
    Inspección y Clasificación final."""
    _name = 'mrp.rollo.bitacora'
    _description = 'Bitácora de rollos'
    _order = 'production_id desc, secuencia'
    _rec_name = 'name'

    name = fields.Char('Rollo', compute='_compute_name', store=True)
    production_id = fields.Many2one(
        'mrp.production', 'Orden (kg)', required=True, index=True, ondelete='cascade', readonly=True)
    product_id = fields.Many2one(related='production_id.product_id', string='Artículo', store=True)
    secuencia = fields.Integer('No. de rollo', required=True, readonly=True)
    lote_origen_id = fields.Many2one('stock.lot', 'Lote de tela cruda (Alm. 5)')
    company_id = fields.Many2one(related='production_id.company_id', store=True)

    # --- Rama: proceso y pesaje (ÚNICO pesaje de la orden en kg) ---
    rama_medicion_ids = fields.One2many('mrp.rollo.medicion.rama', 'bitacora_id', 'Mediciones de Rama')
    lot_id = fields.Many2one('stock.lot', 'Lote de A (kg)', readonly=True, copy=False, index=True)
    workcenter_rama_id = fields.Many2one('mrp.workcenter', 'Rama', readonly=True)
    kg_bruto_rama = fields.Float('Peso bruto Rama (kg)', digits=(12, 4), readonly=True)
    tara_rama = fields.Float('Tara Rama (kg)', digits=(12, 4), readonly=True)
    kg_rama = fields.Float('Kg netos (Rama)', digits=(12, 4), readonly=True,
                           help='Cantidad del lote de A. Es el kg que usan Inspección y Clasificación final.')
    metros_rama = fields.Float(
        'Metros de Rama (cuentametros)', digits=(12, 2), readonly=True,
        help='3.6.0: lo que marca el cuentametros al final de la Rama cuando sale el rollo. Se captura junto con el '
             'peso; si no se pudo leer, queda el motivo.')
    motivo_sin_metros_rama = fields.Char('Motivo sin metros (Rama)', readonly=True)
    rendimiento_rama = fields.Float(
        'Rendimiento en Rama (m/kg)', digits=(12, 4), compute='_compute_rendimiento_rama', store=True,
        help='3.6.0: metros del cuentametros entre los kg netos de Rama del rollo.')
    captura_manual_rama = fields.Boolean('Captura manual (Rama)', readonly=True)
    motivo_manual_rama = fields.Char('Motivo de captura manual (Rama)', readonly=True)
    operador_rama = fields.Char('Operador de Rama', readonly=True)
    fecha_registro_rama = fields.Datetime('Registro en Rama', readonly=True)
    fecha_etiqueta_rama = fields.Datetime('Etiqueta de Rama impresa', readonly=True)
    zpl_etiqueta_rama = fields.Text('ZPL etiqueta de Rama', readonly=True, copy=False)

    # --- Inspección (MO de A): sin pesaje y sin control de calidad ---
    estado_inspeccion = fields.Selection(
        [('pendiente', 'Pendiente'), ('inspeccionado', 'Inspeccionado')],
        'Inspección', default='pendiente', required=True)
    clasificacion_inicial = fields.Selection(CLASIFICACIONES, 'Clasificación inicial')
    no_empalmes = fields.Integer('No. de empalmes')
    metros_insp = fields.Float('Metros (Inspección, referencia)', digits=(12, 2))
    fecha_etiqueta_preliminar = fields.Datetime('Etiqueta preliminar impresa')
    defecto_ids = fields.Many2many(
        'calidad.defecto.tela', 'mrp_rollo_bitacora_defecto_rel', 'bitacora_id', 'defecto_id', 'Defectos')
    siglas_defectos = fields.Char('Siglas de defectos', compute='_compute_siglas_defectos')
    workcenter_insp_id = fields.Many2one('mrp.workcenter', 'Máquina de Inspección (Revisadora)', readonly=True)
    operador_insp = fields.Char('Operador de Inspección', readonly=True)
    fecha_registro_insp = fields.Datetime('Registro en Inspección', readonly=True)
    zpl_etiqueta_preliminar = fields.Text('ZPL etiqueta preliminar', readonly=True, copy=False)
    # resultado_qc_laboratorio_ids se agrega con el QC de laboratorio (Clasificación final).

    # --- Clasificación final (MO de B, centro Empaque) ---
    lot_b_id = fields.Many2one('stock.lot', 'Lote de B (m)', copy=False)
    qc_laboratorio_aplica = fields.Boolean(
        'Aplica QC de laboratorio', compute='_compute_qc_laboratorio_aplica', store=True,
        help='Solo los rollos PQ llevan control de calidad de laboratorio.')
    clasificacion_final = fields.Selection(CLASIFICACIONES, 'Clasificación definitiva')
    kg_bruto_final = fields.Float('Peso bruto final (kg)', digits=(12, 4))
    tara_final = fields.Float('Tara Clasificación final (kg)', digits=(12, 4))
    kg_final = fields.Float('Kg finales (referencia)', digits=(12, 4))
    captura_manual_final = fields.Boolean('Captura manual (Clasificación final)')
    motivo_manual_final = fields.Char('Motivo de captura manual (Clasificación final)')
    metros_final = fields.Float('Metros finales', digits=(12, 2),
                                help='Cantidad del lote de B: los de Inspección confirmados o remedidos.')
    remedido = fields.Boolean('Remedido')
    motivo_ajuste_metros = fields.Char('Motivo del ajuste de metros')
    diferencia_metros_insp = fields.Float(
        'Metros finales − Inspección', compute='_compute_alertas', store=True, digits=(12, 2))
    alerta_metros_insp = fields.Boolean('Alerta 1: metros vs Inspección', compute='_compute_alertas', store=True)
    rendimiento_real = fields.Float('Rendimiento real (m/kg)', compute='_compute_alertas', store=True, digits=(12, 4))
    rendimiento_teorico = fields.Float('Rendimiento de acabado (m/kg)', compute='_compute_alertas', store=True,
                                       digits=(12, 4))
    diferencia_metros_vs_teorico = fields.Float(
        'Metros finales − teóricos', compute='_compute_alertas', store=True, digits=(12, 2))
    alerta_diferencia_metros = fields.Boolean(
        'Alerta 2: metros vs teórico', compute='_compute_alertas', store=True)
    diferencia_kg = fields.Float('Kg Rama − kg finales', compute='_compute_alertas', store=True, digits=(12, 4))
    reetiquetado = fields.Boolean('Reetiquetado', compute='_compute_alertas', store=True)
    fecha_etiqueta_final = fields.Datetime('Etiqueta final impresa')
    production_b_id = fields.Many2one('mrp.production', 'Orden (m)', readonly=True, index=True, copy=False)
    workcenter_final_id = fields.Many2one('mrp.workcenter', 'Centro de Clasificación final', readonly=True)
    operador_final = fields.Char('Operador de Clasificación final', readonly=True)
    siglas_operador_final = fields.Char('Siglas del operador (etiqueta)', readonly=True)
    fecha_registro_final = fields.Datetime('Registro en Clasificación final', readonly=True)
    zpl_etiqueta_final = fields.Text('ZPL etiqueta final', readonly=True, copy=False)

    _unique_rollo = models.Constraint(
        'unique(production_id, secuencia)', 'Ese número de rollo ya existe en la orden.')
    _check_kg_rama = models.Constraint(
        'CHECK(kg_rama > 0)', 'Los kg netos de Rama deben ser mayores a cero.')

    @api.depends('lot_id.name', 'production_id.name', 'secuencia')
    def _compute_name(self):
        for rollo in self:
            rollo.name = rollo.lot_id.name or '%s-%04d' % (rollo.production_id.name or '', rollo.secuencia or 0)

    @api.depends('clasificacion_inicial')
    def _compute_qc_laboratorio_aplica(self):
        for rollo in self:
            rollo.qc_laboratorio_aplica = rollo.clasificacion_inicial == 'PQ'

    def _rendimiento_acabado(self):
        """Rendimiento de la ficha de acabado que liga el «I…» de la orden (product_proceso_id), o 0."""
        self.ensure_one()
        Ficha = self.env.get('ficha.tecnica.acabado')
        if Ficha is None or 'product_proceso_id' not in Ficha._fields \
                or 'rendimiento_tela_acabada' not in Ficha._fields:
            return 0.0
        ficha = Ficha.search([('product_proceso_id', '=', self.product_id.id)], order='id desc', limit=1)
        return ficha.rendimiento_tela_acabada or 0.0

    @api.depends('metros_rama', 'kg_rama')
    def _compute_rendimiento_rama(self):
        for rollo in self:
            rollo.rendimiento_rama = rollo.metros_rama / rollo.kg_rama if rollo.metros_rama and rollo.kg_rama else 0.0

    @api.depends('metros_insp', 'metros_final', 'kg_rama', 'kg_final', 'clasificacion_inicial',
                 'clasificacion_final', 'product_id')
    def _compute_alertas(self):
        """Alertas de metros (§9.1). Ambos umbrales salen de Ajustes de Manufactura."""
        params = self.env['ir.config_parameter'].sudo()
        limite_metros = float(params.get_param(PARAM_ALERTA_METROS_INSP, 1.0))
        limite_pct = float(params.get_param(PARAM_ALERTA_DIFERENCIA_PCT, 3.0))
        for rollo in self:
            tiene_final = bool(rollo.metros_final)
            rollo.diferencia_metros_insp = rollo.metros_final - rollo.metros_insp if tiene_final else 0.0
            rollo.alerta_metros_insp = tiene_final and abs(rollo.diferencia_metros_insp) > limite_metros
            rollo.rendimiento_real = rollo.metros_final / rollo.kg_final if tiene_final and rollo.kg_final else 0.0
            teorico = rollo._rendimiento_acabado() if tiene_final else 0.0
            rollo.rendimiento_teorico = teorico
            esperado = rollo.kg_final * teorico
            rollo.diferencia_metros_vs_teorico = rollo.metros_final - esperado if esperado else 0.0
            rollo.alerta_diferencia_metros = bool(esperado) and \
                abs(rollo.diferencia_metros_vs_teorico) > esperado * limite_pct / 100.0
            rollo.diferencia_kg = rollo.kg_rama - rollo.kg_final if rollo.kg_final else 0.0
            rollo.reetiquetado = bool(rollo.clasificacion_final and rollo.clasificacion_inicial
                                      and rollo.clasificacion_final != rollo.clasificacion_inicial)

    def write(self, vals):
        # El lote nace en Rama y es inmutable (§12.4).
        if 'lot_id' in vals:
            for rollo in self:
                if rollo.lot_id and rollo.lot_id.id != vals['lot_id']:
                    raise UserError(_('El lote de un rollo no se puede cambiar.'))
        return super().write(vals)

    @api.depends('defecto_ids.clave')
    def _compute_siglas_defectos(self):
        for rollo in self:
            # Las siglas se separan con un espacio en blanco (§10, §12.3).
            rollo.siglas_defectos = ' '.join(rollo.defecto_ids.sorted('clave').mapped('clave'))

    @api.model
    def _zpl_texto(self, texto):
        """Quita los caracteres de control de ZPL (^ y ~) de un texto libre."""
        return (texto or '').replace('^', ' ').replace('~', ' ')

    @api.model
    def _zpl_preliminar_acomodo(self, param_ancho, param_alto, renglones, cantidades, lote, calidad=False):
        """Etiqueta preliminar de 68 x 38 mm (544 x 304 puntos a 203 dpi; escala si Ajustes cambia el tamaño), 3.4.0.
        Arriba los `renglones` (artículo, orden, fecha, centro); en medio las `cantidades` en grande (kg, metros); abajo
        el código de barras del lote con su texto. `calidad` (PQ, FE, 2DA, DESP) va en un recuadro a la derecha."""
        params = self.env['ir.config_parameter'].sudo()
        ancho = int(params.get_param(param_ancho, PRELIM_ANCHO))
        alto = int(params.get_param(param_alto, PRELIM_ALTO))
        escala = min(ancho / PRELIM_ANCHO, alto / PRELIM_ALTO)

        def p(n):
            return int(round(n * escala))

        t = self._zpl_texto
        x = p(12)
        zpl = ['^XA^PW%d^LL%d^CI28' % (ancho, alto)]
        caja = p(140) if calidad else 0
        if calidad:
            # Recuadro de la calidad a la derecha de los renglones, letra según su largo.
            x_caja, letra = ancho - caja - p(8), p(64) if len(calidad) <= 2 else p(46) if len(calidad) == 3 else p(36)
            zpl.append('^FO%d,%d^GB%d,%d,%d^FS' % (x_caja, p(10), caja, p(96), p(4)))
            zpl.append('^FO%d,%d^A0N,%d,%d^FB%d,1,0,C^FD%s^FS' % (
                x_caja, p(10) + (p(96) - letra) // 2 + p(4), letra, letra, caja, t(calidad)))
        y = p(8)
        for i, texto in enumerate(renglones):
            tam = p(26) if i == 0 else p(22)
            zpl.append('^FO%d,%d^A0N,%d,%d^FB%d,1,0,L^FD%s^FS' % (x, y, tam, tam, ancho - caja - p(28), t(texto)))
            y += tam + p(4)  # renglones en 8, 38, 64 y 90; el último termina en 112
        tam = p(40) if len(cantidades) == 1 else p(30)
        zpl.append('^FO%d,%d^A0N,%d,%d^FD%s^FS' % (x, p(118), tam, tam, t('     '.join(cantidades))))
        # Código con su texto abajo: 166 + 84 + ~30 del texto = 280 de 304.
        zpl.append(self._zpl_codigo_barras(lote, x, ancho, p(166), alto_barras=p(84)))
        zpl.append('^XZ')
        return '\n'.join(zpl)

    def _zpl_etiqueta_rama(self):
        """Etiqueta preliminar de Rama (68 x 38 mm, 3.4.0): artículo, orden de trabajo, fecha, Rama, kg netos, metros del
        cuentametros (3.6.0, si se capturaron) y el código de barras del lote (que ya lleva el número de rollo)."""
        self.ensure_one()
        fecha = fields.Datetime.context_timestamp(self, self.fecha_registro_rama or fields.Datetime.now())
        return self._zpl_preliminar_acomodo(PARAM_RAMA_ANCHO, PARAM_RAMA_ALTO, [
            'ARTÍCULO: %s' % (self.product_id.default_code or self.product_id.name or ''),
            'ORDEN: %s' % (self.production_id.name or ''),
            'FECHA: %s' % fecha.strftime('%d/%m/%Y %H:%M'),
            'CENTRO: %s' % (self.workcenter_rama_id.name or ''),
        ], ['KG: %.2f' % self.kg_rama] + (['M: %.2f' % self.metros_rama] if self.metros_rama else []),
            self.lot_id.name or self.name)

    def _zpl_etiqueta_preliminar(self):
        """Etiqueta preliminar de Inspección del flujo anterior a la estación única (F-P-I01-02), con el mismo acomodo de
        68 x 38 mm que la del rollo final (3.4.0)."""
        self.ensure_one()
        fecha = fields.Datetime.context_timestamp(self, self.fecha_registro_insp or fields.Datetime.now())
        return self._zpl_preliminar_acomodo(PARAM_PRELIM_ANCHO, PARAM_PRELIM_ALTO, [
            'ARTÍCULO: %s' % (self.product_id.default_code or self.product_id.name or ''),
            'ORDEN: %s' % (self.production_id.name or ''),
            'FECHA: %s' % fecha.strftime('%d/%m/%Y %H:%M'),
            'CENTRO: %s' % (self.workcenter_insp_id.name or ''),
        ], ['KG: %.2f' % self.kg_rama, 'M: %.2f' % self.metros_insp], self.lot_id.name or self.name,
            CALIDAD_CORTA.get(self.clasificacion_inicial, ''))

    def action_imprimir_etiqueta_preliminar(self):
        self.ensure_one()
        if not self.zpl_etiqueta_preliminar:
            raise UserError(_('Este rollo no tiene etiqueta preliminar.'))
        return self.env.ref('quimibond_rollos_bitacora.action_report_etiqueta_preliminar').report_action(self)

    # ------------------------------------------------------------ etiquetas finales
    def _datos_etiqueta_final(self):
        """Datos del producto B para la etiqueta final. Ancho de la ficha de acabado (si no hay, posiciones
        12-14 del código, DAT P-D02-01); color decodificado de las posiciones 10-11 del código."""
        self.ensure_one()
        producto = self.production_b_id.product_id
        codigo = producto.default_code or ''
        Ficha = self.env.get('ficha.tecnica.acabado')
        ficha = None
        if Ficha is not None:
            ficha = Ficha.search([('product_acabado_id', '=', producto.id)], order='id desc', limit=1) \
                or Ficha.search([('product_proceso_id', '=', self.product_id.id)], order='id desc', limit=1)
        ancho = ficha.ancho_acabado if ficha and 'ancho_acabado' in ficha._fields else 0
        if not ancho and codigo[11:14].isdigit():
            ancho = int(codigo[11:14])
        return {
            'codigo': codigo,
            'color': COLORES.get(codigo[9:11].upper(), codigo[9:11]),
            'ancho': ancho,
        }

    @api.model
    def _code128_zpl(self, dato):
        """Code 128 con los subconjuntos explícitos de ZPL (>: inicio B, >; inicio C, >5 a C, >6 a B): los dígitos
        seguidos van de dos en dos en C (4 o más al principio o al final, 6 o más en medio, la regla usual). Devuelve
        (datos para ^FD, módulos del código completo con inicio, control y paro). 3.3.7: en 76 mm de ancho el dato
        metros;kg;orden;rollo cabe con barras de 2 puntos."""
        dato = self._zpl_texto(dato)
        n = len(dato)

        def digitos(i):
            j = i
            while j < n and dato[j].isdigit():
                j += 1
            return j - i

        partes, simbolos, i = [], 0, 0
        corrida = digitos(0)
        subconjunto = 'C' if corrida >= 4 or (corrida == n and corrida % 2 == 0 and n) else 'B'
        partes.append('>;' if subconjunto == 'C' else '>:')
        while i < n:
            corrida = digitos(i)
            if subconjunto == 'C':
                if corrida >= 2:
                    partes.append(dato[i:i + 2])
                    simbolos, i = simbolos + 1, i + 2
                    continue
                partes.append('>6')
                simbolos, subconjunto = simbolos + 1, 'B'
                continue
            if corrida >= 4 and (i + corrida == n or corrida >= 6):
                if corrida % 2:  # el dígito sobrante va en B antes de pasar a C
                    partes.append(dato[i])
                    simbolos, i = simbolos + 1, i + 1
                partes.append('>5')
                simbolos, subconjunto = simbolos + 1, 'C'
                continue
            partes.append('><' if dato[i] == '>' else dato[i])
            simbolos, i = simbolos + 1, i + 1
        return ''.join(partes), (simbolos + 2) * 11 + 13

    def _zpl_codigo_barras(self, dato, x_min, ancho, y, alto_barras=70):
        """Code 128 centrado: el módulo más grande que deja el código dentro de la etiqueta."""
        datos, modulos = self._code128_zpl(dato)
        modulo = next((m for m in (3, 2, 1) if modulos * m <= ancho - 2 * x_min), 1)
        x = max((ancho - modulos * modulo) // 2, x_min)
        return '^BY%d,3,%d^FO%d,%d^BCN,%d,Y,N,N^FD%s^FS' % (modulo, alto_barras, x, y, alto_barras, datos)

    @api.model
    def _zpl_final_acomodo(self, clasif, renglones, codigos):
        """Acomodo de la etiqueta final (§12.3) en el tamaño de Ajustes; el diseño es de 76 x 102 mm (608 x 816
        puntos) y escala si se cambia. `renglones`: texto o (izquierda, derecha) en dos columnas; `codigos`: los
        datos de 1 (PQ) o 2 (PNC) códigos de barras. La usan el rollo final y la bitácora."""
        params = self.env['ir.config_parameter'].sudo()
        ancho = int(params.get_param(PARAM_FINAL_ANCHO, FINAL_ANCHO))
        alto = int(params.get_param(PARAM_FINAL_ALTO, FINAL_ALTO))
        escala = min(ancho / float(FINAL_ANCHO), alto / float(FINAL_ALTO))

        def p(valor):
            return max(int(round(valor * escala)), 1)

        t = self._zpl_texto
        pnc = clasif != 'PQ'
        x, caja = p(20), ancho - 2 * p(20)
        columna = ancho // 2 + p(4)
        titulo = 'ETIQUETA DE PRODUCTO CON DEFECTIVA' if pnc else 'ETIQUETA DE PRODUCTO TERMINADO'
        zpl = ['^XA^PW%d^LL%d^CI28' % (ancho, alto),
               '^FO%d,%d^A0N,%d,%d^FB%d,1,0,C^FD%s^FS' % (x, p(16), p(22), p(20), caja, EMPRESA_ETIQUETA),
               '^FO%d,%d^A0N,%d,%d^FB%d,1,0,C^FD%s^FS' % (x, p(46), p(24), p(24), caja, titulo)]
        y = p(86)
        if pnc:
            zpl.append('^FO%d,%d^A0N,%d,%d^FB%d,1,0,C^FD%s^FS' % (x, y, p(36), p(36), caja, LEYENDA_PNC[clasif]))
            y += p(48)
        for renglon in renglones:
            izquierda, derecha = renglon if isinstance(renglon, tuple) else (renglon, None)
            zpl.append('^FO%d,%d^A0N,%d,%d^FD%s^FS' % (x, y, p(26), p(26), t(izquierda)))
            if derecha:
                zpl.append('^FO%d,%d^A0N,%d,%d^FD%s^FS' % (columna, y, p(26), p(26), t(derecha)))
            y += p(38)
        y += p(10)
        for dato in codigos:
            zpl.append(self._zpl_codigo_barras(dato, p(12), ancho, y, alto_barras=p(70)))
            y += p(120)
        zpl.append('^XZ')
        return '\n'.join(zpl)

    def _zpl_etiqueta_final(self):
        """Etiquetas finales (§12.3). PQ: neto/bruto del segundo pesaje y un código de barras
        (Metros;Peso Neto;OF;No. Rollo). FE / SEGUNDA / DESP (PNC): kilos y libras, siglas de defectos y un
        segundo código (No. Rollo + siglas). Acomodo y tamaño en `_zpl_final_acomodo` (76 x 102 mm)."""
        self.ensure_one()
        datos = self._datos_etiqueta_final()
        clasif = self.clasificacion_final
        pnc = clasif != 'PQ'
        fecha = fields.Datetime.context_timestamp(self, self.fecha_registro_final or fields.Datetime.now())
        lote = self.lot_id.name or self.name
        of = lote.rsplit('-', 1)[0]
        metros, kg, bruto = self.metros_final, self.kg_final, self.kg_bruto_final
        renglones = [
            'CÓDIGO: %s%s' % (datos['codigo'], SUFIJO_CODIGO.get(clasif, '')),
            'COLOR: %s' % datos['color'],
            'ANCHO (cm): %g' % datos['ancho'],
            ('METROS: %.2f' % metros, 'YARDAS: %.2f' % (metros * YARDAS_POR_METRO)),
            ('KILOS: %.2f' % kg, 'LIBRAS: %.2f' % (kg * LIBRAS_POR_KG)) if pnc
            else ('PESO NETO: %.2f' % kg, 'PESO BRUTO: %.2f' % bruto),
            'LOTE DE PRODUCCIÓN: %s' % of,
            'NO. DE ROLLO: %s' % self.secuencia,
            'OPERADOR INSPECCIÓN: %s' % (self.siglas_operador_final or ''),
            'FECHA: %s' % fecha.strftime('%d/%m/%Y'),
        ]
        codigos = ['%.2f;%.3f;%s;%s' % (metros, kg, of, self.secuencia)]
        if pnc:
            renglones.append('DEFECTOS: %s' % (self.siglas_defectos or ''))
            codigos.append(('%s %s' % (self.secuencia, self.siglas_defectos or '')).strip())
        return self._zpl_final_acomodo(clasif, renglones, codigos)

    def action_imprimir_etiqueta_final(self):
        self.ensure_one()
        if not self.zpl_etiqueta_final:
            raise UserError(_('Este rollo no tiene etiqueta final.'))
        return self.env.ref('quimibond_rollos_bitacora.action_report_etiqueta_final').report_action(self)

    def action_imprimir_etiqueta_rama(self):
        self.ensure_one()
        if not self.zpl_etiqueta_rama:
            raise UserError(_('Este rollo no tiene etiqueta de Rama.'))
        return self.env.ref('quimibond_rollos_bitacora.action_report_etiqueta_rama').report_action(self)


class MrpRolloMedicionRama(models.Model):
    """Mediciones de proceso de Rama por rollo (zona/apartado y parámetros de máquina), §9.1."""
    _name = 'mrp.rollo.medicion.rama'
    _description = 'Medición de Rama por rollo'
    _order = 'bitacora_id, zona, apartado'

    bitacora_id = fields.Many2one('mrp.rollo.bitacora', 'Rollo', required=True, ondelete='cascade', index=True)
    production_id = fields.Many2one(related='bitacora_id.production_id', store=True)
    secuencia = fields.Integer(related='bitacora_id.secuencia', store=True)
    zona = fields.Selection([('1', '1'), ('2', '2'), ('3', '3'), ('4', '4')], 'Zona', required=True)
    apartado = fields.Selection([('1', '1'), ('2', '2')], 'Apartado', required=True)
    valor_medicion = fields.Float('Valor de medición', digits=(12, 3))
    temperatura = fields.Float('Temperatura', digits=(12, 1))
    velocidad_cadena = fields.Float('Velocidad de cadena', digits=(12, 2))
    extraccion = fields.Float('Extracción', digits=(12, 2))
    alimentacion_rodillos = fields.Float('Alimentación de rodillos', digits=(12, 2))

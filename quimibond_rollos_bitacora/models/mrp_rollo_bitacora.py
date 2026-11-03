# -*- coding: utf-8 -*-
from odoo import api, fields, models, _
from odoo.exceptions import UserError

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

    def _zpl_etiqueta_preliminar(self):
        """ETIQUETA DE IDENTIFICACIÓN PRELIMINAR DE INSPECCIÓN (F-P-I01-02, en kg), §12.2.

        Los Kgs son el neto de Rama y los metros los medidos en Inspección. Tamaño en puntos ZPL desde
        Ajustes (provisional: 812x609 = 10x7.5 cm a 203 dpi). Código de barras = lote-serie del rollo.
        """
        self.ensure_one()
        params = self.env['ir.config_parameter'].sudo()
        ancho = int(params.get_param('quimibond_rollos_bitacora.etiqueta_prelim_ancho', 812))
        alto = int(params.get_param('quimibond_rollos_bitacora.etiqueta_prelim_alto', 609))
        t = self._zpl_texto
        fecha = fields.Datetime.context_timestamp(self, self.fecha_registro_insp or fields.Datetime.now())
        centro = self.workcenter_insp_id
        revisadora = ''.join(ch for ch in (centro.name or '') if ch.isdigit()) or (centro.code or '')
        clases = [('PQ', 'PQ'), ('FE', 'FE'), ('SEGUNDA', '2das.'), ('DESP', 'DESP.')]
        marcas = '   '.join('[%s] %s' % ('X' if self.clasificacion_inicial == k else ' ', txt) for k, txt in clases)
        lote = self.lot_id.name or self.name
        modulos = (len(lote) + 3) * 11 + 2
        modulo = next((m for m in (3, 2, 1) if modulos * m <= ancho - 40), 1)
        x_barras = max((ancho - modulos * modulo) // 2, 0)
        renglones = [
            ('ARTÍCULO: %s' % (self.product_id.default_code or ''), 28),
            ('ORDEN DE TRABAJO: %s' % (self.production_id.name or ''), 28),
            ('FECHA: %s' % fecha.strftime('%d/%m/%Y %H:%M'), 28),
            ('REVISADORA No.: %s     OPERADOR: %s' % (revisadora, self.operador_insp or ''), 28),
            ('NO. ROLLO: %s' % self.secuencia, 28),
            (marcas, 30),
            ('CANTIDAD MTS: %.2f     KGS: %.2f' % (self.metros_insp, self.kg_rama), 30),
            ('CALIDAD: %s     NO. EMPALMES: %s' % (self.clasificacion_inicial or '', self.no_empalmes), 28),
        ]
        zpl = ['^XA^PW%d^LL%d^CI28' % (ancho, alto),
               '^FO20,15^A0N,26,26^FDPRODUCTORA DE NO TEJIDOS QUIMIBOND S.A. DE C.V.^FS',
               '^FO20,48^A0N,24,24^FDETIQUETA DE IDENTIFICACIÓN PRELIMINAR DE INSPECCIÓN^FS']
        y = 95
        for texto, tam in renglones:
            zpl.append('^FO20,%d^A0N,%d,%d^FD%s^FS' % (y, tam, tam, t(texto)))
            y += 48
        zpl.append('^BY%d,3,80^FO%d,%d^BCN,80,Y,N,N^FD%s^FS' % (modulo, x_barras, min(y + 10, alto - 120), t(lote)))
        zpl.append('^XZ')
        return '\n'.join(zpl)

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

    def _zpl_codigo_barras(self, dato, x_min, ancho, y, alto_barras=70):
        """Code 128 centrado: el módulo más grande que deja el código dentro de la etiqueta."""
        modulos = (len(dato) + 3) * 11 + 2
        modulo = next((m for m in (3, 2, 1) if modulos * m <= ancho - 2 * x_min), 1)
        x = max((ancho - modulos * modulo) // 2, x_min)
        return '^BY%d,3,%d^FO%d,%d^BCN,%d,Y,N,N^FD%s^FS' % (
            modulo, alto_barras, x, y, alto_barras, self._zpl_texto(dato))

    def _zpl_etiqueta_final(self):
        """Etiquetas finales (§12.3). PQ: neto/bruto del segundo pesaje y un código de barras
        (Metros;Peso Neto;OF;No. Rollo). FE / SEGUNDA / DESP (PNC): kilos y libras, siglas de defectos y un
        segundo código (No. Rollo + siglas). Tamaño en puntos ZPL desde parámetros (provisional 812x1218)."""
        self.ensure_one()
        params = self.env['ir.config_parameter'].sudo()
        ancho = int(params.get_param('quimibond_rollos_bitacora.etiqueta_final_ancho', 812))
        alto = int(params.get_param('quimibond_rollos_bitacora.etiqueta_final_alto', 1218))
        t = self._zpl_texto
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
            'METROS: %.2f     YARDAS: %.2f' % (metros, metros * YARDAS_POR_METRO),
        ]
        if pnc:
            renglones.append('KILOS: %.2f     LIBRAS: %.2f' % (kg, kg * LIBRAS_POR_KG))
        else:
            renglones.append('PESO NETO: %.2f     PESO BRUTO: %.2f' % (kg, bruto))
        renglones += [
            'LOTE DE PRODUCCIÓN: %s' % of,
            'NO. DE ROLLO: %s' % self.secuencia,
            'OPERADOR INSPECCIÓN: %s' % (self.siglas_operador_final or ''),
            'FECHA: %s' % fecha.strftime('%d/%m/%Y'),
        ]
        if pnc:
            renglones.append('DEFECTOS: %s' % (self.siglas_defectos or ''))
        titulo = 'ETIQUETA DE PRODUCTO CON DEFECTIVA' if pnc else 'ETIQUETA DE PRODUCTO TERMINADO'
        zpl = ['^XA^PW%d^LL%d^CI28' % (ancho, alto),
               '^FO30,25^A0N,30,30^FDPRODUCTORA DE NO TEJIDOS QUIMIBOND S.A. DE C.V.^FS',
               '^FO30,70^A0N,28,28^FD%s^FS' % t(titulo)]
        if pnc:
            zpl.append('^FO30,110^A0N,40,40^FD%s^FS' % {'FE': 'FUERA DE ESPECIFICACIÓN', 'SEGUNDA': 'SEGUNDA',
                                                         'DESP': 'DESPERDICIO'}[clasif])
        y = 170
        for texto in renglones:
            zpl.append('^FO30,%d^A0N,34,34^FD%s^FS' % (y, t(texto)))
            y += 58
        zpl.append(self._zpl_codigo_barras('%.2f;%.3f;%s;%s' % (metros, kg, of, self.secuencia), 30, ancho, y + 10))
        if pnc:
            zpl.append(self._zpl_codigo_barras(
                ('%s %s' % (self.secuencia, self.siglas_defectos or '')).strip(), 30, ancho, y + 140))
        zpl.append('^XZ')
        return '\n'.join(zpl)

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

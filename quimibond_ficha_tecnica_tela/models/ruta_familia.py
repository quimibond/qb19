# -*- coding: utf-8 -*-
"""Rutas por familia y listas generadas desde la ficha (2.23.0, Dirección General 2026-11-03).

Ingeniería mantiene una ruta por familia (las telas del mismo crudo) y la ficha de cada tela; Odoo arma la lista en kg
y la de metros. Las reglas salen de la tabla «rutas de proceso rev4» de Innovación (2026-07-15, 105 artículos):

- Devanado, abridora, rama (termofijado) e inspección en todas las telas. La abridora aplica aunque el tejido salga
  abierto (la tela sale del jet en cuerda); abierto o tubular solo cambia la velocidad, que ya usa el cálculo de tiempo.
- Tintorería: **lavado** si la tela es natural u off white (color NT, NN, OW en el código), **teñido** si lleva color,
  blanco óptico incluido. El jet es el mismo; cambian la fórmula y el tiempo. Una tela sin tintorería (WJ032) se marca
  en su ficha.
- Abridora volteado: solo algunas familias (WD038): es un paso más de la ruta de esa familia.
- La rama (1, 2 o 3) y el jet son de la familia; una tela que va a otra los lleva en su ficha.

Reglas de seguridad: nunca se toca una lista **activa** (la cambia Ingeniería con su visto bueno); lo generado nace
archivado como borrador del flujo, y los tiempos, la capacidad de los jets y la conversión a metros salen de la ficha
(«Usar los datos de la ficha»). Los componentes que no son el crudo (fórmula, engomado) se conservan.

Saldos (2.24.0, Dirección General 2026-11-03): la lista en metros lleva de subproductos los dos saldos en kg en que
se venden las segundas y el desperdicio de la clasificación (``SALDO DE TELA TEJIDO`` y ``SALDO TEJIDO D``; las
referencias están en los parámetros ``quimibond_ficha_tecnica_tela.saldo_segunda`` y ``.saldo_desperdicio``), con
cantidad 0: la real es el peso de cada rollo al clasificar. **La lista en metros no lleva operaciones**: Inspección
(en la orden en kg) corta, pesa, clasifica y etiqueta cada rollo, y la orden en metros se registra sola al cerrar el
baño; sin paso «Clasificación final» en la ruta, «Generar listas» le quita la de Empaque. Diseño:
``docs/superpowers/specs/2026-11-03-inspeccion-clasificacion-rollos-design.md``.
"""
from collections import Counter

from odoo import api, fields, models

PASOS = [
    ('devanado', 'Devanado'),
    ('tintoreria', 'Tintorería'),
    ('abridora_volteado', 'Abridora volteado'),
    ('abridora', 'Abridora'),
    ('rama', 'Rama (termofijado)'),
    ('inspeccion', 'Inspección'),
    ('clasificacion', 'Clasificación final (lista en metros)'),
]
CONDICIONES = [('siempre', 'Siempre'), ('tenido', 'Solo si se tiñe'), ('lavado', 'Solo si se lava')]
TINTORERIA = [('lavado', 'Lavado'), ('tenido', 'Teñido'), ('ninguna', 'Sin tintorería')]
# Tabla de Innovación (rev4): natural y off white se lavan; todo lo demás (blanco óptico incluido) se tiñe.
COLORES_LAVADO = ('NT', 'NN', 'OW')
SECUENCIA = {'devanado': 10, 'tintoreria': 20, 'abridora_volteado': 24, 'abridora': 25, 'rama': 30,
             'inspeccion': 40, 'clasificacion': 10}
NOMBRE_OPERACION = {'devanado': 'DEVANADO', 'abridora_volteado': 'ABRIDORA VOLTEADO', 'abridora': 'ABRIDORA',
                    'rama': 'RAMA', 'inspeccion': 'INSPECCION', 'clasificacion': 'CLASIFICACION'}
# Tipo de máquina de la ficha → paso (para reconocer las operaciones de una lista armada a mano).
PASO_POR_TIPO = {'devanadora': 'devanado', 'tintoreria': 'tintoreria', 'abridora': 'abridora', 'acabado': 'rama',
                 'inspeccion': 'inspeccion'}
# Tabla de Innovación (rev4): pasos que llevan todas las telas, y los que tienen una máquina estándar en la planta.
PASOS_TODAS = ('devanado', 'tintoreria', 'abridora', 'rama', 'inspeccion')
PASOS_PLANTA = ('devanado', 'abridora', 'inspeccion')
CODIGO_KG = 'Flujo en kg (borrador)'
CODIGO_M = 'Clasificación final (borrador)'
# Subproductos de la lista en metros: (parámetro, referencia por defecto). Segundas y desperdicio se venden en kg.
SALDOS = (('quimibond_ficha_tecnica_tela.saldo_segunda', 'SALDO DE TELA TEJIDO'),
          ('quimibond_ficha_tecnica_tela.saldo_desperdicio', 'SALDO TEJIDO D'))


def tintoreria_por_codigo(code):
    """'lavado' o 'tenido' según el color del código de la tela acabada (posiciones 10 y 11: WJ045Q22JNT160 → NT)."""
    code = (code or '').strip().upper()
    if code.startswith('I'):
        code = code[1:]
    return 'lavado' if code[9:11] in COLORES_LAVADO else 'tenido'


class FichaTecnicaRuta(models.Model):
    _name = 'ficha.tecnica.ruta'
    _description = 'Ruta de una familia de telas (las del mismo crudo)'
    _order = 'name'

    name = fields.Char(string='Familia', compute='_compute_name', store=True)
    crudo_id = fields.Many2one('product.product', string='Crudo de la familia', required=True, index=True,
                               ondelete='restrict')
    paso_ids = fields.One2many('ficha.tecnica.ruta.paso', 'ruta_id', string='Pasos', copy=True)
    acabado_ids = fields.Many2many('ficha.tecnica.acabado', string='Telas de la familia', compute='_compute_acabados')
    acabado_count = fields.Integer(string='Telas', compute='_compute_acabados')
    resumen = fields.Char(string='Ruta', compute='_compute_resumen')
    notas = fields.Text(string='Notas')
    active = fields.Boolean(default=True)

    _crudo_uniq = models.Constraint('unique(crudo_id)', 'Ese crudo ya tiene su ruta de familia.')

    @api.depends('crudo_id.default_code', 'crudo_id.name')
    def _compute_name(self):
        for ruta in self:
            ruta.name = ruta.crudo_id.default_code or ruta.crudo_id.name or ''

    def _compute_acabados(self):
        Acabado = self.env['ficha.tecnica.acabado']
        for ruta in self:
            acabados = Acabado.search([('tejido_id.product_proceso_id', '=', ruta.crudo_id.id)]) \
                if ruta.crudo_id else Acabado
            ruta.acabado_ids = acabados
            ruta.acabado_count = len(acabados)

    @api.depends('paso_ids.paso', 'paso_ids.workcenter_id', 'paso_ids.condicion', 'paso_ids.sequence')
    def _compute_resumen(self):
        for ruta in self:
            partes = []
            for paso in ruta.paso_ids.filtered(lambda p: p.paso != 'clasificacion').sorted(lambda p: (p.sequence, p.id)):
                texto = paso.workcenter_id.name or ('%s (sin máquina)' % dict(PASOS)[paso.paso])
                if paso.condicion != 'siempre':
                    texto += ' (%s)' % dict(CONDICIONES)[paso.condicion].lower()
                partes.append(texto)
            ruta.resumen = ' → '.join(partes)

    def action_ft_generar_listas(self):
        """Genera las listas de todas las telas de la familia (solo las archivadas; nunca las activas)."""
        return self.acabado_ids.action_ft_generar_listas()

    def action_ft_telas(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': 'Telas de %s' % self.name, 'res_model': 'ficha.tecnica.acabado',
                'view_mode': 'list,form', 'domain': [('id', 'in', self.acabado_ids.ids)]}

    # ------------------------------------------------------------------
    # Armar las rutas desde las listas que ya existen
    # ------------------------------------------------------------------
    @api.model
    def _ft_paso_de_operacion(self, op):
        tipo = op.workcenter_id.ft_tipo_id.code
        if tipo == 'abridora' and 'VOLTEAD' in (op.name or '').upper():
            return 'abridora_volteado'
        return PASO_POR_TIPO.get(tipo)

    @api.model
    def _ft_armar(self, crudo, acabados):
        """Lo que dicen las listas del flujo de las telas de un crudo: {paso: Counter(máquina)}, la Clasificación
        (máquina, tiempo) más usada de sus listas en metros y, por tela, (máquinas por paso, consumo de crudo)."""
        Revision = self.env['ficha.tecnica.revision']
        por_paso = {}
        por_tela = {}
        clasificacion = Counter()
        for ficha in acabados:
            bom_kg = Revision._ft_bom_de(ficha.product_proceso_id) if ficha.product_proceso_id else self.env['mrp.bom']
            maquinas = {}
            for op in bom_kg.with_context(active_test=False).operation_ids:
                paso = self._ft_paso_de_operacion(op)
                if paso and paso not in maquinas:
                    maquinas[paso] = op.workcenter_id
                    por_paso.setdefault(paso, Counter())[op.workcenter_id.id] += 1
            consumo = sum(line.product_qty for line in bom_kg.bom_line_ids if line.product_id == crudo)
            por_tela[ficha] = (maquinas, consumo)
            bom_m = Revision._ft_bom_de(ficha.product_acabado_id) if ficha.product_acabado_id else self.env['mrp.bom']
            # 2.23.0: solo máquinas sin tipo de ficha (Empaque); una lista en metros vieja puede traer una circular.
            for op in bom_m.with_context(active_test=False).operation_ids.filtered(lambda o: not o.workcenter_id.ft_tipo_id):
                clasificacion[(op.workcenter_id.id, op.time_cycle_manual)] += 1
        return por_paso, (clasificacion.most_common(1)[0][0] if clasificacion else None), por_tela

    def action_ft_armar_desde_listas(self):
        """Botón de la lista: arma la ruta de cada crudo que no la tiene y completa las que ya existen.

        Cada paso toma la máquina más usada por las telas de la familia. Los pasos que la tabla de Innovación pone en
        todas las telas (devanado, tintorería, abridora, rama, inspección) se agregan aunque falten en las listas (les
        faltaba el dato): devanado, abridora e inspección con la máquina más usada en toda la planta; tintorería y rama
        sin máquina si la familia no la tiene, para que lo defina Ingeniería. Nunca cambia un paso que ya existe.
        Método de registros: el botón de la cabecera llega con los renglones seleccionados."""
        Ruta = self.env['ficha.tecnica.ruta'].with_context(active_test=False)
        Paso = self.env['ficha.tecnica.ruta.paso']
        Acabado = self.env['ficha.tecnica.acabado']
        acabados = Acabado.search([('tejido_id.product_proceso_id', '!=', False)])
        datos = {crudo: self._ft_armar(crudo, acabados.filtered(lambda f, c=crudo: f.tejido_id.product_proceso_id == c))
                 for crudo in acabados.tejido_id.product_proceso_id}
        planta = {}
        for paso in PASOS_PLANTA:
            total = Counter()
            for por_paso, _clas, _telas in datos.values():
                total.update(por_paso.get(paso, Counter()))
            planta[paso] = total.most_common(1)[0][0] if total else False
        rutas = {ruta.crudo_id: ruta for ruta in Ruta.search([])}
        creadas = completadas = 0
        nombres = dict(PASOS)
        for crudo, (por_paso, clasif, por_tela) in datos.items():
            familia = {paso: contador.most_common(1)[0][0] for paso, contador in por_paso.items()}
            ruta = rutas.get(crudo)
            existentes = set(ruta.paso_ids.mapped('paso')) if ruta else set()
            nuevos, de_planta, sin_maquina = [], [], []
            for paso, _n in PASOS:
                if paso in existentes:
                    continue
                if paso == 'clasificacion':
                    # 2.24.0: la lista en metros no lleva operación (la inspección pesa, clasifica y etiqueta en la
                    # orden en kg; Dirección General 2026-11-03). No se arma el paso desde listas viejas con Empaque.
                    continue
                wc = familia.get(paso)
                if not wc and paso in PASOS_TODAS:
                    wc = planta.get(paso)
                    (de_planta if wc else sin_maquina).append(nombres[paso])
                elif not wc:
                    continue
                nuevos.append({'paso': paso, 'sequence': SECUENCIA[paso], 'workcenter_id': wc or False})
            if not nuevos:
                continue
            nota = ''
            if de_planta or sin_maquina:
                nota = 'Tabla de Innovación (rev4): %s%s.' % (
                    ('%s con la máquina más usada en la planta' % ', '.join(de_planta)) if de_planta else '',
                    ('%s%s sin máquina (por definir)' % ('; ' if de_planta else '', ', '.join(sin_maquina)))
                    if sin_maquina else '')
            if ruta:
                Paso.create([dict(vals, ruta_id=ruta.id) for vals in nuevos])
                if nota:
                    ruta.notas = ((ruta.notas + '\n') if ruta.notas else '') + nota
                completadas += 1
                continue
            ruta = Ruta.create({'crudo_id': crudo.id, 'paso_ids': [(0, 0, vals) for vals in nuevos], 'notas': nota or False})
            creadas += 1
            for ficha, (maquinas, consumo) in por_tela.items():
                vals = {}
                if maquinas.get('rama') and maquinas['rama'].id != familia.get('rama'):
                    vals['ruta_rama_id'] = maquinas['rama'].id
                if maquinas.get('tintoreria') and maquinas['tintoreria'].id != familia.get('tintoreria'):
                    vals['ruta_jet_id'] = maquinas['tintoreria'].id
                if consumo and not ficha.consumo_crudo:
                    vals['consumo_crudo'] = consumo
                if vals:
                    ficha.write(vals)
        partes = []
        if creadas:
            partes.append('%s rutas nuevas' % creadas)
        if completadas:
            partes.append('%s rutas completadas con los pasos de la tabla de Innovación' % completadas)
        return {'type': 'ir.actions.client', 'tag': 'display_notification', 'params': {
            'title': 'Rutas por familia', 'type': 'success' if partes else 'info',
            'message': ('; '.join(partes) + '.') if partes else 'Las rutas ya tienen todos sus pasos.',
            'next': {'type': 'ir.actions.client', 'tag': 'reload'}}}


    @api.model
    def _ft_habilitar_mcp(self):
        """2.23.0: expone por MCP las rutas y la revisión, si el servidor MCP del repo está instalado (corre al instalar
        y al actualizar; los modelos que ya estén configurados no se tocan)."""
        if 'mcp.enabled.model' not in self.env:
            return False
        Enabled = self.env['mcp.enabled.model'].sudo().with_context(active_test=False)
        creados = Enabled
        for model, escribir in (('ficha.tecnica.ruta', True), ('ficha.tecnica.ruta.paso', True),
                                ('ficha.tecnica.revision', False), ('ficha.tecnica.revision.pendiente', False)):
            ir_model = self.env['ir.model']._get(model)
            if not ir_model or Enabled.search([('model_id', '=', ir_model.id)], limit=1):
                continue
            creados |= Enabled.create({
                'model_id': ir_model.id, 'active': True, 'allow_read': True, 'allow_create': escribir,
                'allow_write': escribir, 'allow_unlink': False, 'allow_method_calls': True,
                'notes': 'quimibond_ficha_tecnica_tela: habilitado al instalar o actualizar.'})
        return bool(creados)


class FichaTecnicaRutaPaso(models.Model):
    _name = 'ficha.tecnica.ruta.paso'
    _description = 'Paso de la ruta de una familia de telas'
    _order = 'ruta_id, sequence, id'

    ruta_id = fields.Many2one('ficha.tecnica.ruta', string='Ruta', required=True, index=True, ondelete='cascade')
    sequence = fields.Integer(string='Orden', default=10)
    paso = fields.Selection(PASOS, string='Paso', required=True)
    workcenter_id = fields.Many2one('mrp.workcenter', string='Máquina',
                                    help='Sin máquina, el paso no se genera y se avisa al generar las listas.')
    condicion = fields.Selection(CONDICIONES, string='Cuándo', required=True, default='siempre')
    tiempo = fields.Float(string='Tiempo fijo (min por unidad)', digits=(12, 4),
                          help='Solo para pasos cuyo tiempo no sale de la ficha (por ejemplo, la Clasificación final). '
                               'Los demás tiempos los calcula «Usar los datos de la ficha».')

    @api.onchange('paso')
    def _onchange_paso(self):
        if self.paso:
            self.sequence = SECUENCIA[self.paso]


class FichaTecnicaAcabadoRutaFamilia(models.Model):
    _inherit = 'ficha.tecnica.acabado'

    ruta_familia_id = fields.Many2one('ficha.tecnica.ruta', string='Ruta de la familia',
                                      compute='_compute_ruta_familia')
    ruta_tintoreria_manual = fields.Selection(
        TINTORERIA, string='Tintorería (si no es la del código)',
        help='Vacío: lavado si el color del código es NT, NN u OW; teñido en los demás (tabla de Innovación).')
    ruta_tintoreria = fields.Selection(TINTORERIA, string='Tintorería', compute='_compute_ruta_familia')
    ruta_rama_id = fields.Many2one('mrp.workcenter', string='Rama propia de la tela',
                                   help='Solo si la tela no va a la rama de su familia.')
    ruta_jet_id = fields.Many2one('mrp.workcenter', string='Jet propio de la tela',
                                  help='Solo si la tela no va al jet de su familia (por ejemplo, los colores oscuros).')
    consumo_crudo = fields.Float(string='Consumo de crudo (kg por kg)', digits=(12, 4),
                                 help='Kg de crudo por kg de acabado: el renglón del crudo en la lista en kg.')
    ruta_resumen = fields.Char(string='Ruta que se genera', compute='_compute_ruta_familia')

    @api.depends('tejido_id.product_proceso_id', 'ruta_tintoreria_manual', 'ruta_rama_id', 'ruta_jet_id',
                 'product_acabado_id.default_code', 'articulo')
    def _compute_ruta_familia(self):
        Ruta = self.env['ficha.tecnica.ruta']
        for ficha in self:
            crudo = ficha.tejido_id.product_proceso_id
            ficha.ruta_familia_id = Ruta.search([('crudo_id', '=', crudo.id)], limit=1) if crudo else Ruta
            ficha.ruta_tintoreria = ficha.ruta_tintoreria_manual or tintoreria_por_codigo(
                ficha.product_acabado_id.default_code or ficha.articulo)
            ficha.ruta_resumen = ' → '.join(
                (wc.name if wc else '%s (sin máquina)' % dict(PASOS)[paso.paso])
                for paso, wc in ficha._ft_pasos('kg')) if ficha.ruta_familia_id else False

    def _ft_pasos(self, lista):
        """[(paso de la ruta, máquina)] de la tela para la lista en kg o en metros, en orden."""
        self.ensure_one()
        res = []
        for paso in self.ruta_familia_id.paso_ids.sorted(lambda p: (p.sequence, p.id)):
            if (paso.paso == 'clasificacion') != (lista == 'm'):
                continue
            if paso.condicion != 'siempre' and paso.condicion != self.ruta_tintoreria:
                continue
            if paso.paso == 'tintoreria' and self.ruta_tintoreria == 'ninguna':
                continue
            wc = paso.workcenter_id
            if paso.paso == 'rama' and self.ruta_rama_id:
                wc = self.ruta_rama_id
            elif paso.paso == 'tintoreria' and self.ruta_jet_id:
                wc = self.ruta_jet_id
            res.append((paso, wc))
        return res

    def _ft_nombre_operacion(self, paso, producto):
        if paso.paso == 'tintoreria':
            nombre = 'TINTORERIA %s' % ('LAVADO' if self.ruta_tintoreria == 'lavado' else 'TEÑIDO')
        else:
            nombre = NOMBRE_OPERACION[paso.paso]
        return '%s %s' % (nombre, producto.default_code or producto.name)

    def _ft_poner_operaciones(self, bom, pasos, producto):
        """Deja en la lista exactamente las operaciones de la ruta. Si ya están (misma máquina y orden) no las toca;
        si no, las rehace y pasa los componentes que iban en una operación a la del mismo paso (el crudo a la primera)."""
        Ruta = self.env['ficha.tecnica.ruta']
        avisos = []
        pasos_ok = [(paso, wc) for paso, wc in pasos if wc]
        for paso, wc in pasos:
            if not wc:
                avisos.append('%s: el paso «%s» de la ruta no tiene máquina.' % (self.articulo, dict(PASOS)[paso.paso]))
        viejas = bom.with_context(active_test=False).operation_ids.sorted(lambda o: (o.sequence, o.id))
        actual = [(o.workcenter_id.id, o.sequence) for o in viejas]
        deseado = [(wc.id, paso.sequence) for paso, wc in pasos_ok]
        if actual == deseado:
            return avisos
        Op = self.env['mrp.routing.workcenter'].with_context(active_test=False)
        nuevas = {}
        primera = Op
        for paso, wc in pasos_ok:
            vieja = viejas.filtered(lambda o: o.workcenter_id == wc)[:1]
            op = Op.create({'bom_id': bom.id, 'name': self._ft_nombre_operacion(paso, producto),
                            'sequence': paso.sequence, 'workcenter_id': wc.id, 'time_mode': 'manual',
                            'time_cycle_manual': vieja.time_cycle_manual if vieja else paso.tiempo,
                            'active': bom.active})
            nuevas.setdefault(paso.paso, op)
            primera = primera or op
        for line in bom.bom_line_ids:
            if line.operation_id in viejas:
                paso = Ruta._ft_paso_de_operacion(line.operation_id)
                line.operation_id = nuevas.get(paso) or primera
        viejas.unlink()
        return avisos

    def _ft_poner_saldos(self, bom):
        """Subproductos de saldo (segundas y desperdicio, en kg) en la lista en metros, con cantidad 0. Los que ya
        están no se tocan; una referencia que no existe se avisa y se omite (no se inventa el producto)."""
        params = self.env['ir.config_parameter'].sudo()
        avisos = []
        for clave, defecto in SALDOS:
            codigo = (params.get_param(clave) or defecto).strip()
            producto = self.env['product.product'].search([('default_code', '=', codigo)], limit=1)
            if not producto:
                avisos.append('No existe el producto %s (parámetro %s): la lista en metros va sin ese saldo.'
                              % (codigo, clave))
                continue
            if producto in bom.byproduct_ids.product_id:
                continue
            bom.write({'byproduct_ids': [(0, 0, {'product_id': producto.id, 'product_qty': 0.0,
                                                 'product_uom_id': producto.uom_id.id, 'cost_share': 0.0})]})
        return avisos

    def _ft_generar_listas(self):
        """Genera o pone al día las listas archivadas de la tela; devuelve los avisos."""
        self.ensure_one()
        Revision = self.env['ficha.tecnica.revision']
        Bom = self.env['mrp.bom'].with_context(active_test=False)
        nombre = self.articulo
        if not self.ruta_familia_id:
            return ['%s: su crudo no tiene ruta de familia.' % nombre]
        avisos = []
        generadas = Bom
        kg, tela_m = self.product_proceso_id, self.product_acabado_id
        crudo = self.ruta_familia_id.crudo_id
        if not kg:
            return ['%s: la ficha no tiene el acabado en kg.' % nombre]
        bom_kg = Revision._ft_bom_de(kg)
        # 2.23.0: una lista activa sin operaciones es la anterior (no la del flujo): se deja como está y la del flujo se
        # crea archivada junto a ella.
        if bom_kg and ('no usar' in (bom_kg.code or '').lower() or (
                bom_kg.active and not bom_kg.with_context(active_test=False).operation_ids)):
            bom_kg = Bom
        if bom_kg.active:
            avisos.append('%s: la lista en kg está activa y no se toca (la cambia Ingeniería).' % nombre)
        else:
            if not bom_kg and not self.consumo_crudo:
                avisos.append('%s: falta el consumo de crudo para crear la lista en kg.' % nombre)
            else:
                if not bom_kg:
                    bom_kg = Bom.create({
                        'product_tmpl_id': kg.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': kg.uom_id.id,
                        'code': CODIGO_KG, 'active': False,
                        'bom_line_ids': [(0, 0, {'product_id': crudo.id, 'product_qty': self.consumo_crudo,
                                                 'product_uom_id': crudo.uom_id.id})]})
                elif self.consumo_crudo:
                    bom_kg.bom_line_ids.filtered(lambda line: line.product_id == crudo).product_qty = self.consumo_crudo
                avisos += self._ft_poner_operaciones(bom_kg, self._ft_pasos('kg'), kg)
                generadas |= bom_kg
        if tela_m:
            bom_m = Revision._ft_bom_de(tela_m)
            if bom_m and ('no usar' in (bom_m.code or '').lower() or (
                    bom_m.active and not bom_m.with_context(active_test=False).operation_ids)):
                bom_m = Bom
            if bom_m.active:
                avisos.append('%s: la lista en metros está activa y no se toca.' % nombre)
            elif not bom_m and not self.rendimiento_tela_acabada:
                avisos.append('%s: falta el rendimiento de tela acabada para crear la lista en metros.' % nombre)
            else:
                if not bom_m:
                    from .capacidad_conversion import primera_para_planear  # local: orden de carga de models/
                    kg_por_m = 1.0 / (self.rendimiento_tela_acabada * primera_para_planear(self.env, tela_m)[0])
                    bom_m = Bom.create({
                        'product_tmpl_id': tela_m.product_tmpl_id.id, 'product_qty': 1,
                        'product_uom_id': tela_m.uom_id.id, 'code': CODIGO_M, 'active': False,
                        'bom_line_ids': [(0, 0, {'product_id': kg.id, 'product_qty': kg_por_m,
                                                 'product_uom_id': kg.uom_id.id})]})
                avisos += self._ft_poner_operaciones(bom_m, self._ft_pasos('m'), tela_m)
                avisos += self._ft_poner_saldos(bom_m)
                generadas |= bom_m
        if generadas:
            self.env.invalidate_all()
            generadas.action_ft_datos_ficha()
        return avisos

    def action_ft_generar_listas(self):
        """«Generar listas desde la ruta»: arma o pone al día la lista en kg y la de metros de estas telas (solo las
        archivadas) y vuelve a revisarlas."""
        avisos = []
        for ficha in self:
            avisos += ficha._ft_generar_listas()
        self.env.invalidate_all()
        self.env['ficha.tecnica.revision']._ft_actualizar(self)
        mensaje = 'Listas generadas de %s tela(s).' % len(self)
        if avisos:
            mensaje += ' Avisos: ' + ' '.join(avisos[:15]) + (' (y %s más)' % (len(avisos) - 15) if len(avisos) > 15 else '')
        return {'type': 'ir.actions.client', 'tag': 'display_notification', 'params': {
            'title': 'Listas desde la ruta', 'message': mensaje, 'type': 'warning' if avisos else 'success',
            'sticky': bool(avisos), 'next': {'type': 'ir.actions.client', 'tag': 'reload'}}}

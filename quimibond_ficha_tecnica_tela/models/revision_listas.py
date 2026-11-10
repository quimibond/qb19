# -*- coding: utf-8 -*-
"""Revisión de las listas en kg contra la ficha (2.23.0, Dirección General 2026-11-03).

Lo que revisó la auditoría de las listas en kg (scripts de Dirección, octubre 2026), dentro de Odoo, para que cada área
vea sus pendientes sin esperar correos y para saber qué telas pueden entrar en cada ola:

- **Por tela** (una fila por ficha de acabado): su acabado en proceso (kg), su lista en kg y su lista en metros, cuántos
  pendientes tiene y de qué áreas; en verde si no tiene ninguno.
- **Por pendiente**: área responsable, qué es y el detalle, con la operación, la máquina y la lista.

Qué se revisa:

1. Que la ficha tenga su acabado en proceso (kg) y que existan la lista en kg y la lista en metros (también archivadas:
   las listas en kg nacen archivadas hasta que Ingeniería las revisa).
2. Que la lista en kg lleve el crudo de la ficha de tejido en que se basa la ficha de acabado.
3. En cada operación de la lista en kg: que su máquina tenga proceso del artículo, que el tiempo no esté en cero, que no
   falte un dato para calcular el tiempo y que el tiempo sea el de la ficha (2.22.0 y 2.23.0).
4. En las operaciones de tintorería: que el jet tenga la capacidad para el producto y que sea la de la ficha (2.23.0).
5. Que la lista en metros convierta kg → m con el rendimiento de tela acabada de la ficha (2.23.0).
6. Que las listas del flujo tengan su tipo de operación (Fabricación Tela en kg / en m) (2.25.0).
7. En la lista «Flujo en kg»: el desperdicio de Rama y, ya activa, la ruta MTO del acabado en kg (2.25.0).

El área sale del tipo de la máquina (tintorería, acabado = ramas, inspección, devanadora y abridora = Manufactura); lo
que es de la lista o de la ficha es de Ingeniería (Innovación). La revisión es una foto: se rehace cada noche y con
«Actualizar»; no cambia nada por sí misma. Para corregir, «Usar los datos de la ficha» en la tela o en las listas.
"""
from odoo import api, fields, models

AREAS = [
    ('ingenieria', 'Ingeniería (Innovación)'),
    ('tintoreria', 'Tintorería'),
    ('acabado', 'Acabado (ramas)'),
    ('inspeccion', 'Inspección'),
    ('manufactura', 'Manufactura (devanado y abridora)'),
]
AREA_POR_TIPO = {'tintoreria': 'tintoreria', 'acabado': 'acabado', 'inspeccion': 'inspeccion',
                 'devanadora': 'manufactura', 'abridora': 'manufactura'}
TIPOS = [
    ('sin_producto_kg', 'Ficha sin acabado en proceso (kg)'),
    ('lista_kg', 'Falta la lista en kg'),
    ('lista_m', 'Falta la lista en metros'),
    ('crudo', 'Crudo distinto de la ficha'),
    ('sin_proceso', 'Máquina sin proceso del artículo'),
    ('tiempo_cero', 'Tiempo en cero'),
    ('dato', 'Falta un dato para el tiempo'),
    ('tiempo_distinto', 'Tiempo distinto de la ficha'),
    ('capacidad_falta', 'Falta la capacidad del jet'),
    ('capacidad_distinta', 'Capacidad del jet distinta de la ficha'),
    ('conversion', 'Conversión kg → m'),
    ('datos_acabado', 'Ficha de acabado sin peso o rendimiento'),
    ('datos_tejido', 'Ficha de tejido sin rendimiento'),
    ('tipo_operacion', 'Lista sin tipo de operación'),
    ('desperdicio', 'Lista en kg sin desperdicio de Rama'),
    ('ruta_mto', 'Producto sin la ruta del flujo'),
    ('rendimiento_cambio', 'Lo fabricado da otro rendimiento'),
    ('rendimiento_fuera', 'Lo fabricado da un rendimiento fuera del teórico'),
]


class FichaTecnicaRevision(models.Model):
    _name = 'ficha.tecnica.revision'
    _description = 'Revisión de las listas de una tela contra la ficha'
    _order = 'listo, n_pendientes desc, articulo'
    _rec_name = 'articulo'

    acabado_id = fields.Many2one('ficha.tecnica.acabado', string='Ficha de acabado', required=True, index=True,
                                 ondelete='cascade',
                                 help='La ficha de acabado de la tela revisada (una fila por ficha). Lo llena la '
                                      'revisión de listas, cada noche o con «Actualizar la revisión».')
    articulo = fields.Char(string='Artículo',
                           help='Copia del artículo de la ficha de acabado al hacer la revisión; no se edita.')
    product_acabado_id = fields.Many2one('product.product', string='Tela acabada (m)',
                                         help='Copia de la tela acabada en metros de la ficha al hacer la revisión.')
    product_proceso_id = fields.Many2one('product.product', string='Acabado en proceso (kg)',
                                         help='Copia del acabado en proceso en kg (código con «I» al frente) de la '
                                              'ficha al hacer la revisión.')
    bom_kg_id = fields.Many2one('mrp.bom', string='Lista en kg',
                                help='La lista en kg del acabado en proceso que se revisó: la del flujo, aunque siga '
                                     'archivada esperando el visto bueno de Ingeniería.')
    bom_kg_activa = fields.Boolean(string='Lista en kg activa',
                                   help='La lista en kg revisada estaba activa (Odoo ya fabrica con ella). Si no, '
                                        'espera el visto bueno de Ingeniería.')
    bom_m_id = fields.Many2one('mrp.bom', string='Lista en metros',
                               help='La lista en metros de la tela acabada que se revisó (Clasificación final, '
                                    'conversión kg → m).')
    pendiente_ids = fields.One2many('ficha.tecnica.revision.pendiente', 'revision_id', string='Pendientes',
                                    help='Lo que no cuadra entre las listas y la ficha, uno por renglón con su área. '
                                         'Lo llena la revisión de listas; se corrige donde abre «Resolver».')
    n_pendientes = fields.Integer(string='Núm. de pendientes',
                                  help='Se calcula en la revisión: cuántos pendientes tiene la tela. Con alguno, la '
                                       'tela sale en rojo en Fichas técnicas → Artículos → Acabado.')
    listo = fields.Boolean(string='Lista',
                           help='Se calcula en la revisión: sin pendientes, las listas cuadran con la ficha (la tela '
                                'sale en verde en Artículos → Acabado).')
    areas = fields.Char(string='Áreas con pendientes',
                        help='Se calcula en la revisión: las áreas que tienen algo que corregir en esta tela.')
    pend_ingenieria = fields.Boolean(string='Pendiente de Ingeniería',
                                     help='Se calcula en la revisión: hay al menos un pendiente de Ingeniería '
                                          '(Innovación). Sirve para el filtro por área.')
    pend_tintoreria = fields.Boolean(string='Pendiente de Tintorería',
                                     help='Se calcula en la revisión: hay al menos un pendiente de Tintorería. Sirve '
                                          'para el filtro por área.')
    pend_acabado = fields.Boolean(string='Pendiente de Acabado',
                                  help='Se calcula en la revisión: hay al menos un pendiente de Acabado (ramas). Sirve '
                                       'para el filtro por área.')
    pend_inspeccion = fields.Boolean(string='Pendiente de Inspección',
                                     help='Se calcula en la revisión: hay al menos un pendiente de Inspección. Sirve '
                                          'para el filtro por área.')
    pend_manufactura = fields.Boolean(string='Pendiente de Manufactura',
                                      help='Se calcula en la revisión: hay al menos un pendiente de Manufactura '
                                           '(devanado y abridora). Sirve para el filtro por área.')
    fecha = fields.Datetime(string='Revisado el',
                            help='Fecha y hora de la última revisión de la tela (la de la noche o la del botón).')

    _acabado_uniq = models.Constraint('unique(acabado_id)', 'La tela ya tiene su revisión.')

    # ------------------------------------------------------------------
    # Revisión
    # ------------------------------------------------------------------
    @api.model
    def _ft_bom_de(self, product):
        """La lista de materiales normal del producto que corresponde al flujo en kg (también las archivadas).

        2.23.0: no «la activa primero». En la transición, 44 de las 111 telas tienen activa la lista vieja (un solo
        renglón con el teñido, sin operaciones) y archivada la nueva («Flujo en kg (borrador)», con operaciones) hasta
        que Ingeniería la revise. Orden: las que no dicen «no usar» en la referencia, las que tienen operaciones, la
        activa, la secuencia."""
        Bom = self.env['mrp.bom']
        if not product:
            return Bom
        boms = Bom.with_context(active_test=False).search([
            ('type', '=', 'normal'),
            '|', ('product_id', '=', product.id),
            '&', ('product_id', '=', False), ('product_tmpl_id', '=', product.product_tmpl_id.id)],
            order='sequence, id')
        if not boms:
            return Bom

        # 2.25.0: la lista del flujo (por su referencia) va antes que las otras: desde que la lista en metros no lleva
        # operación (Inspección como estación única), «tiene operaciones» ya no la distingue de la vieja.
        from .ruta_familia import CODIGO_KG, CODIGO_M  # local: ruta_familia se carga después

        def clave(bom):
            return ('no usar' in (bom.code or '').lower(), bom.code not in (CODIGO_KG, CODIGO_M),
                    not bom.operation_ids, not bom.active, bom.sequence, bom.id)
        return min(boms, key=clave)

    @api.model
    def _ft_pendientes_operacion(self, op, product):
        """Pendientes de una operación de la lista en kg: proceso, tiempo y (tintorería) capacidad del jet."""
        wc = op.workcenter_id
        tipo = wc.ft_tipo_id.code
        area = AREA_POR_TIPO.get(tipo, 'ingenieria')
        base = {'area': area, 'operation_id': op.id, 'workcenter_id': wc.id, 'bom_id': op.bom_id.id}
        nombre = '%s (%s)' % (op.name, wc.name)
        res = []
        if op.ft_aviso:
            res.append(dict(base, tipo='sin_proceso', detalle=op.ft_aviso))
        tiempo_lista = op.time_cycle_manual if op.time_mode == 'manual' else op.time_cycle
        if not tiempo_lista:
            res.append(dict(base, tipo='tiempo_cero', detalle='%s: el tiempo de la lista está en cero%s.' % (
                nombre, ('; la ficha da %.4f min' % op.ft_tiempo_ficha) if op.ft_tiempo_ficha else '')))
        elif op.ft_tiempo_difiere:
            res.append(dict(base, tipo='tiempo_distinto', detalle='%s: la lista tiene %.4f min y la ficha %.4f min.' % (
                nombre, tiempo_lista, op.ft_tiempo_ficha)))
        capacidad_jet = tipo == 'tintoreria' and product
        if not op.ft_tiempo_ficha and op.ft_tiempo_calculo and not op.ft_aviso and not (
                capacidad_jet and 'falta la capacidad' in op.ft_tiempo_calculo):
            res.append(dict(base, tipo='dato', detalle='%s: %s' % (nombre, op.ft_tiempo_calculo)))
        if capacidad_jet:
            line = wc.capacity_ids.filtered(lambda c: c.product_id == product)[:1]
            if not line:
                capacidad, motivo = wc._ft_capacidad_jet(product)
                res.append(dict(base, tipo='capacidad_falta', area='tintoreria', detalle='%s: %s' % (
                    wc.name, ('falta la capacidad para el producto; la ficha da %s kg (%s).' % (capacidad, motivo))
                    if capacidad else ('falta la capacidad para el producto y la ficha no la da: %s.' % motivo))))
            elif line.ft_capacidad_difiere:
                res.append(dict(base, tipo='capacidad_distinta', area='tintoreria', detalle='%s: %s kg capturados y la '
                                'ficha da %s kg (%s).' % (wc.name, line.capacity, line.ft_capacidad_ficha,
                                                          line.ft_capacidad_motivo)))
        return res

    @api.model
    def _ft_revisar(self, acabado):
        """(valores de la fila, lista de pendientes) de una ficha de acabado."""
        Tejido = self.env['ficha.tecnica.tejido']
        proceso, tela_m = acabado.product_proceso_id, acabado.product_acabado_id
        bom_kg, bom_m = self._ft_bom_de(proceso), self._ft_bom_de(tela_m)
        vals = {'acabado_id': acabado.id, 'articulo': acabado.articulo, 'product_acabado_id': tela_m.id,
                'product_proceso_id': proceso.id, 'bom_kg_id': bom_kg.id, 'bom_kg_activa': bom_kg.active,
                'bom_m_id': bom_m.id, 'fecha': fields.Datetime.now()}
        pendientes = []
        ing = {'area': 'ingenieria'}
        # 2.23.0: los datos de la ficha que usan los cálculos, avisados en su origen (antes solo salían como «falta un
        # dato» en cada operación, a nombre del área de la máquina).
        faltan = [nombre for nombre, valor in (('peso', acabado.peso_acabado),
                                                ('rendimiento de tela acabada', acabado.rendimiento_tela_acabada))
                  if not valor]
        if faltan:
            pendientes.append(dict(ing, tipo='datos_acabado', detalle='La ficha de acabado no tiene %s: sin eso no '
                                   'salen el tiempo de rama, el de inspección ni la conversión a metros.' % (
                                       ' ni '.join(faltan))))
        if acabado.rendimiento_por_revisar and acabado.rendimiento_medido:
            # 2.28.0: lo medido se aleja de la ficha más que la alerta. La ficha no cambia sola: Ingeniería lo usa
            # («Usar el medido») o se queda con el de la ficha («Marcar revisado»).
            fuentes = dict(acabado._fields['rendimiento_medido_fuente']._description_selection(acabado.env))
            ficha_r = acabado.rendimiento_tela_acabada
            pendientes.append(dict(ing, tipo='rendimiento_cambio', detalle='Lo fabricado da %.4f m/kg (%s, %s muestras) '
                                   'contra %.4f de la ficha (%+.1f %%). En la ficha: «Usar el medido» o «Marcar '
                                   'revisado».' % (
                                       acabado.rendimiento_medido,
                                       (fuentes.get(acabado.rendimiento_medido_fuente) or '').lower(),
                                       acabado.rendimiento_medido_ordenes, ficha_r,
                                       (acabado.rendimiento_medido / ficha_r - 1) * 100.0 if ficha_r else 0.0)))
        if acabado.rendimiento_medido_estado == 'fuera':
            pendientes.append(dict(ing, tipo='rendimiento_fuera', detalle='Lo fabricado da %.4f m/kg y el teórico (peso '
                                   'de la ficha y ancho de %s) es %.4f: se aleja demasiado para proponerlo. Revisa el '
                                   'peso, el ancho o las órdenes de la tela.' % (
                                       acabado.rendimiento_medido, acabado.rendimiento_teorico_ancho or 'la ficha',
                                       acabado.rendimiento_teorico)))
        tejido = acabado.tejido_id
        if tejido and not tejido.rendimiento_tela_tejida:
            pendientes.append(dict(ing, tipo='datos_tejido', detalle='La ficha de tejido %s no tiene rendimiento de '
                                   'tejido: sin él no salen los tiempos de devanado y abridora ni la banda del jet.' % (
                                       tejido.display_name)))
        if not proceso:
            pendientes.append(dict(ing, tipo='sin_producto_kg', detalle='La ficha %s no tiene el acabado en proceso '
                                   '(kg).' % acabado.display_name))
        elif not bom_kg:
            pendientes.append(dict(ing, tipo='lista_kg', detalle='%s no tiene lista de materiales.' % (
                proceso.default_code or proceso.name)))
        elif not bom_kg.with_context(active_test=False).operation_ids:
            # 2.23.0: la única lista es la anterior (un renglón, sin operaciones: la «Conversión de unidades» que
            # consume la tela en metros); falta la del flujo en kg. Antes salía como «crudo distinto».
            # 2.23.0: también una «Flujo en kg» sin ruta (8 activas en producción, 2026-11-03).
            pendientes.append(dict(ing, tipo='lista_kg', bom_id=bom_kg.id, detalle='La lista en kg de %s no tiene '
                                   'operaciones: es la lista anterior o le falta la ruta (Devanado → Tintorería → '
                                   'Abridora → Rama → Inspección).' % (proceso.default_code or proceso.name)))
        else:
            esperado = acabado.tejido_id.product_proceso_id
            crudo = next((line.product_id for line in bom_kg.bom_line_ids
                          if Tejido._ft_de_producto(line.product_id)), self.env['product.product'])
            if not crudo:
                pendientes.append(dict(ing, tipo='crudo', bom_id=bom_kg.id,
                                       detalle='La lista en kg no lleva un crudo con ficha de tejido.'))
            elif esperado and crudo != esperado:
                pendientes.append(dict(ing, tipo='crudo', bom_id=bom_kg.id, detalle='La lista en kg lleva %s y la '
                                       'ficha de acabado se basa en el tejido de %s.' % (
                                           crudo.default_code or crudo.name, esperado.default_code or esperado.name)))
            for op in bom_kg.with_context(active_test=False).operation_ids:
                pendientes += self._ft_pendientes_operacion(op, proceso)
        if tela_m and not bom_m:
            pendientes.append(dict(ing, tipo='lista_m', detalle='%s no tiene lista de materiales.' % (
                tela_m.default_code or tela_m.name)))
        elif bom_m and (bom_m.ft_conversion_difiere or (not bom_m.ft_conversion_kg and bom_m.ft_conversion_calculo)):
            detalle = bom_m.ft_conversion_calculo
            if bom_m.ft_conversion_difiere:
                detalle = 'La lista en metros no convierte con la ficha: %s.' % detalle
            pendientes.append(dict(ing, tipo='conversion', bom_id=bom_m.id, detalle=detalle))
        # 2.25.0: sin tipo de operación la orden nace como «Re-proceso Tintorería» (piloto de José, 2026-11-04).
        from .ruta_familia import CODIGO_KG as _KG, CODIGO_M as _M  # local: ruta_familia se carga después
        for bom, cual in ((bom_kg, 'kg'), (bom_m, 'metros')):
            del_flujo = bom.code in (_KG, _M) or bom.with_context(active_test=False).operation_ids
            if bom and del_flujo and not bom.picking_type_id:
                pendientes.append(dict(ing, tipo='tipo_operacion', bom_id=bom.id, detalle='La lista en %s no tiene '
                                       'tipo de operación: la orden tomaría el predeterminado del almacén. Va «%s».' % (
                                           cual, 'Fabricación Tela en kg' if cual == 'kg' else 'Fabricación Tela en m')))
        # 2.25.0 (piloto 2026-11-04): en la lista «Flujo en kg», el desperdicio que pesa Rama y, ya activa, la ruta MTO
        # del «I…» (sin ella la orden en metros no pide la orden en kg).
        from .ruta_familia import CODIGO_KG, DESPERDICIO_CODIGO  # local: ruta_familia se carga después
        if bom_kg and bom_kg.code == CODIGO_KG:
            Ruta = self.env['ficha.tecnica.ruta']
            con_rama = bom_kg.with_context(active_test=False).operation_ids.filtered(
                lambda o: Ruta._ft_paso_de_operacion(o) == 'rama')
            if con_rama and not bom_kg.byproduct_ids.filtered(lambda b: b.product_id.default_code == DESPERDICIO_CODIGO):
                pendientes.append(dict(ing, tipo='desperdicio', bom_id=bom_kg.id, detalle='La lista en kg no tiene el '
                                       'subproducto «%s» en la Rama: Rama no puede pesar el desperdicio.' % (
                                           DESPERDICIO_CODIGO)))
            ruta = self.env['mrp.bom']._ft_ruta_mto_kg(bom_kg.company_id or None)
            if bom_kg.active and ruta and ruta not in bom_kg.product_tmpl_id.route_ids:
                pendientes.append(dict(ing, tipo='ruta_mto', bom_id=bom_kg.id, detalle='%s no tiene la ruta «%s»: la '
                                       'orden en metros no pide la orden en kg.' % (
                                           proceso.default_code or proceso.name, ruta.name)))
        # 2.25.0: la lista en metros activa del flujo necesita la ruta «Fabricar Tela m» en la tela.
        from .ruta_familia import CODIGO_M as _CM  # local: ruta_familia se carga después
        if bom_m and bom_m.active and bom_m.code == _CM:
            ruta_m = self.env['mrp.bom']._ft_ruta_flujo('m', bom_m.company_id or None)
            if ruta_m and ruta_m not in bom_m.product_tmpl_id.route_ids:
                pendientes.append(dict(ing, tipo='ruta_mto', bom_id=bom_m.id, detalle='%s no tiene la ruta «%s»: el plan '
                                       'maestro no puede pedir la orden en metros.' % (
                                           tela_m.default_code or tela_m.name, ruta_m.name)))
        areas = {p['area'] for p in pendientes}
        vals.update({
            'n_pendientes': len(pendientes), 'listo': not pendientes,
            'areas': ', '.join(nombre for code, nombre in AREAS if code in areas),
            **{'pend_%s' % code: code in areas for code, _nombre in AREAS}})
        return vals, pendientes

    @api.model
    def _ft_actualizar(self, acabados=None):
        """Rehace la revisión de esas fichas de acabado (todas las activas si no se dan)."""
        Acabado = self.env['ficha.tecnica.acabado']
        acabados = acabados if acabados is not None else Acabado.search([('product_acabado_id', '!=', False)])
        self.sudo().search([('acabado_id', 'in', acabados.ids)]).unlink()
        if not acabados:
            return self.browse()
        filas = []
        for acabado in acabados:
            vals, pendientes = self._ft_revisar(acabado)
            vals['pendiente_ids'] = [(0, 0, p) for p in pendientes]
            filas.append(vals)
        return self.sudo().create(filas)

    @api.model
    def _cron_ft_actualizar(self):
        # Fichas que ya no existen o se archivaron: su fila se va (cascade) o se limpia aquí.
        self.sudo().search([('acabado_id.active', '=', False)]).unlink()
        self._ft_actualizar()

    def action_ft_actualizar(self):
        """Botón «Actualizar» de la lista: rehace la revisión de todas las telas. 2.23.0: método de registros (sin
        ``@api.model``): el botón de la cabecera lo llama con los renglones seleccionados (o ninguno) y un
        ``@api.model`` recibía ese argumento de más («takes 1 positional argument but 2 were given»)."""
        self.env['ficha.tecnica.revision']._cron_ft_actualizar()
        return {'type': 'ir.actions.client', 'tag': 'reload'}

    def action_ft_datos_ficha(self):
        """«Usar los datos de la ficha» en las listas de estas telas y vuelve a revisarlas."""
        acabados = self.acabado_id
        (self.bom_kg_id | self.bom_m_id).action_ft_datos_ficha()
        self.env.invalidate_all()  # tiempos, capacidades y avisos son calculados sin guardar: se leen de nuevo
        self._ft_actualizar(acabados)
        return True

    def action_ft_revisar(self):
        """Vuelve a revisar solo estas telas."""
        self._ft_actualizar(self.acabado_id)
        return True


class FichaTecnicaRevisionPendiente(models.Model):
    _name = 'ficha.tecnica.revision.pendiente'
    _description = 'Pendiente de la revisión de listas'
    _order = 'area, articulo, tipo, id'

    revision_id = fields.Many2one('ficha.tecnica.revision', string='Tela', required=True, index=True,
                                  ondelete='cascade',
                                  help='La fila de la tela en la revisión de listas. Los pendientes los crea la '
                                       'revisión (cada noche o con «Actualizar la revisión») y se rehacen cada vez.')
    articulo = fields.Char(related='revision_id.articulo', string='Artículo', store=True,
                           help='El artículo de la tela, tomado de su revisión.')
    area = fields.Selection(AREAS, string='Área', required=True, index=True,
                            help='Quién lo corrige: sale del tipo de la máquina de la operación (Tintorería, Acabado '
                                 '= ramas, Inspección, Manufactura = devanado y abridora); lo de la lista o de la '
                                 'ficha es de Ingeniería.')
    tipo = fields.Selection(TIPOS, string='Pendiente', required=True,
                            help='Qué no cuadra (falta una lista, tiempo distinto de la ficha, capacidad del jet…). '
                                 '«Resolver» abre la pantalla donde se corrige.')
    detalle = fields.Char(string='Detalle',
                          help='La explicación con los números de la lista y de la ficha, o el dato que falta.')
    operation_id = fields.Many2one('mrp.routing.workcenter', string='Operación', ondelete='set null',
                                   help='La operación de la lista en kg a la que se refiere, si es de una operación.')
    workcenter_id = fields.Many2one('mrp.workcenter', string='Máquina', ondelete='set null',
                                    help='La máquina (centro de trabajo) de esa operación.')
    bom_id = fields.Many2one('mrp.bom', string='Lista de materiales', ondelete='set null',
                             help='La lista de materiales donde está lo que no cuadra.')

    # 2.24.0 (Dirección General 2026-11-03): cada pendiente abre el lugar donde se corrige, para que nadie tenga que
    # saber en qué menú se captura cada dato.
    def _ft_abrir(self, record, name=None, target='current'):
        return {'type': 'ir.actions.act_window', 'name': name or record.display_name, 'res_model': record._name,
                'view_mode': 'form', 'res_id': record.id, 'target': target}

    def action_ft_resolver(self):
        """«Resolver»: abre el formulario donde se corrige el pendiente."""
        self.ensure_one()
        op, acabado = self.operation_id, self.revision_id.acabado_id
        if self.tipo == 'sin_proceso' and op:
            return op.action_ft_nuevo_proceso()  # los datos de la tela en esa máquina (los crea si no hay)
        if self.tipo == 'dato' and op.ft_proceso_id:
            return op.ft_proceso_id.action_ft_editar()
        if self.tipo in ('tiempo_cero', 'tiempo_distinto', 'dato') and op:
            return self._ft_abrir(op, 'Operación: %s' % op.name)  # «Usar el tiempo de la ficha» está ahí
        if self.tipo in ('capacidad_falta', 'capacidad_distinta') and self.workcenter_id:
            return self._ft_abrir(self.workcenter_id)  # pestaña Capacidades, «Usar la capacidad de la ficha»
        if self.tipo == 'datos_tejido' and acabado.tejido_id:
            return self._ft_abrir(acabado.tejido_id, 'Ficha de tejido')
        if self.tipo == 'ruta_mto' and self.bom_id:
            return self._ft_abrir(self.bom_id.product_tmpl_id, 'Acabado en kg')  # pestaña Inventario → Rutas
        if self.tipo in ('crudo', 'conversion', 'tipo_operacion', 'desperdicio') and self.bom_id:
            return self._ft_abrir(self.bom_id)
        # Sin acabado en kg, sin listas, ficha sin peso o rendimiento: la ficha de acabado («Generar listas» vive ahí).
        return self._ft_abrir(acabado, 'Ficha de acabado')

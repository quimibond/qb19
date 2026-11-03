# -*- coding: utf-8 -*-
"""Revisión de las listas en kg contra la ficha (2.24.0, Dirección General 2026-11-03).

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
]


class FichaTecnicaRevision(models.Model):
    _name = 'ficha.tecnica.revision'
    _description = 'Revisión de las listas de una tela contra la ficha'
    _order = 'listo, n_pendientes desc, articulo'
    _rec_name = 'articulo'

    acabado_id = fields.Many2one('ficha.tecnica.acabado', string='Ficha de acabado', required=True, index=True,
                                 ondelete='cascade')
    articulo = fields.Char(string='Artículo')
    product_acabado_id = fields.Many2one('product.product', string='Tela acabada (m)')
    product_proceso_id = fields.Many2one('product.product', string='Acabado en proceso (kg)')
    bom_kg_id = fields.Many2one('mrp.bom', string='Lista en kg')
    bom_kg_activa = fields.Boolean(string='Lista en kg activa')
    bom_m_id = fields.Many2one('mrp.bom', string='Lista en metros')
    pendiente_ids = fields.One2many('ficha.tecnica.revision.pendiente', 'revision_id', string='Pendientes')
    n_pendientes = fields.Integer(string='Núm. de pendientes')
    listo = fields.Boolean(string='Lista', help='Sin pendientes: las listas cuadran con la ficha.')
    areas = fields.Char(string='Áreas con pendientes')
    pend_ingenieria = fields.Boolean(string='Pendiente de Ingeniería')
    pend_tintoreria = fields.Boolean(string='Pendiente de Tintorería')
    pend_acabado = fields.Boolean(string='Pendiente de Acabado')
    pend_inspeccion = fields.Boolean(string='Pendiente de Inspección')
    pend_manufactura = fields.Boolean(string='Pendiente de Manufactura')
    fecha = fields.Datetime(string='Revisado el')

    _acabado_uniq = models.Constraint('unique(acabado_id)', 'La tela ya tiene su revisión.')

    # ------------------------------------------------------------------
    # Revisión
    # ------------------------------------------------------------------
    @api.model
    def _ft_bom_de(self, product):
        """La lista de materiales normal del producto que corresponde al flujo en kg (también las archivadas).

        2.25.1: no «la activa primero». En la transición, 44 de las 111 telas tienen activa la lista vieja (un solo
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

        def clave(bom):
            return ('no usar' in (bom.code or '').lower(), not bom.operation_ids, not bom.active, bom.sequence, bom.id)
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
        if not proceso:
            pendientes.append(dict(ing, tipo='sin_producto_kg', detalle='La ficha %s no tiene el acabado en proceso '
                                   '(kg).' % acabado.display_name))
        elif not bom_kg:
            pendientes.append(dict(ing, tipo='lista_kg', detalle='%s no tiene lista de materiales.' % (
                proceso.default_code or proceso.name)))
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

    @api.model
    def action_ft_actualizar(self):
        """Botón «Actualizar» de la lista: rehace la revisión de todas las telas."""
        self._cron_ft_actualizar()
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
                                  ondelete='cascade')
    articulo = fields.Char(related='revision_id.articulo', string='Artículo', store=True)
    area = fields.Selection(AREAS, string='Área', required=True, index=True)
    tipo = fields.Selection(TIPOS, string='Pendiente', required=True)
    detalle = fields.Char(string='Detalle')
    operation_id = fields.Many2one('mrp.routing.workcenter', string='Operación', ondelete='set null')
    workcenter_id = fields.Many2one('mrp.workcenter', string='Máquina', ondelete='set null')
    bom_id = fields.Many2one('mrp.bom', string='Lista de materiales', ondelete='set null')

# -*- coding: utf-8 -*-
"""Máquinas alternativas: lo nativo de Odoo y la ficha, juntos (2.20.0, Dirección General 2026-10-27).

Odoo planea una orden de trabajo en la máquina de la operación o en una de sus **alternativas**
(``alternative_workcenter_ids``): busca el primer hueco en cada una y se queda con la que termina antes. Esas
alternativas son de la máquina, iguales para cualquier artículo. La ficha sabe otra cosa: en qué máquinas corre
cada artículo (el **proceso del artículo en la máquina**). Antes de 2.20.0 las dos listas no se hablaban: de 54
operaciones de rama de las listas en kg, solo en una coincidían las alternativas de Odoo con los procesos.

Cada lado responde una pregunta:

- **Alternativas de Odoo**: qué máquinas son equivalentes y están operando. Las arma la ficha: en un tipo con
  «Alternativas entre sí», las alternativas de cada máquina son las demás de su tipo que estén **operando**
  (estado de la máquina: operando, en montaje, en pruebas o fuera de servicio). Se recalculan solas al cambiar el
  estado, el tipo o al archivar una máquina; el campo nativo se ve en el centro de trabajo, pero no se edita a
  mano. Un tipo sin la marca deja las alternativas como estén.
- **Procesos de la ficha**: en cuáles de esas máquinas corre este artículo.

Al planear, Odoo solo considera las máquinas que cumplen las dos: operando y, si el tipo pide datos del artículo,
con proceso del artículo. A las demás el módulo les contesta «sin hueco» y Odoo las salta (no se reescribe el
planificador). Si ninguna cumple, se planea como siempre y la orden de trabajo lo avisa: faltar un dato no impide
planear. Al cambiar la máquina a mano, el mismo aviso.

2.21.0: una máquina fijada a mano no se mueve. Con la clave de contexto ``ft_maquina_fija`` (o cuando otro módulo lo
dice en ``mrp.workorder._ft_maquina_fija``, como la Programación de Tintorería con el jet ya programado) solo cuenta la
máquina de la orden de trabajo, opere o no y tenga o no proceso del artículo; queda solo el aviso.

La ficha de acabado muestra en «Ruta y máquinas», por operación, las máquinas posibles: ✓ corre ahí, ✗ Odoo la
usaría pero no hay proceso, ⚠ la máquina no está operando, ○ el artículo corre en otra máquina del tipo que no es
alternativa.
"""
from odoo import Command, api, fields, models

ESTADOS_MAQUINA = [('operando', 'Operando'), ('montaje', 'En montaje'), ('pruebas', 'En pruebas'),
                   ('fuera', 'Fuera de servicio')]
# Claves de ``mrp.workcenter`` que cambian las alternativas de su tipo.
CAMPOS_ALTERNATIVAS = {'ft_tipo_id', 'ft_estado', 'active', 'company_id'}


class FichaTecnicaMaquinaTipoAlternativas(models.Model):
    _inherit = 'ficha.tecnica.maquina.tipo'

    alternativas_por_tipo = fields.Boolean(
        string='Alternativas entre sí',
        help='Las máquinas de este tipo son alternativas unas de otras: las alternativas de cada máquina (las que '
             'Odoo prueba al planear) son las demás de su tipo que estén operando, y se recalculan solas. Al '
             'planear, de esas solo cuentan las que tienen proceso del artículo.')

    def _ft_sync_alternativas(self):
        """Alternativas nativas de cada máquina de los tipos con «Alternativas entre sí»: las demás de su tipo
        (activas, de la misma compañía o sin compañía) que estén operando."""
        Workcenter = self.env['mrp.workcenter']
        for tipo in self.filtered(lambda t: t.alternativas_por_tipo):
            maquinas = Workcenter.search([('ft_tipo_id', '=', tipo.id)])
            operando = maquinas.filtered(lambda w: w.ft_estado == 'operando')
            for wc in maquinas:
                alternativas = operando.filtered(
                    lambda w: w != wc and (not w.company_id or not wc.company_id or w.company_id == wc.company_id))
                if wc.alternative_workcenter_ids != alternativas:
                    wc.with_context(ft_sync_alternativas=True).write(
                        {'alternative_workcenter_ids': [Command.set(alternativas.ids)]})

    @api.model_create_multi
    def create(self, vals_list):
        tipos = super().create(vals_list)
        tipos._ft_sync_alternativas()
        return tipos

    def write(self, vals):
        res = super().write(vals)
        if vals.get('alternativas_por_tipo'):
            self._ft_sync_alternativas()
        return res


class MrpWorkcenterAlternativas(models.Model):
    _inherit = 'mrp.workcenter'

    ft_estado = fields.Selection(
        ESTADOS_MAQUINA, string='Estado de la máquina', required=True, default='operando',
        help='Solo una máquina operando es alternativa de las demás de su tipo y se usa al planear. En montaje, en '
             'pruebas o fuera de servicio: Odoo no le asigna órdenes de trabajo por su cuenta.')
    ft_alternativas_auto = fields.Boolean(
        related='ft_tipo_id.alternativas_por_tipo', string='Alternativas por tipo',
        help='Las alternativas de esta máquina las arma su tipo de máquina («Alternativas entre sí»): las demás de '
             'su tipo que estén operando.')

    @api.model_create_multi
    def create(self, vals_list):
        workcenters = super().create(vals_list)
        workcenters.ft_tipo_id._ft_sync_alternativas()
        return workcenters

    def write(self, vals):
        if self.env.context.get('ft_sync_alternativas') or not CAMPOS_ALTERNATIVAS & set(vals):
            return super().write(vals)
        tipos = self.ft_tipo_id
        res = super().write(vals)
        (tipos | self.ft_tipo_id)._ft_sync_alternativas()
        return res

    def _ft_pide_articulo(self):
        """El tipo de la máquina tiene parámetros del artículo (rama, jet, inspección…): ahí el proceso dice si el
        artículo corre en la máquina."""
        self.ensure_one()
        return bool(self.ft_tipo_id and self.ft_tipo_id._ft_parametros('articulo'))

    def _ft_alternativas_para(self, product):
        """(máquinas en que puede correr ``product`` entre esta y sus alternativas, renglones (estado, máquina)).

        Estados: ``ok`` (operando y, si el tipo pide datos del artículo, con proceso), ``sin_proceso``,
        ``no_opera`` (en montaje, en pruebas o fuera de servicio) y ``fuera`` (el artículo tiene proceso en otra
        máquina del tipo que no es alternativa)."""
        self.ensure_one()
        Proceso = self.env['ficha.tecnica.proceso']
        candidatas = self | self.alternative_workcenter_ids
        pide = self._ft_pide_articulo() and product
        permitidas, renglones = self.browse(), []
        for wc in candidatas:
            if wc.ft_estado != 'operando':
                renglones.append(('no_opera', wc))
            elif pide and not Proceso._ft_buscar(product, wc):
                renglones.append(('sin_proceso', wc))
            else:
                permitidas |= wc
                renglones.append(('ok', wc))
        if pide and product.default_code:
            otras = Proceso.search([('articulo', 'in', Proceso._ft_codigos(product.default_code)),
                                    ('workcenter_id.ft_tipo_id', '=', self.ft_tipo_id.id),
                                    ('workcenter_id', 'not in', candidatas.ids)])
            renglones += [('fuera', wc) for wc in otras.workcenter_id.sorted('name')]
        return permitidas, renglones

    def _ft_texto_alternativas(self, product):
        """Las máquinas posibles para ``product`` en una línea (columna «Máquinas posibles»)."""
        self.ensure_one()
        estados = dict(ESTADOS_MAQUINA)
        partes = []
        for estado, wc in self._ft_alternativas_para(product)[1]:
            if estado == 'ok':
                partes.append('✓ %s' % wc.name)
            elif estado == 'sin_proceso':
                partes.append('✗ %s (sin proceso del artículo)' % wc.name)
            elif estado == 'no_opera':
                partes.append('⚠ %s (%s)' % (wc.name, estados[wc.ft_estado].lower()))
            elif wc.ft_estado != 'operando':
                partes.append('○ %s (corre ahí; %s)' % (wc.name, estados[wc.ft_estado].lower()))
            else:
                partes.append('○ %s (corre ahí y no es alternativa)' % wc.name)
        return ' · '.join(partes)

    def _ft_aviso_alternativa(self, product):
        """Aviso de la orden de trabajo en esta máquina: no está operando, o el artículo no tiene proceso en ella.
        False si no hay nada que avisar."""
        self.ensure_one()
        if self.ft_estado != 'operando':
            return '%s está %s.' % (self.name, dict(ESTADOS_MAQUINA)[self.ft_estado].lower())
        if product and self._ft_pide_articulo() and \
                not self.env['ficha.tecnica.proceso']._ft_buscar(product, self):
            return '%s no tiene proceso del artículo %s (Fichas técnicas → Máquinas → Procesos por artículo).' % (
                self.name, product.default_code or product.display_name)
        return False

    def _get_first_available_slot(self, start_datetime, duration, *args, **kwargs):
        # 2.20.0: al planear una orden de trabajo, una máquina en que el artículo no puede correr no tiene hueco
        # (Odoo la salta y prueba la siguiente alternativa). Ver ``mrp.workorder._plan_workorder``.
        permitidas = self.env.context.get('ft_wc_permitidas')
        if permitidas and self.id not in permitidas:
            return False, '%s no está operando o el artículo no tiene proceso en ella.' % self.name
        return super()._get_first_available_slot(start_datetime, duration, *args, **kwargs)


class MrpRoutingWorkcenterAlternativas(models.Model):
    _inherit = 'mrp.routing.workcenter'

    ft_maquinas_posibles = fields.Char(
        string='Máquinas posibles', compute='_compute_ft_maquinas_posibles',
        help='La máquina de la operación y sus alternativas, como las ve el planificador: ✓ el artículo corre ahí, '
             '✗ Odoo la usaría pero no hay proceso del artículo, ⚠ la máquina no está operando, ○ el artículo '
             'corre en otra máquina del tipo que no es alternativa.')

    @api.depends('workcenter_id', 'workcenter_id.alternative_workcenter_ids', 'workcenter_id.ft_estado',
                 'bom_id.product_id', 'bom_id.product_tmpl_id')
    def _compute_ft_maquinas_posibles(self):
        for op in self:
            product = op.bom_id.product_id or op.bom_id.product_tmpl_id.product_variant_ids[:1]
            op.ft_maquinas_posibles = op.workcenter_id._ft_texto_alternativas(product) if op.workcenter_id else False


class MrpWorkorderAlternativas(models.Model):
    _inherit = 'mrp.workorder'

    def _ft_maquina_fija(self):
        """2.21.0: la máquina de esta orden de trabajo se fijó a mano y el planificador no la cambia. Aquí, con
        la clave de contexto ``ft_maquina_fija`` (vale para todas las órdenes de trabajo que se planean en esa
        llamada); otros módulos agregan sus casos (la Programación de Tintorería, el jet ya programado)."""
        self.ensure_one()
        return bool(self.env.context.get('ft_maquina_fija'))

    def _plan_workorder(self, replan=False):
        # 2.20.0: de la máquina de la operación y sus alternativas, solo cuentan las que están operando y (si el
        # tipo pide datos del artículo) tienen proceso del artículo. Si ninguna cumple, se planea como siempre.
        # 2.21.0: si la máquina se fijó a mano (``_ft_maquina_fija``), solo cuenta ella, opere o no y tenga o no
        # proceso del artículo: el planificador no la mueve a una alternativa y queda solo el aviso.
        # La clave de contexto se pone siempre (o se limpia): Odoo planea aquí mismo las órdenes de trabajo
        # anteriores, y cada una lleva su propia lista.
        self.ensure_one()
        permitidas = False
        if self.workcenter_id and self._ft_maquina_fija():
            permitidas = (self.workcenter_id.id,)
        elif self.workcenter_id:
            validas = self.workcenter_id._ft_alternativas_para(self.product_id)[0]
            if validas and validas != self.workcenter_id | self.workcenter_id.alternative_workcenter_ids:
                permitidas = tuple(validas.ids)
        return super(MrpWorkorderAlternativas, self.with_context(ft_wc_permitidas=permitidas))._plan_workorder(
            replan=replan)

    def _ft_aviso(self):
        aviso = super()._ft_aviso()
        extra = self.workcenter_id._ft_aviso_alternativa(self.product_id) if self.workcenter_id else False
        return ' '.join(a for a in (aviso, extra) if a) or False

# -*- coding: utf-8 -*-
"""57.141.0 (Dirección General 2026-10-09): «Generar artículos» parte del **artículo base**.

Antes armaba crudo, teñido y acabado con las claves del producto terminado, y eso está mal:
peso y ancho cambian en el proceso (el acabado WJ053Q22JNT160 consume el teñido WJ044Q22INT235,
que consume el crudo WJ044Q22HNT235: 44 g/m² a 235 cm, no 53 g a 160 cm). Con artículo base:

1. Se recorre la cadena del base por sus listas de materiales: acabado → teñido → crudo.
2. En cada nivel se decide si cambia con las claves del proyecto (``KEY_LEVELS``): el color
   cambia teñido y acabado, el crudo no; el ancho acabado solo el acabado; galga, hilo,
   composición, dibujo y peso, todos.
3. Nivel que no cambia: se liga el artículo del base. Nivel que cambia: se crea el artículo con
   el código del nivel del base sustituyendo solo las claves que cambiaron, se copia su lista
   de materiales (cantidades, operaciones, subproductos) y se reemplaza el insumo del nivel
   inferior por el artículo nuevo de ese nivel.
4. Los componentes que dependen de la clave que cambió (la fórmula de color en el teñido, el
   hilo en el crudo) se copian marcados **«Por capturar»** (``mrp.bom.line.sgi_dev_pending``);
   el proyecto los cuenta y la cotización no se manda a aprobar mientras haya pendientes.
5. Todo queda en el chatter del proyecto. Sin artículo base, el botón sigue como antes.

57.192.0 (Dirección General 2026-10-26, proyecto 504): el crudo sale de los **códigos de crudo que ya existen**, no
se inventa. Al regenerar con el base WJ060Q21JNT175 y peso 53, Odoo creó el crudo WJ042Q21HNT112 (peso estimado en
proporción) cuando el acabado WJ053Q21JNT178 ya existía y consume el teñido y el crudo WJ047Q21…112. Ahora:

- El **peso** del acabado ya no baja a crudo ni teñido: el peso final lo da el acabado (rama, ancho); el tejido es
  el mismo. Crudo y teñido cambian solo con composición, dibujo, hilo, galga, ancho crudo (y el teñido con el color).
- Si el **acabado que pide el proyecto ya existe** con lista de materiales, se ligan el teñido y el crudo que él
  consume.
- **Crudo de tejido** (``sgi_dev_tejido_id``, opcional): Diseño elige un crudo existente y se usa ese; el teñido se
  arma con su código.
- Si el crudo tiene que cambiar y su código no existe, se busca un crudo existente con las mismas claves y otro
  peso (el de peso más cercano al del base) antes de crear uno.
- Los códigos propuestos de la pestaña Cotización salen del mismo plan que «Generar artículos».
"""
from markupsafe import Markup

from odoo import api, fields, models
from odoo.exceptions import UserError

from .sgi_dev_product import COLORED_YARN_CODES, NATURAL_COLOR

# Posiciones del código (1 composición, 2 dibujo, 3-5 peso, 6 hilo, 7-8 galga, 9 operación,
# 10-11 color, 12-14 ancho, 15-16 acabado especial).
CODE_SLICES = {
    'composicion': (0, 1), 'dibujo': (1, 2), 'peso': (2, 5), 'hilo': (5, 6), 'galga': (6, 8),
    'op': (8, 9), 'color': (9, 11), 'ancho': (11, 14), 'acabado': (14, 16),
}
# 57.160.0: el acabado en kg (rol 'proceso', sale inspeccionado de la orden de acabado) va entre el teñido y el acabado (J, m).
# Su código es el del acabado con la «I» al frente (DAT P-D02-01: posición 1 = unidad, I = kilogramos), así que
# lleva las claves del acabado (color, ancho, acabado especial).
UNIT_KG = 'I'
ROLE_BY_OP = {'H': 'crudo', 'I': 'tenido', 'J': 'acabado'}
ROLE_ORDER = ('crudo', 'tenido', 'proceso', 'acabado')
ROLE_FIELD = {'crudo': 'sgi_dev_product_crudo_id', 'tenido': 'sgi_dev_product_tenido_id',
              'proceso': 'sgi_dev_product_proceso_id', 'acabado': 'sgi_dev_product_id'}
ROLE_LABEL = {'crudo': "crudo", 'tenido': "teñido", 'proceso': "acabado en kg", 'acabado': "acabado"}
ALL_ROLES = frozenset(ROLE_ORDER)
# Qué niveles cambian con cada clave del proyecto.
KEY_LEVELS = {
    # 57.192.0: el peso es del acabado (lo da la rama); crudo y teñido no cambian con él.
    'composicion': ALL_ROLES, 'dibujo': ALL_ROLES, 'peso': frozenset({'proceso', 'acabado'}), 'hilo': ALL_ROLES,
    'galga': ALL_ROLES,
    'color': frozenset({'tenido', 'proceso', 'acabado'}), 'ancho': frozenset({'proceso', 'acabado'}),
    'ancho_crudo': frozenset({'crudo', 'tenido'}), 'acabado': frozenset({'proceso', 'acabado'}),
}
# Claves de las que depende la fórmula del nivel (los renglones que no son el insumo del nivel inferior).
FORMULA_KEYS = {
    'crudo': frozenset({'composicion', 'dibujo', 'peso', 'hilo', 'galga', 'ancho_crudo'}),
    'tenido': frozenset({'color'}),
    'proceso': frozenset({'acabado'}),
    'acabado': frozenset({'acabado'}),
}
# Claves de las que depende la cantidad del insumo del nivel inferior.
INPUT_QTY_KEYS = {
    'crudo': frozenset(), 'tenido': frozenset({'ancho_crudo'}),
    'proceso': frozenset({'peso', 'ancho', 'ancho_crudo'}),
    'acabado': frozenset({'peso', 'ancho', 'ancho_crudo'}),
}
KEY_LABEL = {
    'composicion': "composición", 'dibujo': "dibujo", 'peso': "peso", 'hilo': "tipo de hilo", 'galga': "galga",
    'color': "color", 'ancho': "ancho acabado", 'ancho_crudo': "ancho crudo", 'acabado': "acabado especial",
}


def parse_code(code):
    """Partes del código o None si no tiene la forma de la regla (al menos 14 posiciones y
    operación H, I o J). 57.160.0: con la «I» al frente (15 o 17 posiciones) es el acabado en kg
    (``role`` 'proceso', ``unit`` 'I'); ninguna composición usa la I, así que no se confunde."""
    code = (code or '').strip().upper()
    if code[:1] == UNIT_KG and len(code) in (15, 17):
        parts = parse_code(code[1:])
        if not parts or parts['role'] != 'acabado':
            return None
        parts.update(role='proceso', unit=UNIT_KG)
        return parts
    if len(code) < 14 or code[8] not in ROLE_BY_OP:
        return None
    parts = {k: code[a:b] for k, (a, b) in CODE_SLICES.items()}
    parts['role'] = ROLE_BY_OP[parts['op']]
    return parts


def build_code(parts):
    return (parts.get('unit') or '') + (
        parts['composicion'] + parts['dibujo'] + parts['peso'] + parts['hilo'] + parts['galga'] + parts['op']
        + parts['color'] + parts['ancho'] + (parts.get('acabado') or ''))


class MrpBomLineDevPending(models.Model):
    _inherit = 'mrp.bom.line'

    sgi_dev_pending = fields.Boolean(
        string="Por capturar", copy=False,
        help="Renglón copiado del artículo base que depende de una clave que cambió en el desarrollo (la fórmula de "
             "color, el hilo…): el componente y la cantidad se capturan antes de cotizar.")


class ProjectProjectDevBase(models.Model):
    _inherit = 'project.project'

    sgi_dev_bom_pending_count = fields.Integer(string="Renglones de lista por capturar",
                                               compute='_compute_sgi_dev_bom_pending_count')

    def _sgi_dev_bom_templates(self):
        self.ensure_one()
        return (self.sgi_dev_product_tmpl_ids
                | (self.sgi_dev_product_crudo_id | self.sgi_dev_product_tenido_id | self.sgi_dev_product_id)
                .mapped('product_tmpl_id'))

    def _sgi_dev_bom_pending_lines(self):
        self.ensure_one()
        tmpls = self._sgi_dev_bom_templates()
        if not tmpls:
            return self.env['mrp.bom.line']
        return self.env['mrp.bom.line'].sudo().search(
            [('sgi_dev_pending', '=', True), ('bom_id.active', '=', True), ('bom_id.product_tmpl_id', 'in', tmpls.ids)])

    def _compute_sgi_dev_bom_pending_count(self):
        for project in self:
            project.sgi_dev_bom_pending_count = len(project._sgi_dev_bom_pending_lines()) if project.sgi_is_ft else 0

    def action_sgi_dev_bom_pending(self):
        self.ensure_one()
        boms = self._sgi_dev_bom_pending_lines().mapped('bom_id')
        return {'type': 'ir.actions.act_window', 'name': "Listas de materiales por capturar", 'res_model': 'mrp.bom',
                'view_mode': 'list,form', 'domain': [('id', 'in', boms.ids)], 'target': 'current'}

    # ------------------------------------------------------------------
    # La cadena del artículo base
    # ------------------------------------------------------------------
    def _sgi_dev_bom_of(self, product):
        self.ensure_one()
        if not product:
            return self.env['mrp.bom']
        return self.env['mrp.bom'].sudo()._bom_find(product, company_id=self.company_id.id).get(product) \
            or self.env['mrp.bom']

    def _sgi_dev_base_chain(self):
        """[{'role', 'product', 'parts', 'bom', 'input_line'}] del artículo base, de abajo hacia
        arriba (crudo → teñido → acabado). Vacío si el base no sigue la regla de codificación."""
        self.ensure_one()
        return self._sgi_dev_chain_from(self.sgi_dev_base_product_id)

    def _sgi_dev_chain_from(self, product):
        """La cadena de `product` por sus listas de materiales (57.192.0: también la de un acabado existente)."""
        self.ensure_one()
        parts = parse_code(product.default_code) if product else None
        # 57.160.0: un base en kg («I…») no da cadena (como antes): se genera por claves.
        if not parts or parts['role'] == 'proceso':
            return []
        chain, seen = [], set()
        while product and parts and product.id not in seen:
            seen.add(product.id)
            bom = self._sgi_dev_bom_of(product)
            input_line = self.env['mrp.bom.line']
            lower_parts = None
            for line in bom.bom_line_ids:
                lp = parse_code(line.product_id.default_code)
                if lp and ROLE_ORDER.index(lp['role']) < ROLE_ORDER.index(parts['role']):
                    input_line, lower_parts = line, lp
                    break
            chain.append({'role': parts['role'], 'product': product, 'parts': parts, 'bom': bom, 'input_line': input_line})
            product, parts = input_line.product_id, lower_parts
        chain.reverse()
        return chain

    def _sgi_dev_project_keys(self):
        """Claves capturadas en el proyecto, en la forma del código; None las que no se capturaron."""
        self.ensure_one()
        gauge = self._sgi_dev_gauge_digits() or None
        return {
            'galga_n': self.sgi_dev_code_galga or None,
            'composicion': (self.sgi_dev_code_composicion_id.code or '').upper() or None,
            'dibujo': (self.sgi_dev_code_dibujo_id.code or '').upper() or None,
            'peso': ("%03d" % self.sgi_dev_code_peso) if 0 < self.sgi_dev_code_peso < 1000 else None,
            'hilo': (self.sgi_dev_code_hilo_id.code or '').upper() or None,
            'galga': gauge or None,
            'color': (self.sgi_dev_code_color_id.code or '').upper() or None,
            'ancho': ("%03d" % self.sgi_dev_code_ancho) if 0 < self.sgi_dev_code_ancho < 1000 else None,
            'ancho_crudo': ("%03d" % self.sgi_dev_code_ancho_crudo) if 0 < self.sgi_dev_code_ancho_crudo < 1000 else None,
            'acabado': (self.sgi_dev_code_acabado_id.code or '').upper() or None,
        }

    def _sgi_dev_changed_keys(self, chain):
        """Claves del proyecto distintas a las del base: las del acabado contra el acabado del base,
        el ancho crudo contra el nivel más bajo del base."""
        self.ensure_one()
        by_role = {level['role']: level['parts'] for level in chain}
        top = by_role.get('acabado') or chain[-1]['parts']
        low = by_role.get('crudo') or by_role.get('tenido')
        keys = self._sgi_dev_project_keys()
        changed = set()
        for key in ('composicion', 'dibujo', 'peso', 'hilo', 'color', 'ancho', 'acabado'):
            if keys[key] is not None and keys[key] != top[key]:
                changed.add(key)
        # 57.142.0: la galga se compara por número, no por dígitos: el base puede llevar 22 y el
        # proyecto proponer 21 para la misma galga 18; en ese caso se conservan los dígitos del base.
        if keys['galga_n'] and keys['galga']:
            Clave = self.env['ficha.tecnica.clave.codigo']
            if Clave.gauge_from_code(top['galga']) != keys['galga_n']:
                changed.add('galga')
        if keys['ancho_crudo'] is not None and low and keys['ancho_crudo'] != low['ancho']:
            changed.add('ancho_crudo')
        return changed, keys

    # ------------------------------------------------------------------
    # Generar desde el base
    # ------------------------------------------------------------------
    @staticmethod
    def _sgi_dev_level_changed(role, changed, colored_yarn):
        """Claves que cambian el código del nivel."""
        return {k for k in changed if role in KEY_LEVELS[k] or (k == 'color' and role == 'crudo' and colored_yarn)}

    @staticmethod
    def _sgi_dev_level_parts(parts, level_changed, keys):
        """Las partes del código del nivel con las claves del proyecto que lo cambian."""
        new_parts = dict(parts)
        for key in level_changed:
            if key == 'ancho_crudo':
                new_parts['ancho'] = keys['ancho_crudo']
            else:
                new_parts[key] = keys[key]
        return new_parts

    def _sgi_dev_forced_lower(self, chain, changed, keys, colored_yarn):
        """({papel: artículo}, por qué) de los niveles bajos que se ligan tal cual: el crudo de tejido que eligió
        Diseño o, si el acabado que pide el proyecto ya existe, el teñido y el crudo que él consume."""
        self.ensure_one()
        if self.sgi_dev_tejido_id:
            return {'crudo': self.sgi_dev_tejido_id}, "crudo de tejido elegido en el proyecto"
        top = chain[-1]
        if top['role'] != 'acabado':
            return {}, ''
        code = build_code(self._sgi_dev_level_parts(
            top['parts'], self._sgi_dev_level_changed('acabado', changed, colored_yarn), keys))
        if code == (top['product'].default_code or '').strip().upper():
            return {}, ''
        existing = self.env['product.product'].with_context(active_test=False).search(
            [('default_code', '=', code)], limit=1)
        if not existing:
            return {}, ''
        forced = {level['role']: level['product'] for level in self._sgi_dev_chain_from(existing)
                  if level['role'] in ('crudo', 'tenido')}
        return forced, "el que ya consume el acabado existente %s" % code

    def _sgi_dev_existing_crudo(self, parts, base_parts):
        """(crudo, otros) existentes con las mismas claves que `parts` y cualquier peso: el de peso más cercano al
        del crudo base."""
        self.ensure_one()
        pattern = build_code(dict(parts, peso='___'))
        found = self.env['product.product'].search([('default_code', '=like', pattern)])
        found = found.filtered(lambda p: (parse_code(p.default_code) or {}).get('role') == 'crudo')
        if not found:
            return found, found
        target = int(base_parts['peso']) if base_parts['peso'].isdigit() else 0
        found = found.sorted(lambda p: (abs(int(parse_code(p.default_code)['peso'] or 0) - target), p.id))
        return found[:1], found[1:]

    def _sgi_dev_base_plan(self, chain=None):
        """Lo que haría «Generar artículos» con el base, sin escribir nada: (pasos, cambiadas, claves). Cada paso,
        de abajo hacia arriba: {'role', 'level', 'base', 'changed', 'code', 'product', 'how', 'notes'}, con
        ``how`` = 'base' (se liga el del base), 'forced' (crudo elegido o el del acabado existente), 'existing'
        (el código ya existe) o 'create'."""
        self.ensure_one()
        chain = self._sgi_dev_base_chain() if chain is None else chain
        if not chain:
            return [], set(), {}
        Product = self.env['product.product'].with_context(active_test=False)
        changed, keys = self._sgi_dev_changed_keys(chain)
        colored_yarn = (keys['hilo'] or chain[-1]['parts']['hilo']) in COLORED_YARN_CODES
        forced, why = self._sgi_dev_forced_lower(chain, changed, keys, colored_yarn)
        base_crudo = next((level['product'] for level in chain if level['role'] == 'crudo'), None)
        plan, actual = [], {}
        for level in chain:
            role, base = level['role'], level['product']
            step = {'role': role, 'level': level, 'base': base, 'notes': [],
                    'changed': self._sgi_dev_level_changed(role, changed, colored_yarn)}
            if role in forced:
                step.update(product=forced[role], code=forced[role].default_code, how='forced')
                step['notes'].append(why)
            else:
                parts = self._sgi_dev_level_parts(level['parts'], step['changed'], keys)
                crudo = actual.get('crudo')
                crudo_parts = parse_code(crudo.default_code) if crudo else None
                if role == 'tenido' and crudo_parts and crudo != base_crudo:
                    # El teñido es el crudo que entra, con su color.
                    for key in ('composicion', 'dibujo', 'peso', 'hilo', 'galga', 'ancho'):
                        parts[key] = crudo_parts[key]
                code = build_code(parts)
                product = Product.browse()
                if code == (base.default_code or '').strip().upper():
                    step.update(product=base, code=base.default_code, how='base')
                else:
                    product = Product.search([('default_code', '=', code)], limit=1)
                    if not product and role == 'crudo':
                        product, others = self._sgi_dev_existing_crudo(parts, level['parts'])
                        if product:
                            step['notes'].append("el crudo %s no existe; se toma el crudo existente %s%s (verifíquelo)" % (
                                code, product.default_code,
                                (", también hay %s" % ", ".join(others.mapped('default_code'))) if others else ''))
                            code = product.default_code
                    step.update(product=product, code=code, how='existing' if product else 'create')
            actual[role] = step['product'] or None
            plan.append(step)
        return plan, changed, keys

    def _sgi_dev_input_of(self, product, lower_role):
        """El insumo del nivel `lower_role` en la lista de materiales de `product`."""
        for line in self._sgi_dev_bom_of(product).bom_line_ids:
            parts = parse_code(line.product_id.default_code)
            if parts and parts['role'] == lower_role:
                return line.product_id
        return self.env['product.product']

    def _sgi_dev_generate_from_base(self):
        """Liga o crea los artículos de la ruta a partir del base. Devuelve False si el base no
        tiene cadena que seguir (el que llama cae al generador por claves)."""
        self.ensure_one()
        chain = self._sgi_dev_base_chain()
        if not chain:
            return False
        Product = self.env['product.product']
        plan, _changed, keys = self._sgi_dev_base_plan(chain)
        new_by_role, log, pending_total = {}, [], 0
        name = self._sgi_dev_product_name() if 0 < self.sgi_dev_code_peso < 1000 else chain[-1]['product'].name
        for index, step in enumerate(plan):
            role, level, base_product = step['role'], step['level'], step['base']
            label = ROLE_LABEL[role].capitalize()
            notes = ("; " + "; ".join(step['notes'])) if step['notes'] else ''
            lower_role = plan[index - 1]['role'] if index else None
            lower_new = new_by_role.get(lower_role)
            if self[ROLE_FIELD[role]]:
                product = self[ROLE_FIELD[role]]
                log.append(Markup("<li>%s: ya estaba ligado <b>%s</b>; no se toca.</li>") % (label, product.default_code))
                new_by_role[role] = product
                continue
            if step['how'] != 'create':
                product = step['product']
                self.write({ROLE_FIELD[role]: product.id})
                new_by_role[role] = product
                if step['how'] == 'base':
                    text = Markup("se liga el del base, <b>%s</b> (no cambia)") % product.default_code
                elif step['how'] == 'forced':
                    text = Markup("se liga <b>%s</b>") % product.default_code
                else:
                    text = Markup("<b>%s</b> ya existía, se liga desde %s (cambió %s)") % (
                        product.default_code, base_product.default_code,
                        ", ".join(KEY_LABEL[k] for k in sorted(step['changed'])) or "el insumo")
                consumed = self._sgi_dev_input_of(product, lower_role) if lower_new else None
                warn = (Markup(" <b>Atención:</b> su lista de materiales consume %s, no %s; revísela.") % (
                    consumed.default_code, lower_new.default_code)) if consumed and consumed != lower_new else ''
                log.append(Markup("<li>%s: %s%s.%s</li>") % (label, text, notes, warn))
                continue
            code, bom = step['code'], level['bom']
            product = Product.create({
                'name': name, 'default_code': code, 'type': 'consu', 'is_storable': True,
                'uom_id': base_product.uom_id.id, 'categ_id': base_product.categ_id.id,
                'sale_ok': False, 'purchase_ok': False, 'company_id': self.company_id.id or False,
                'sgi_dev_state': 'desarrollo', 'sgi_dev_project_id': self.id, 'sgi_dev_role': role,
            })
            self.write({ROLE_FIELD[role]: product.id})
            new_by_role[role] = product
            pending_here = []
            if bom:
                new_bom = bom.copy({'product_tmpl_id': product.product_tmpl_id.id, 'product_id': False})
                base_input = level['input_line'].product_id if level['input_line'] else None
                # 57.160.0: el papel real del insumo (un base sin acabado en proceso salta del teñido al acabado).
                input_parts = parse_code(base_input.default_code) if base_input else None
                input_new = new_by_role.get(input_parts['role']) if input_parts else None
                for line in new_bom.bom_line_ids:
                    if base_input and line.product_id == base_input:
                        swapped = bool(input_new and input_new != line.product_id)
                        if swapped:
                            line.product_id = input_new
                        # La cantidad cambia si cambia una clave que la mueve o si entra un insumo de otro peso o ancho.
                        new_parts = parse_code(input_new.default_code) if swapped else None
                        other_size = bool(new_parts) and (new_parts['peso'], new_parts['ancho']) != (
                            input_parts['peso'], input_parts['ancho'])
                        line.sgi_dev_pending = other_size or bool(step['changed'] & INPUT_QTY_KEYS[role])
                    else:
                        line.sgi_dev_pending = bool(step['changed'] & FORMULA_KEYS[role])
                    if line.sgi_dev_pending:
                        pending_here.append(line.product_id.default_code or line.product_id.name)
                pending_total += len(pending_here)
            log.append(Markup("<li>%s: <b>%s</b> creado desde %s (cambió %s%s). %s</li>") % (
                label, code, base_product.default_code,
                ", ".join(KEY_LABEL[k] for k in sorted(step['changed'])) or "el insumo", notes,
                ("Lista de materiales copiada de %s%s." % (
                    bom.display_name, ("; por capturar: %s" % ", ".join(pending_here)) if pending_here else ''))
                if bom else "Sin lista de materiales que copiar."))
        roles = {level['role'] for level in chain}
        if 'tenido' not in roles and self.sgi_dev_code_tenido and keys['color'] and keys['color'] != NATURAL_COLOR:
            log.append(Markup("<li>Teñido: el artículo base no lleva teñido; el proyecto pide color %s. Capture el "
                              "teñido y su lista a mano.</li>") % keys['color'])
        # 57.192.1: Markup, si no Odoo 19 escapa el HTML y el chatter muestra las etiquetas.
        self.message_post(body=Markup("Artículos generados desde el base <b>%s</b>:<ul>%s</ul>%s") % (
            self.sgi_dev_base_product_id.default_code, Markup("").join(log),
            (Markup("<p><b>%d renglón(es) de lista de materiales por capturar</b>: la cotización no se manda a "
                    "aprobar hasta capturarlos.</p>") % pending_total) if pending_total else ''))
        return True

    # ------------------------------------------------------------------
    # 57.192.0: los códigos propuestos salen del mismo plan
    # ------------------------------------------------------------------
    @api.depends('sgi_dev_code_composicion_id', 'sgi_dev_code_dibujo_id', 'sgi_dev_code_peso', 'sgi_dev_code_hilo_id',
                 'sgi_dev_code_galga', 'sgi_dev_code_galga_digits', 'sgi_dev_code_color_id', 'sgi_dev_code_ancho',
                 'sgi_dev_code_ancho_crudo', 'sgi_dev_code_acabado_id', 'sgi_dev_code_tenido',
                 'sgi_dev_base_product_id', 'sgi_dev_tejido_id')
    def _compute_sgi_dev_codes(self):
        super()._compute_sgi_dev_codes()
        for project in self:
            if project.sgi_dev_tejido_id:
                project.sgi_dev_code_crudo = project.sgi_dev_tejido_id.default_code
            if not (project.sgi_is_ft and project.sgi_dev_base_product_id):
                continue
            plan = project._sgi_dev_base_plan()[0]
            codes = {step['role']: step['code'] for step in plan}
            if codes.get('crudo'):
                project.sgi_dev_code_crudo = codes['crudo']
            if codes.get('tenido') and project.sgi_dev_code_tenido:
                project.sgi_dev_code_tenido_code = codes['tenido']
            if codes.get('acabado'):
                project.sgi_dev_code_acabado_code = codes['acabado']


class ProjectProjectDevGenerate(models.Model):
    _inherit = 'project.project'

    def action_sgi_dev_generate_products(self):
        """57.141.0: un producto de línea no genera artículos (se cotiza el de línea); con artículo
        base se parte de su cadena; sin base, el generador por claves de siempre."""
        for project in self.filtered('sgi_is_ft'):
            if project.sgi_dev_analysis_result == 'linea':
                raise UserError("El resultado del análisis es «Producto de línea»: se cotiza el artículo de línea %s, "
                                "no se generan artículos nuevos." % (project.sgi_dev_base_product_id.display_name or ''))
        from_base = self.filtered(lambda p: p.sgi_is_ft and p.sgi_dev_base_product_id)
        rest = self
        for project in from_base:
            project.action_sgi_dev_code_from_table()
            if project._sgi_dev_generate_from_base():
                rest -= project
            else:
                project.message_post(body="El artículo base %s no sigue la regla de codificación: los artículos se "
                                          "generan con las claves del proyecto." % project.sgi_dev_base_product_id.default_code)
        if rest:
            super(ProjectProjectDevGenerate, rest).action_sgi_dev_generate_products()
        return True

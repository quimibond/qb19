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
"""
from odoo import fields, models
from odoo.exceptions import UserError

from .sgi_dev_product import COLORED_YARN_CODES, NATURAL_COLOR

# Posiciones del código (1 composición, 2 dibujo, 3-5 peso, 6 hilo, 7-8 galga, 9 operación,
# 10-11 color, 12-14 ancho, 15-16 acabado especial).
CODE_SLICES = {
    'composicion': (0, 1), 'dibujo': (1, 2), 'peso': (2, 5), 'hilo': (5, 6), 'galga': (6, 8),
    'op': (8, 9), 'color': (9, 11), 'ancho': (11, 14), 'acabado': (14, 16),
}
ROLE_BY_OP = {'H': 'crudo', 'I': 'tenido', 'J': 'acabado'}
ROLE_ORDER = ('crudo', 'tenido', 'acabado')
ROLE_FIELD = {'crudo': 'sgi_dev_product_crudo_id', 'tenido': 'sgi_dev_product_tenido_id', 'acabado': 'sgi_dev_product_id'}
ROLE_LABEL = {'crudo': "crudo", 'tenido': "teñido", 'acabado': "acabado"}
ALL_ROLES = frozenset(ROLE_ORDER)
# Qué niveles cambian con cada clave del proyecto.
KEY_LEVELS = {
    'composicion': ALL_ROLES, 'dibujo': ALL_ROLES, 'peso': ALL_ROLES, 'hilo': ALL_ROLES, 'galga': ALL_ROLES,
    'color': frozenset({'tenido', 'acabado'}), 'ancho': frozenset({'acabado'}),
    'ancho_crudo': frozenset({'crudo', 'tenido'}), 'acabado': frozenset({'acabado'}),
}
# Claves de las que depende la fórmula del nivel (los renglones que no son el insumo del nivel inferior).
FORMULA_KEYS = {
    'crudo': frozenset({'composicion', 'dibujo', 'peso', 'hilo', 'galga', 'ancho_crudo'}),
    'tenido': frozenset({'color'}),
    'acabado': frozenset({'acabado'}),
}
# Claves de las que depende la cantidad del insumo del nivel inferior.
INPUT_QTY_KEYS = {
    'crudo': frozenset(), 'tenido': frozenset({'peso', 'ancho_crudo'}),
    'acabado': frozenset({'peso', 'ancho', 'ancho_crudo'}),
}
KEY_LABEL = {
    'composicion': "composición", 'dibujo': "dibujo", 'peso': "peso", 'hilo': "tipo de hilo", 'galga': "galga",
    'color': "color", 'ancho': "ancho acabado", 'ancho_crudo': "ancho crudo", 'acabado': "acabado especial",
}


def parse_code(code):
    """Partes del código o None si no tiene la forma de la regla (al menos 14 posiciones y
    operación H, I o J)."""
    code = (code or '').strip().upper()
    if len(code) < 14 or code[8] not in ROLE_BY_OP:
        return None
    parts = {k: code[a:b] for k, (a, b) in CODE_SLICES.items()}
    parts['role'] = ROLE_BY_OP[parts['op']]
    return parts


def build_code(parts):
    return (parts['composicion'] + parts['dibujo'] + parts['peso'] + parts['hilo'] + parts['galga'] + parts['op']
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
        product = self.sgi_dev_base_product_id
        parts = parse_code(product.default_code) if product else None
        if not parts:
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
        Clave = self.env['ficha.tecnica.clave.codigo']
        gauge = Clave.gauge_code(self.sgi_dev_code_galga) if self.sgi_dev_code_galga else None
        return {
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
        for key in ('composicion', 'dibujo', 'peso', 'hilo', 'galga', 'color', 'ancho', 'acabado'):
            if keys[key] is not None and keys[key] != top[key]:
                changed.add(key)
        if keys['ancho_crudo'] is not None and low and keys['ancho_crudo'] != low['ancho']:
            changed.add('ancho_crudo')
        return changed, keys

    # ------------------------------------------------------------------
    # Generar desde el base
    # ------------------------------------------------------------------
    def _sgi_dev_generate_from_base(self):
        """Liga o crea los artículos de la ruta a partir del base. Devuelve False si el base no
        tiene cadena que seguir (el que llama cae al generador por claves)."""
        self.ensure_one()
        chain = self._sgi_dev_base_chain()
        if not chain:
            return False
        Product = self.env['product.product']
        changed, keys = self._sgi_dev_changed_keys(chain)
        colored_yarn = (keys['hilo'] or chain[-1]['parts']['hilo']) in COLORED_YARN_CODES
        top_parts = chain[-1]['parts']
        old_top_peso = int(top_parts['peso'] or 0)
        new_top_peso = int(keys['peso']) if 'peso' in changed else old_top_peso
        new_by_role, log, pending_total = {}, [], 0
        name = self._sgi_dev_product_name() if 0 < self.sgi_dev_code_peso < 1000 else chain[-1]['product'].name
        for level in chain:
            role, base_product, parts, bom = level['role'], level['product'], level['parts'], level['bom']
            level_changed = {k for k in changed if role in KEY_LEVELS[k] or (k == 'color' and role == 'crudo' and colored_yarn)}
            if not level_changed:
                if not self[ROLE_FIELD[role]]:
                    self.write({ROLE_FIELD[role]: base_product.id})
                new_by_role[role] = base_product
                log.append("<li>%s: se liga el del base, <b>%s</b> (no cambia).</li>" % (ROLE_LABEL[role].capitalize(),
                                                                                           base_product.default_code))
                continue
            new_parts = dict(parts)
            notes = []
            for key in level_changed:
                if key == 'peso' and role != 'acabado' and old_top_peso:
                    scaled = max(1, min(999, round(int(parts['peso']) * new_top_peso / old_top_peso)))
                    new_parts['peso'] = "%03d" % scaled
                    notes.append("peso %s estimado en proporción al base (verifíquelo)" % new_parts['peso'])
                elif key == 'ancho_crudo':
                    new_parts['ancho'] = keys['ancho_crudo']
                elif key == 'color' and role == 'crudo':
                    new_parts['color'] = keys['color']
                else:
                    new_parts[key] = keys[key]
            code = build_code(new_parts)
            if self[ROLE_FIELD[role]]:
                product = self[ROLE_FIELD[role]]
                log.append("<li>%s: ya estaba ligado <b>%s</b>; no se toca.</li>" % (ROLE_LABEL[role].capitalize(), product.default_code))
                new_by_role[role] = product
                continue
            product = Product.with_context(active_test=False).search([('default_code', '=', code)], limit=1)
            created = False
            if not product:
                product = Product.create({
                    'name': name, 'default_code': code, 'type': 'consu', 'is_storable': True,
                    'uom_id': base_product.uom_id.id, 'categ_id': base_product.categ_id.id,
                    'sale_ok': False, 'purchase_ok': False, 'company_id': self.company_id.id or False,
                    'sgi_dev_state': 'desarrollo', 'sgi_dev_project_id': self.id, 'sgi_dev_role': role,
                })
                created = True
            self.write({ROLE_FIELD[role]: product.id})
            new_by_role[role] = product
            pending_here = []
            if bom and created:
                new_bom = bom.copy({'product_tmpl_id': product.product_tmpl_id.id, 'product_id': False})
                lower_role = ROLE_ORDER[ROLE_ORDER.index(role) - 1] if ROLE_ORDER.index(role) else None
                for line in new_bom.bom_line_ids:
                    is_input = level['input_line'] and line.product_id == level['input_line'].product_id
                    if is_input:
                        lower_new = new_by_role.get(lower_role)
                        if lower_new and lower_new != line.product_id:
                            line.product_id = lower_new
                        line.sgi_dev_pending = bool(level_changed & INPUT_QTY_KEYS[role])
                    else:
                        line.sgi_dev_pending = bool(level_changed & FORMULA_KEYS[role])
                    if line.sgi_dev_pending:
                        pending_here.append(line.product_id.default_code or line.product_id.name)
                pending_total += len(pending_here)
            log.append("<li>%s: <b>%s</b> %s desde %s (cambió %s%s). %s</li>" % (
                ROLE_LABEL[role].capitalize(), code, "creado" if created else "ya existía, se liga",
                base_product.default_code, ", ".join(KEY_LABEL[k] for k in sorted(level_changed)),
                ("; " + "; ".join(notes)) if notes else '',
                ("Lista de materiales copiada de %s%s." % (
                    bom.display_name, ("; por capturar: %s" % ", ".join(pending_here)) if pending_here else ''))
                if (bom and created) else ("Sin lista de materiales que copiar." if created else "")))
        roles = {level['role'] for level in chain}
        if 'tenido' not in roles and self.sgi_dev_code_tenido and keys['color'] and keys['color'] != NATURAL_COLOR:
            log.append("<li>Teñido: el artículo base no lleva teñido; el proyecto pide color %s. Capture el teñido y su "
                       "lista a mano.</li>" % keys['color'])
        self.message_post(body="Artículos generados desde el base <b>%s</b>:<ul>%s</ul>%s" % (
            self.sgi_dev_base_product_id.default_code, "".join(log),
            ("<p><b>%d renglón(es) de lista de materiales por capturar</b>: la cotización no se manda a aprobar "
             "hasta capturarlos.</p>" % pending_total) if pending_total else ''))
        return True


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

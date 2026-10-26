# -*- coding: utf-8 -*-
from collections import defaultdict

from odoo import _, api, fields, models
from odoo.exceptions import UserError


def _fmt_dosis(gpl, pct):
    if gpl:
        return '%s g/L' % ('%.4f' % gpl).rstrip('0').rstrip('.')
    return '%s %%' % ('%.5f' % pct).rstrip('0').rstrip('.')


class MrpBom(models.Model):
    _inherit = 'mrp.bom'

    formula_tintoreria_id = fields.Many2one(
        'tintoreria.formula', 'Fórmula de tintorería', tracking=True, index=True, copy=True,
        help="Fórmula del catálogo de la que provienen los ingredientes de Tintorería. "
             "Es modificable; las líneas se cargan con el botón «Cargar desde fórmula».")
    formula_estado = fields.Selection(
        [('sin_formula', 'Sin fórmula'),
         ('sincronizada', 'Sincronizada con el catálogo'),
         ('modificada', 'Difiere del catálogo')],
        'Estado de la fórmula', compute='_compute_formula_estado', store=True)

    # ------------------------------------------------------------------
    # Comparación contra el catálogo
    # ------------------------------------------------------------------
    @api.model
    def _firmas_dosis(self, registros):
        """{producto: [(g/L, %), ...]} ordenado, para comparar BoM contra catálogo."""
        firmas = defaultdict(list)
        for reg in registros:
            firmas[reg.product_id].append((round(reg.gramos_por_litro, 4), round(reg.porcentaje_colorante, 5)))
        return {producto: sorted(valores) for producto, valores in firmas.items()}

    def _diferencias_formula(self):
        """Lista de textos con las diferencias entre las líneas dosificadas de la BoM y la fórmula."""
        self.ensure_one()
        formula = self.formula_tintoreria_id
        if not formula:
            return []
        en_bom = self._firmas_dosis(self.bom_line_ids.filtered('dosificado'))
        en_catalogo = self._firmas_dosis(formula.line_ids)
        diferencias = []
        for producto in sorted(set(en_bom) | set(en_catalogo), key=lambda p: p.display_name or ''):
            bom_vals = en_bom.get(producto)
            cat_vals = en_catalogo.get(producto)
            nombre = producto.default_code or producto.display_name
            if bom_vals == cat_vals:
                continue
            if bom_vals is None:
                diferencias.append(_("Falta en la BoM: %(p)s (%(d)s)", p=nombre,
                                     d=', '.join(_fmt_dosis(*v) for v in cat_vals)))
            elif cat_vals is None:
                diferencias.append(_("No está en el catálogo: %(p)s (%(d)s)", p=nombre,
                                     d=', '.join(_fmt_dosis(*v) for v in bom_vals)))
            else:
                diferencias.append(_("%(p)s: BoM %(b)s vs catálogo %(c)s", p=nombre,
                                     b=', '.join(_fmt_dosis(*v) for v in bom_vals),
                                     c=', '.join(_fmt_dosis(*v) for v in cat_vals)))
        return diferencias

    @api.depends('formula_tintoreria_id',
                 'formula_tintoreria_id.line_ids.product_id',
                 'formula_tintoreria_id.line_ids.gramos_por_litro',
                 'formula_tintoreria_id.line_ids.porcentaje_colorante',
                 'bom_line_ids.product_id',
                 'bom_line_ids.gramos_por_litro',
                 'bom_line_ids.porcentaje_colorante',
                 'bom_line_ids.dosificado')
    def _compute_formula_estado(self):
        for bom in self:
            if not bom.formula_tintoreria_id:
                bom.formula_estado = 'sin_formula'
            elif bom._diferencias_formula():
                bom.formula_estado = 'modificada'
            else:
                bom.formula_estado = 'sincronizada'

    # ------------------------------------------------------------------
    # Acciones
    # ------------------------------------------------------------------
    def _operacion_tintoreria(self):
        """Operación de Tintorería de la BoM: la que usa un work center de tipo Tintorería (ficha de
        la máquina) o cuyo nombre/work center contiene 'tintorer'."""
        self.ensure_one()
        candidatas = self.operation_ids.filtered(
            lambda op: 'tintorer' in (op.workcenter_id.ft_tipo_id.name or '').lower()
            or 'tintorer' in (op.name or '').lower()
            or 'tintorer' in (op.workcenter_id.name or '').lower())
        return candidatas.sorted('sequence')[:1]

    def action_cargar_formula(self):
        """Reemplaza los ingredientes dosificados de la BoM por los de la fórmula del catálogo."""
        Linea = self.env['mrp.bom.line']
        for bom in self:
            formula = bom.formula_tintoreria_id
            if not formula:
                raise UserError(_("Selecciona primero la fórmula de tintorería en la BoM."))
            if not formula.line_ids:
                raise UserError(_("La fórmula %s no tiene ingredientes en el catálogo.", formula.name))
            operacion = bom._operacion_tintoreria()
            if not operacion:
                raise UserError(_(
                    "La BoM no tiene una operación de Tintorería. Agrégala en la pestaña Operaciones "
                    "con un work center de Tintorería."))
            a_borrar = bom.bom_line_ids.filtered(
                lambda ln: ln.formula_line_id or ln.dosificado
                or (formula.product_id and ln.product_id == formula.product_id))
            quitadas = len(a_borrar)
            a_borrar.unlink()
            Linea.create([{
                'bom_id': bom.id,
                'product_id': fl.product_id.id,
                'product_uom_id': fl.product_id.uom_id.id,
                'operation_id': operacion.id,
                'gramos_por_litro': fl.gramos_por_litro,
                'porcentaje_colorante': fl.porcentaje_colorante,
                'formula_line_id': fl.id,
                'sequence': 100 + fl.sequence,
            } for fl in formula.line_ids.sorted('sequence')])
            bom.message_post(body=_(
                "Ingredientes cargados desde la fórmula %(f)s: %(n)s líneas creadas, %(q)s reemplazadas "
                "(operación %(op)s).", f=formula.name, n=len(formula.line_ids), q=quitadas, op=operacion.name))
        return True

    def action_validar_formula(self):
        self.ensure_one()
        if not self.formula_tintoreria_id:
            raise UserError(_("La BoM no tiene fórmula de tintorería."))
        diferencias = self._diferencias_formula()
        if diferencias:
            titulo, tipo, mensaje, fijo = (
                _("La BoM difiere del catálogo"), 'warning', ' | '.join(diferencias[:12]), True)
        else:
            titulo, tipo, mensaje, fijo = (
                _("BoM sincronizada"), 'success',
                _("Las líneas dosificadas coinciden con la fórmula %s.", self.formula_tintoreria_id.name), False)
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {'title': titulo, 'message': mensaje, 'type': tipo, 'sticky': fijo},
        }

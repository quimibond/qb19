# -*- coding: utf-8 -*-
import logging

from odoo import models, fields, api, _
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

# Tolerancia para permitir redondeos de báscula al validar contra el stock del sistema
PESO_TOLERANCIA_KG = 0.005


class MrpYarnReturnWizard(models.TransientModel):
    _name = 'mrp.yarn.return.wizard'
    _inherit = ['scale.wizard.mixin']
    _description = 'Asistente para Devolución de Hilo Sobrante por Producto'

    # NOTA: 'weighing_mode', 'iot_device_id' y 'scale_read_url' ya llegan de
    # scale.wizard.mixin (iot_scale_common). No se redeclaran aquí para no
    # arriesgar que este wizard se desincronice de esa definición común,
    # igual que mrp_revisado_telas / pesaje_rollos_tejido.

    @api.model
    def _search_location(self, complete_name):
        company = self.env.company
        loc = self.env['stock.location'].search([
            ('complete_name', '=', complete_name),
            ('usage', '=', 'internal'),
            '|', ('company_id', '=', company.id), ('company_id', '=', False),
        ], limit=1)
        return loc

    @api.model
    def _get_default_location_id(self):
        loc = self._search_location('Toluca/Stock/2 PRODUCCIÓN')
        return loc.id if loc else False

    @api.model
    def _get_default_location_dest_id(self):
        loc = self._search_location('Toluca/Stock/1 Materia Prima')
        return loc.id if loc else False

    @api.model
    def _get_default_picking_type_id(self):
        # Tipo de operación fijo para este wizard: siempre "Toluca: Devolución
        # Producción". No se deja a elección del usuario -- si no existe o no
        # está configurado en esta compañía, se avisa explícitamente en vez de
        # dejar el campo vacío en silencio (evita que alguien elija el tipo de
        # operación equivocado por accidente).
        company = self.env.company
        picking_type = self.env['stock.picking.type'].search([
            ('name', '=', 'Devolución Producción'),
            ('code', '=', 'internal'),
            ('company_id', '=', company.id),
        ], limit=1)
        if not picking_type:
            raise UserError(_(
                "No se encontró el tipo de operación 'Devolución Producción' para la "
                "compañía %s. Verifique su configuración en Inventario > Configuración > "
                "Tipos de Operación antes de usar este asistente."
            ) % company.name)
        return picking_type.id

    location_id = fields.Many2one('stock.location', string='Ubicación Origen', required=True, default=_get_default_location_id)
    location_dest_id = fields.Many2one('stock.location', string='Ubicación Destino', required=True, default=_get_default_location_dest_id)
    picking_type_id = fields.Many2one('stock.picking.type', string='Tipo de Operación', required=True,
                                       domain=[('code', '=', 'internal')], default=_get_default_picking_type_id)
    product_id = fields.Many2one('product.product', string='Hilo a Devolver', required=True, domain="[('categ_id.complete_name', '=like', 'Materia Prima / Hilo%')]")

    # Una sola tabla persistente en base de datos temporal
    line_ids = fields.One2many('mrp.yarn.return.wizard.line', 'wizard_id', string='Lotes Encontrados')

    # CAMPOS DE CAPTURA RÁPIDA EN EL ENCABEZADO
    input_lot_name = fields.Char(string='Lote/Caja a Pesar')
    input_peso_bruto = fields.Float(string='Peso Bruto Captura (kg)', digits=(16, 3))
    input_tara = fields.Float(string='Tara Captura (kg)', digits=(16, 3))

    # Igual que mrp.production en pesaje_rollos_tejido: el reporte qweb-text
    # simplemente vuelca este campo. La impresión física la resuelve el
    # Virtual IoT Box configurado como impresora del sistema en esa área,
    # no un client action ni un fetch al navegador.
    last_zpl_label = fields.Text(string='Última Etiqueta ZPL', readonly=True, copy=False)

    def _reload_form(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'mrp.yarn.return.wizard',
            'res_id': self.id,
            'view_mode': 'form',
            'target': 'new',
        }

    def action_load_yarn_inventory(self):
        """ Carga física y persistente de existencias del hilo en la ubicación origen """
        self.ensure_one()
        if not self.product_id or not self.location_id:
            raise UserError(_("Por favor, seleccione un Hilo y la Ubicación Origen antes de continuar."))

        # Protección: si ya hay líneas marcadas para devolver sin procesar, evitamos
        # borrar el trabajo capturado por accidente (p.ej. doble clic en el botón).
        pending = self.line_ids.filtered(lambda l: l.to_return and l.peso_neto > 0)
        if pending:
            raise UserError(_(
                "Ya existen %d lote(s) pesados y marcados para devolver que aún no se han "
                "procesado. Procese o desmarque esos lotes antes de recargar las existencias, "
                "de lo contrario se perderá esa captura."
            ) % len(pending))

        self.line_ids.unlink()

        quants = self.env['stock.quant'].search([
            ('product_id', '=', self.product_id.id),
            ('location_id', '=', self.location_id.id),
            ('quantity', '>', 0)
        ])

        new_lines = []
        for quant in quants:
            if not quant.lot_id:
                continue
            lot_name = quant.lot_id.name or ''
            box_no = lot_name[-4:] if len(lot_name) >= 4 else lot_name
            new_lines.append((0, 0, {
                'to_return': False,
                'lot_id': quant.lot_id.id,
                'box_no': box_no,
                'peso_actual': quant.quantity,
                'peso_bruto': 0.0,
                'tara': 0.0,
            }))

        self.write({
            'line_ids': new_lines,
            'input_lot_name': False,
            'input_peso_bruto': 0.0,
            'input_tara': 0.0
        })

        return self._reload_form()

    def action_update_line_from_input(self):
        """ Actualiza la fila y limpia el encabezado manteniendo la ventana modal abierta """
        self.ensure_one()
        if not self.input_lot_name:
            raise UserError(_("Por favor, ingrese o escanee un número de lote o caja."))

        clean_input = str(self.input_lot_name).replace(" ", "").upper()

        target_line = self.line_ids.filtered(
            lambda l: (l.lot_id and l.lot_id.name and l.lot_id.name.replace(" ", "").upper() == clean_input) or
                      (l.box_no and l.box_no.replace(" ", "").upper() == clean_input)
        )

        if not target_line:
            raise UserError(_("El lote o caja '%s' no se encuentra en las existencias cargadas abajo.") % self.input_lot_name)

        line = target_line[0]

        if self.input_peso_bruto <= 0:
            raise UserError(_("El peso bruto capturado debe ser mayor a cero."))
        if self.input_tara < 0 or self.input_tara >= self.input_peso_bruto:
            raise UserError(_(
                "La tara (%.3f kg) debe ser menor al peso bruto (%.3f kg). Revise la captura."
            ) % (self.input_tara, self.input_peso_bruto))

        peso_neto = self.input_peso_bruto - self.input_tara
        if peso_neto > line.peso_actual + PESO_TOLERANCIA_KG:
            raise UserError(_(
                "El peso neto capturado (%.3f kg) excede el stock del sistema para este lote (%.3f kg)."
            ) % (peso_neto, line.peso_actual))

        line.write({
            'peso_bruto': self.input_peso_bruto,
            'tara': self.input_tara,
            'to_return': True,
        })

        self.write({
            'input_lot_name': False,
            'input_peso_bruto': 0.0,
            'input_tara': 0.0
        })

        return self._reload_form()

    def action_generate_internal_transfer(self):
        """ Genera el movimiento de inventario real e imprime las etiquetas ZPL
        usando el mismo mecanismo que pesaje_rollos_tejido: un ir.actions.report
        tipo qweb-text que vuelca el campo last_zpl_label. La impresión física
        la resuelve el Virtual IoT Box configurado como impresora del sistema
        en el área de producción (no un client action print_zpl_usb, que es
        exclusivo del área de recepción con impresora USB directa).
        """
        self.ensure_one()
        if not self.line_ids:
            raise UserError(_("No hay lotes en la lista."))

        lines_to_process = self.line_ids.filtered(lambda l: l.to_return)
        if not lines_to_process:
            raise UserError(_("Por favor, registre y marque al menos un lote para devolver."))

        for line in lines_to_process:
            if line.peso_neto <= 0:
                raise UserError(_("El lote %s debe tener un peso neto mayor a cero.") % line.lot_id.name)
            if line.peso_neto > line.peso_actual + PESO_TOLERANCIA_KG:
                raise UserError(_(
                    "El lote %s tiene un peso neto (%.3f kg) mayor al stock disponible (%.3f kg)."
                ) % (line.lot_id.name, line.peso_neto, line.peso_actual))

        picking = self.env['stock.picking'].create({
            'picking_type_id': self.picking_type_id.id,
            'location_id': self.location_id.id,
            'location_dest_id': self.location_dest_id.id,
            'origin': _('Devolución de Hilo Sobrante - %s') % self.product_id.name,
        })

        for line in lines_to_process:
            move = self.env['stock.move'].create({
                'product_id': self.product_id.id,
                'product_uom_qty': line.peso_neto,
                'product_uom': self.product_id.uom_id.id,
                'picking_id': picking.id,
                'location_id': self.location_id.id,
                'location_dest_id': self.location_dest_id.id,
            })
            self.env['stock.move.line'].create({
                'move_id': move.id,
                'product_id': self.product_id.id,
                'lot_id': line.lot_id.id,
                'quantity': line.peso_neto,
                'product_uom_id': self.product_id.uom_id.id,
                'location_id': self.location_id.id,
                'location_dest_id': self.location_dest_id.id,
                'picking_id': picking.id,
            })

        picking.action_confirm()
        picking.action_assign()

        if picking.state != 'assigned':
            raise UserError(_(
                "La transferencia %s quedó en espera de disponibilidad. Verifique que el stock "
                "del lote no haya cambiado desde que se cargaron las existencias."
            ) % picking.name)

        # A propósito NO se llama a button_validate(): el traslado debe quedar
        # confirmado y reservado (estado 'assigned'), listo para que el
        # almacén lo valide físicamente cuando reciba el hilo. Validarlo aquí
        # daría por hecho un movimiento que todavía no ocurre en planta.

        zpl_body = "".join(line.get_zpl_label() for line in lines_to_process)
        self.last_zpl_label = zpl_body

        action = self.env.ref('devolucion_hilo_produccion.action_report_yarn_return_zpl').report_action(self)
        # 'effect' es el mecanismo estándar de Odoo para mostrar una
        # confirmación (rainbow man) encima de cualquier acción devuelta,
        # sin necesidad de encadenar dos acciones distintas.
        action['effect'] = {
            'fadeout': 'slow',
            'message': _('Traslado Generado: %s') % picking.name,
            'type': 'rainbow_man',
        }
        return action


class MrpYarnReturnWizardLine(models.TransientModel):
    _name = 'mrp.yarn.return.wizard.line'
    _description = 'Línea de Lote Encontrado para Devolución'

    wizard_id = fields.Many2one('mrp.yarn.return.wizard', ondelete='cascade')
    product_id = fields.Many2one('product.product', string='Hilo', related='wizard_id.product_id', store=True)
    lot_id = fields.Many2one('stock.lot', string='Lote / Serie', readonly=True)
    box_no = fields.Char(string='N. Caja', readonly=True)
    peso_actual = fields.Float(string='Stock Sistema (kg)', readonly=True, digits=(16, 3))
    peso_bruto = fields.Float(string='Peso Bruto (kg)', digits=(16, 3))
    tara = fields.Float(string='Tara (kg)', digits=(16, 3))
    peso_neto = fields.Float(string='Peso Neto (kg)', compute='_compute_peso_neto', store=True, digits=(16, 3))
    to_return = fields.Boolean(string='Devolver', default=False)

    @api.depends('peso_bruto', 'tara')
    def _compute_peso_neto(self):
        for record in self:
            # No se recorta en silencio a 0: si tara > bruto, el usuario debe
            # ver el error en action_update_line_from_input antes de llegar aquí.
            record.peso_neto = record.peso_bruto - record.tara

    def get_zpl_label(self):
        self.ensure_one()
        ref = self.product_id.default_code or ''
        name = self.product_id.name or ''
        lot = self.lot_id.name or ''
        qty = "{:.3f}".format(self.peso_neto)
        box = self.box_no or ''

        return f"^XA^CI28\n" \
               f"^CF0,50,50^FO50,50^FDREF: {ref}^FS\n" \
               f"^CF0,40,40^FO50,110^FB700,2,0,C^FD{name}^FS\n" \
               f"^FO50,210^FDLote: {lot}^FS\n" \
               f"^FO550,210^FDCant: {qty}^FS\n" \
               f"^FO50,270^FDN. CAJA: {box}^FS\n" \
               f"^FO030,350^BY3^BCN,100,Y,N,N^FD{lot}^FS\n" \
               f"^XZ"

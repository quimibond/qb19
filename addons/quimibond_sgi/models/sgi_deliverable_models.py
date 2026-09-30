# -*- coding: utf-8 -*-
"""57.65.0 (bloque 2 de formularios 6/6, inventario §3.4 y §5 #12): modelo de
Odoo en los entregables propios cuyo nombre dice sin duda dónde viven.

Cada renglón localiza el entregable por su código en la empresa del SGI. Solo
escribe «Modelo de Odoo» si está vacío (el valor anterior, vacío, y el nuevo
quedan en el log). Se salta el entregable si alguna actividad ya se mide con
él (poner modelo le cambiaría la medición) o si su campo de fecha o de
usuario no existe en el modelo. Idempotente. Nada se borra. El resto de los
entregables sin modelo queda para MAST (tabla en el CHANGELOG de 57.65.0).
"""
import logging

from odoo import api, models

_logger = logging.getLogger(__name__)

# código del entregable → modelo técnico
SGI_DELIVERABLE_MODELS = (
    ('C2-ACUSE', 'stock.picking'),        # Remisión firmada por el cliente
    ('C2-CITA', 'stock.picking'),         # Cita anotada en la entrega
    ('C2-CRUCE', 'stock.picking'),        # Cruce anotado en la entrega
    ('C2-PEDIMENTO', 'stock.picking'),    # Pedimento capturado en la entrega
    ('C2-TRANSPORTE', 'stock.picking'),   # Transporte anotado en la entrega
    ('C3-VIEJAS', 'mrp.production'),      # Órdenes abiertas > 30 días cerradas
    ('C4-PARAMETROS', 'mrp.production'),  # Parámetros registrados en la orden
    ('C4-TARJETA', 'mrp.production'),     # Tarjeta viajera impresa desde la orden
    ('C5-CONCESION', 'quality.alert'),    # Concesión adjunta a la NC
    ('C5-LIBERADO', 'quality.check'),     # Rollo o lote liberado por Calidad
    ('C5-PRUEBAS', 'quality.check'),      # Pruebas de laboratorio de la orden
    ('C6-DESCARGA', 'stock.picking'),     # Descargado y contado contra la remisión
    ('C6-QUIMICOS', 'stock.picking'),     # Químicos entregados a la cocina
    ('S1-FECHA', 'purchase.order'),       # Fecha confirmada en las órdenes atrasadas
    ('S2-CONTRARRECIBO', 'account.move'),  # Contrarrecibo adjunto a la factura
    ('S2-PORTAL', 'account.move'),        # Factura cargada en el portal del cliente
    ('S3-BLOQUEO', 'sgi.lock.date.log'),  # Periodo bloqueado en Odoo
    ('S3-CONCIL-BANCO', 'account.bank.statement.line'),  # Bancos conciliados
    ('S5-PREV-HECHO', 'maintenance.request'),  # Preventivo hecho
    ('S5-REPARACION', 'maintenance.request'),  # Falla reparada
)


class SgiDeliverableModels(models.Model):
    _inherit = 'sgi.deliverable'

    @api.model
    def _sgi_fill_evident_models(self, table=SGI_DELIVERABLE_MODELS, company=None):
        """Llena «Modelo de Odoo» en los entregables de ``table`` que lo tienen
        vacío. Devuelve ``{código: modelo}`` con lo escrito."""
        company = company or self.env['sgi.config']._sgi_company()
        Deliverable = self.sudo().with_context(active_test=False)
        written = {}
        for code, model_name in table:
            deliverable = Deliverable.search([('code', '=', code),
                                              ('company_id', '=', company.id)], limit=1)
            if not deliverable:
                _logger.warning("SGI modelo de entregables: no existe %s en %s; se salta.",
                                code, company.display_name)
                continue
            if deliverable.odoo_model_id:
                _logger.info("SGI modelo de entregables: %s ya tiene modelo %s; se respeta.",
                             code, deliverable.odoo_model_id.model)
                continue
            model = self.env['ir.model']._get(model_name)
            if not model or model_name not in self.env:
                _logger.warning("SGI modelo de entregables: %s: el modelo %s no está "
                                "instalado; se salta.", code, model_name)
                continue
            if deliverable.measured_activity_ids:
                _logger.warning("SGI modelo de entregables: %s: ya se miden con él %s; ponerle "
                                "modelo cambiaría su medición, se salta.", code,
                                ", ".join(deliverable.measured_activity_ids.mapped('display_name')))
                continue
            fields_ = self.env[model_name]._fields
            bad = [f for f in (deliverable.measure_date_field, deliverable.measure_user_field)
                   if f and f not in fields_]
            if bad:
                _logger.warning("SGI modelo de entregables: %s: %s no tiene %s; se salta.",
                                code, model_name, ", ".join(bad))
                continue
            try:
                with self.env.cr.savepoint():
                    deliverable.write({'odoo_model_id': model.id})
            except Exception:  # noqa: BLE001 - un entregable no detiene a los demás
                _logger.exception("SGI modelo de entregables: %s: no se pudo escribir %s.",
                                  code, model_name)
                continue
            written[code] = model_name
            _logger.info("SGI modelo de entregables: %s (%d) «%s»: modelo vacío → %s.",
                         code, deliverable.id, deliverable.name, model_name)
        _logger.info("SGI modelo de entregables: %d de %d en la tabla.", len(written), len(table))
        return written

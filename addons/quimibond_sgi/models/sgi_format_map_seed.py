# -*- coding: utf-8 -*-
"""57.61.0 (bloque 2 de formularios 2/6): la clave propia de cada orden de
producción, vale y transferencia de C4 (Producción) y C6 (Almacén e
inventarios), con los mapeos por criterio de 57.60.0.

Cada renglón localiza el formato por su clave vigente (los documentos del
Dropbox no tienen XML ID) y los tipos de operación por su nombre exacto en la
empresa del SGI (tampoco tienen XML ID: los creó Quimibond). Si falta el
formato, falta algún tipo o un nombre es ambiguo, el renglón se salta y queda
en el log. Nunca pisa: si ya hay un mapeo del mismo modelo con ese documento
o esa clave (activo o archivado), lo respeta. Idempotente. Nada se borra.

Solo entran los casos que se pudieron determinar con certeza en producción
(2026-09-30, MCP solo lectura); el resto queda para MAST (CHANGELOG 57.61.0).
"""
import logging

from odoo import api, models

_logger = logging.getLogger(__name__)

# (modelo, clave, nombres exactos de tipos de operación, filtro, prioridad, nota)
SGI_OPERATION_FORMAT_MAPS = (
    ('mrp.production', 'F-P-P02-01', ('Carda', 'V10-', 'V18'), '', 10,
     "Orden de trabajo de producción de entretelas: carda y líneas V10 "
     "(punteado y termofijado, IT-P-P01-03) y V18 (espolvoreo, IT-P-P01-04)"),
    ('mrp.production', 'F-IT-P-P01-02-02', ('Cocina Entretelas',), '', 10,
     "Orden de cocina de entretelas (IT-P-P01-02)"),
    ('mrp.production', 'F-IT-P-P01-12-01', ('Tintorería',), '', 10,
     "Orden de trabajo de teñido (IT-P-P01-12)"),
    ('stock.picking', 'F-IT-P-A07-01-02', ('Reetiquetado',), '', 10,
     "Orden de reetiquetado y empaque"),
    ('stock.picking', 'F-IT-P-A07-01-01',
     ('Salida de consumibles', 'Salida Refacciones a Gasto (MTTO)'), '', 10,
     "Requisición interna de refacciones y consumibles"),
    ('stock.picking', 'F-IT-P-A05-01-06', (),
     "[('location_dest_id.usage', '=', 'supplier')]", 20,
     "Devolución de materiales a proveedor (destino: ubicación de proveedor)"),
    ('stock.picking', 'F-P-A07-04', (),
     "[('location_id.usage', '=', 'customer')]", 20,
     "Control de devoluciones de cliente (origen: ubicación de cliente)"),
)


class SgiFormatMapSeed(models.Model):
    _inherit = 'sgi.format.map'

    @api.model
    def _sgi_seed_operation_maps(self, table=SGI_OPERATION_FORMAT_MAPS, company=None):
        """Crea los mapeos con criterio de ``table`` que falten. Devuelve la
        lista de claves creadas."""
        company = company or self.env['sgi.config']._sgi_company()
        Map = self.sudo().with_context(active_test=False)
        Doc = self.env['documents.document'].sudo()
        PType = self.env['stock.picking.type'].sudo().with_context(active_test=False)
        created = []
        for model_name, code, type_names, domain, sequence, note in table:
            model = self.env['ir.model']._get(model_name)
            if not model:
                _logger.warning("SGI formatos C4/C6: no existe el modelo %s; %s se salta.",
                                model_name, code)
                continue
            doc = Doc._sgi_find_by_code(code)
            if not doc:
                _logger.warning("SGI formatos C4/C6: no hay formato vigente con clave %s; "
                                "se salta.", code)
                continue
            twin = Map.search([('model_id', '=', model.id), '|',
                               ('document_id', '=', doc.id), ('sgi_code', '=', code)], limit=1)
            if twin:
                _logger.info("SGI formatos C4/C6: ya existe el mapeo %d de %s para %s (%s); "
                             "se respeta.", twin.id, code, model_name,
                             twin.selector_label or "sin criterio")
                continue
            types = PType.browse()
            missing = []
            for name in type_names:
                found = PType.search([('name', '=', name), ('company_id', '=', company.id)])
                if len(found) == 1:
                    types |= found
                else:
                    missing.append("%s (%d)" % (name, len(found)))
            if missing:
                _logger.warning("SGI formatos C4/C6: %s: tipos de operación no encontrados o "
                                "ambiguos en %s: %s; se salta.", code, company.display_name,
                                ", ".join(missing))
                continue
            fmap = Map.create({
                'model_id': model.id,
                'document_id': doc.id,
                'sgi_code': doc.sgi_code,
                'picking_type_ids': [(6, 0, types.ids)],
                'record_domain': domain or False,
                'sequence': sequence,
                'note': note,
            })
            created.append(code)
            _logger.info("SGI formatos C4/C6: mapeo %d creado: %s → %s (%s).", fmap.id, code,
                         model_name, fmap.selector_label)
        _logger.info("SGI formatos C4/C6: %d mapeo(s) creados de %d en la tabla.",
                     len(created), len(table))
        return created

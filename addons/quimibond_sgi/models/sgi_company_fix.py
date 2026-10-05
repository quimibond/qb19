# -*- coding: utf-8 -*-
"""57.95.0 (auditoría 2026-10, hallazgo de datos D-06 «Datos sin compañía»;
no es la decisión D-06 de incidentes de docs/audit/decisiones.md).

Asistente del Jefe MAST que pone la empresa del SGI en los documentos
controlados sin empresa (492 el 2026-10-02). Es un cambio de datos de
negocio: no va en migración; el Jefe MAST lo corre a mano con el visto bueno
de Jose. Al abrirlo solo cuenta; «Asignar» escribe en lotes, deja una nota en
cada documento y registra antes y después en el log. Las rutinas del
procedimiento anterior toman la empresa solas (``related`` guardado).
Idempotente; nada se borra."""
import logging

from markupsafe import Markup

from odoo import api, fields, models
from odoo.exceptions import AccessError

_logger = logging.getLogger(__name__)
BATCH = 100


class SgiCompanyFix(models.TransientModel):
    """Empresa del SGI en los documentos controlados que no tienen empresa (D-06 de datos)."""
    _name = 'sgi.company.fix'
    _description = "Empresa del SGI en documentos controlados"

    company_id = fields.Many2one('res.company', string="Empresa del SGI",
                                 compute='_compute_counts')
    doc_count = fields.Integer(string="Documentos controlados sin empresa", compute='_compute_counts')
    vigente_count = fields.Integer(string="Vigentes", compute='_compute_counts')
    obsoleto_count = fields.Integer(string="Obsoletos", compute='_compute_counts')
    other_count = fields.Integer(string="En borrador o piloto", compute='_compute_counts')
    routine_count = fields.Integer(
        string="Rutinas del procedimiento anterior que toman la empresa", compute='_compute_counts')
    done_count = fields.Integer(string="Documentos corregidos", readonly=True)
    failed_count = fields.Integer(
        string="Documentos que no se pudieron corregir", readonly=True,
        help="Se quedaron sin empresa; el motivo de cada uno está en el log del servidor.")

    @api.model
    def _sgi_check_manager(self):
        if not (self.env.su or self.env.user.has_group('quimibond_sgi.group_sgi_manager')):
            raise AccessError("Solo el Jefe MAST asigna la empresa del SGI a los documentos "
                              "controlados.")

    @api.model
    def _sgi_loose_docs(self):
        return self.env['documents.document'].sudo().with_context(active_test=False).search(
            [('sgi_is_controlled', '=', True), ('company_id', '=', False)], order='id')

    @api.model_create_multi
    def create(self, vals_list):
        self._sgi_check_manager()
        return super().create(vals_list)

    def _compute_counts(self):
        docs = self._sgi_loose_docs()
        routines = self.env['sgi.legacy.routine'].sudo().with_context(active_test=False).search_count(
            [('procedure_id', 'in', docs.ids)]) if docs else 0
        company = self.env['sgi.config']._sgi_company()
        vigente = len(docs.filtered(lambda d: d.sgi_state == 'vigente'))
        obsoleto = len(docs.filtered(lambda d: d.sgi_state == 'obsoleto'))
        for wiz in self:
            wiz.company_id = company
            wiz.doc_count = len(docs)
            wiz.vigente_count = vigente
            wiz.obsoleto_count = obsoleto
            wiz.other_count = len(docs) - vigente - obsoleto
            wiz.routine_count = routines

    def action_view_docs(self):
        self._sgi_check_manager()
        return {
            'type': 'ir.actions.act_window', 'name': "Documentos controlados sin empresa",
            'res_model': 'documents.document', 'view_mode': 'list,form',
            'views': [(False, 'list'), (False, 'form')],
            'domain': [('id', 'in', self._sgi_loose_docs().ids)], 'target': 'current',
        }

    def action_apply(self):
        self.ensure_one()
        self._sgi_check_manager()
        company = self.env['sgi.config']._sgi_company()
        docs = self._sgi_loose_docs()
        who = self.env.user.display_name
        _logger.info("SGI D-06: antes, %d documentos controlados sin empresa: %s", len(docs), docs.ids)
        body = Markup("Empresa del SGI asignada: <b>%s</b> (antes sin empresa). "
                      "D-06, 57.95.0; la aplicó %s.") % (company.display_name, who)

        def _fix(records):
            # Escribir company_id corre _check_sgi_revision_unique (clave +
            # revisión por empresa): un controlado sin empresa que repite la
            # clave y revisión de uno de PNTQ no puede pasar.
            records.write({'company_id': company.id})
            for doc in records:
                doc.message_post(body=body, subtype_xmlid='mail.mt_note')

        done = failed = self.env['documents.document']
        for start in range(0, len(docs), BATCH):
            batch = docs[start:start + BATCH]
            try:
                with self.env.cr.savepoint():
                    _fix(batch)
                done |= batch
                continue
            except Exception:
                _logger.warning("SGI D-06: falló el lote %s; se reintenta documento por documento.",
                                batch.ids, exc_info=True)
            # Un documento malo no detiene a los otros 99 del lote.
            for doc in batch:
                try:
                    with self.env.cr.savepoint():
                        _fix(doc)
                    done |= doc
                except Exception:
                    # WARNING con traza, no ERROR: el log del servidor lo
                    # guarda y el asistente lo cuenta en «no se pudieron».
                    failed |= doc
                    _logger.warning("SGI D-06: el documento %s (%s) se queda sin empresa.",
                                    doc.id, doc.sgi_code or doc.name, exc_info=True)
        left = len(self._sgi_loose_docs())
        _logger.info("SGI D-06: después, %d documentos con la empresa %s, %d con error %s y %d "
                     "sin empresa.", len(done), company.display_name, len(failed), failed.ids, left)
        self.write({'done_count': len(done), 'failed_count': len(failed)})
        return {'type': 'ir.actions.act_window', 'res_model': self._name, 'res_id': self.id,
                'view_mode': 'form', 'target': 'new'}

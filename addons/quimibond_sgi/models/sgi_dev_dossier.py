# -*- coding: utf-8 -*-
"""57.137.0 (revisión de Administración de Ventas 2026-10-08, puntos 3 y final): la **ruta a la vista** y el **expediente de
diseño y desarrollo** (ISO 9001:2015 8.3 / APQP).

- **Ruta.** La ruta son las operaciones de la lista de materiales del artículo (C1.04b, Diseño de Procesos). Aquí
  se ve dentro del proyecto como una cadena de cajas (artículo · operación · centro de trabajo) y
  se edita en la lista de materiales con «Editar ruta». El diagrama de flujo se imprime de ahí.
- **Expediente.** Un semáforo por requisito del punto 8.3 (planificación, entradas, controles:
  revisión / verificación / validación, salidas, cambios) y por fase del APQP, con la evidencia que
  ya existe en el proyecto; se imprime como «Expediente de diseño y desarrollo». El núcleo no conoce
  el cotizador: ``_sgi_dev_dossier_extra`` es el gancho para que ``qb_costeo_sgi`` agregue la
  cotización aprobada y el precio en tarifa.
"""
from markupsafe import Markup, escape

from odoo import fields, models
from odoo.exceptions import UserError


class ProjectProjectDevDossier(models.Model):
    _inherit = 'project.project'

    sgi_dev_route_html = fields.Html(string="Ruta del producto", compute='_compute_sgi_dev_route_html', sanitize=False)
    sgi_dev_route_count = fields.Integer(string="Operaciones de la ruta", compute='_compute_sgi_dev_route_html')
    sgi_dev_dossier_html = fields.Html(string="Expediente ISO 9001 8.3 / APQP", compute='_compute_sgi_dev_dossier_html',
                                       sanitize=False)
    sgi_dev_dossier_done = fields.Integer(string="Requisitos cubiertos", compute='_compute_sgi_dev_dossier_html')
    sgi_dev_dossier_total = fields.Integer(string="Requisitos del expediente", compute='_compute_sgi_dev_dossier_html')

    # ------------------------------------------------------------------
    # Ruta
    # ------------------------------------------------------------------
    def _compute_sgi_dev_route_html(self):
        for project in self:
            ops = project._sgi_dev_route_operations() if project.sgi_is_ft else []
            project.sgi_dev_route_count = len(ops)
            if not ops:
                project.sgi_dev_route_html = Markup(
                    '<p class="text-muted mb-0">Sin ruta todavía: genere los artículos y capture las operaciones '
                    'con su centro de trabajo en la lista de materiales («Editar ruta»).</p>')
                continue
            boxes = []
            for product, op in ops:
                boxes.append(
                    '<div style="display:inline-block; border:1px solid #adb5bd; border-radius:6px; padding:6px 10px; '
                    'margin:4px 0; background:#f8f9fa; text-align:center; min-width:120px;">'
                    '<div style="font-size:11px; color:#6c757d;">%s</div><div><b>%s</b></div>'
                    '<div style="font-size:11px;">%s</div></div>' % (
                        escape(product.default_code or product.display_name), escape(op.name or ''),
                        escape(op.workcenter_id.name or 'sin centro de trabajo')))
            arrow = '<span style="font-size:20px; margin:0 6px; color:#6c757d;">→</span>'
            project.sgi_dev_route_html = Markup('<div style="white-space:normal;">%s</div>' % arrow.join(boxes))

    def action_sgi_dev_open_route(self):
        """Abre la lista de materiales del artículo acabado (o del primero que exista) para capturar
        la ruta: operaciones y centros de trabajo."""
        self.ensure_one()
        Bom = self.env['mrp.bom']
        for product in (self.sgi_dev_product_id | self.sgi_dev_product_tenido_id | self.sgi_dev_product_crudo_id):
            bom = Bom._bom_find(product, company_id=self.company_id.id).get(product)
            if bom:
                return {'type': 'ir.actions.act_window', 'res_model': 'mrp.bom', 'res_id': bom.id,
                        'view_mode': 'form', 'target': 'current'}
        if not self.sgi_dev_product_id:
            raise UserError("Genere los artículos del desarrollo antes de capturar la ruta (pestaña «Cotización»).")
        return {'type': 'ir.actions.act_window', 'res_model': 'mrp.bom', 'view_mode': 'form', 'target': 'current',
                'context': {'default_product_tmpl_id': self.sgi_dev_product_id.product_tmpl_id.id,
                            'default_company_id': self.company_id.id}}

    # ------------------------------------------------------------------
    # Expediente 8.3 / APQP
    # ------------------------------------------------------------------
    def _sgi_dev_dossier_extra(self):
        """[(clausula, requisito, cumplido, detalle)] que agregan otros módulos (cotización, tarifa)."""
        self.ensure_one()
        return []

    def _sgi_dev_dossier_rows(self):
        """Semáforo del expediente: una fila por requisito de ISO 9001:2015 8.3 con la evidencia del
        proyecto. ``cumplido`` es True, False o None (no aplica todavía)."""
        self.ensure_one()
        lines = self.sgi_dev_line_ids
        spec_lines = lines.filtered('in_customer_spec')
        sample_lines = lines.filtered(lambda l: l.sample_value or l.sample_text)
        run_lines = lines.filtered(lambda l: l.run_count or l.run_text)
        ops = self._sgi_dev_route_operations()
        pilots = self.sgi_dev_pilot_ids
        closed_pilots = pilots.filtered(lambda p: p.state == 'cerrado')
        sheets = self.sgi_dev_tech_sheet_ids.filtered(lambda s: s.state == 'vigente')
        specs = self.sgi_dev_customer_spec_ids.filtered(lambda s: s.state == 'vigente')
        coas = self.sgi_dev_coa_ids.filtered(lambda c: c.state == 'emitido')
        shipments = self.sgi_dev_shipment_ids
        responses = shipments.filtered(lambda s: s.state == 'respondida')
        changes = self.sgi_dev_change_request_ids
        signed = changes.filtered(lambda c: c.state in ('firmada', 'aplicada'))
        folio = self.sgi_ft_folio
        rows = [
            ('8.3.2', "Planificación: etapas del desarrollo con responsable y reloj",
             bool(self.sgi_dev_stage_log_ids), "%d etapa(s) registradas" % len(self.sgi_dev_stage_log_ids)),
            ('8.3.2', "Planificación: folio FT del proyecto", bool(folio), folio or "sin folio"),
            ('8.3.3', "Entradas: requisitos del cliente en la tabla de características",
             bool(spec_lines), "%d característica(s) con especificación del cliente" % len(spec_lines)),
            ('8.3.3', "Entradas: uso del producto y referencia a la especificación del cliente",
             bool((self.sgi_dev_use or '').strip()), (self.sgi_dev_use or '')[:80] or "sin uso capturado"),
            ('8.3.3', "Entradas: muestra física o especificación recibida",
             bool(self.sgi_dev_customer_property and self.sgi_dev_customer_property != 'ninguna'),
             dict(self._fields['sgi_dev_customer_property'].selection).get(self.sgi_dev_customer_property) or "sin registro"),
            ('8.3.3', "Entradas: normas y requisitos legales",
             bool((self.sgi_dev_norms or '').strip() or self.sgi_dev_legal_ids),
             ", ".join(filter(None, [self.sgi_dev_norms] + self.sgi_dev_legal_ids.mapped('name'))) or "sin capturar"),
            ('8.3.3', "Entradas: análisis de la muestra del cliente (mediciones)",
             bool(sample_lines), "%d característica(s) medidas en la muestra" % len(sample_lines)),
            ('8.3.3', "Entradas: productos parecidos revisados (artículo base)",
             bool(self.sgi_dev_base_product_id) if self.sgi_dev_analysis_result != 'no_factible' else None,
             self.sgi_dev_base_product_id.display_name or "sin artículo base"),
            ('8.3.4', "Controles: revisión de factibilidad aprobada (Ventas, checklist)",
             self.sgi_dev_review_state == 'aprobado',
             dict(self._fields['sgi_dev_review_state'].selection).get(self.sgi_dev_review_state) or ''),
            ('8.3.4', "Controles: aprobación del cliente para iniciar registrada con evidencia",
             bool(self.sgi_dev_customer_approved_at),
             ("%s, %s" % (dict(self._fields['sgi_dev_customer_approval_medium'].selection).get(self.sgi_dev_customer_approval_medium),
                          self.sgi_dev_customer_approval_date)) if self.sgi_dev_customer_approved_at else "pendiente"),
            ('8.3.4', "Controles: solicitud de desarrollo aprobada por Dirección de Operaciones",
             bool(self.sgi_dev_approved_by_id),
             ("%s, %s" % (self.sgi_dev_approved_by_id.name, self.sgi_dev_approved_date)) if self.sgi_dev_approved_by_id else "pendiente"),
            ('8.3.4', "Verificación: corrida de muestra con resultados contra la especificación",
             bool(run_lines), "%d característica(s) con resultado de corrida" % len(run_lines)),
            ('8.3.4', "Verificación: respuesta del cliente a la muestra",
             bool(responses), "%d respuesta(s) registrada(s)" % len(responses)),
            ('8.3.4', "Verificación: pilotaje cerrado con estudio de habilidad",
             bool(closed_pilots), "%d pilotaje(s) cerrado(s) de %d" % (len(closed_pilots), len(pilots))),
            ('8.3.4', "Validación: ficha técnica interna vigente (C1.15)",
             bool(sheets), "%d ficha(s) vigente(s)" % len(sheets)),
            ('8.3.5', "Salidas: artículos del desarrollo dados de alta",
             bool(self.sgi_dev_product_id), self.sgi_dev_product_id.display_name or "sin artículo"),
            ('8.3.5', "Salidas: ruta con centros de trabajo",
             bool(ops) and all(op.workcenter_id for _p, op in ops), "%d operación(es)" % len(ops)),
            ('8.3.5', "Salidas: especificaciones del producto para el cliente",
             bool(specs), "%d especificación(es) vigente(s)" % len(specs)),
            ('8.3.5', "Salidas: reporte de conformidad emitido",
             bool(coas), "%d certificado(s)" % len(coas)),
            ('8.3.6', "Cambios: solicitudes de modificación firmadas por Dirección",
             bool(signed) if changes else None, "%d firmada(s) de %d" % (len(signed), len(changes))),
            ('8.3.6', "Cambios: bitácora de revisiones",
             bool(self.sgi_dev_revision_ids) if self.sgi_dev_revision else None,
             "revisión %d, %d renglón(es)" % (self.sgi_dev_revision, len(self.sgi_dev_revision_ids))),
        ]
        rows += self._sgi_dev_dossier_extra()
        return rows

    APQP_PHASES = [
        ("Fase 1 · Planeación", "Análisis de mercado industrial, análisis de proyecto"),
        ("Fase 2 · Diseño del producto", "Cotización, aprobación del cliente, solicitud de desarrollo"),
        ("Fase 3 · Diseño del proceso", "Ruta y centros de trabajo, fichas de proceso"),
        ("Fase 4 · Validación", "Muestra, envío, retroalimentación, pilotaje y estudio de habilidad"),
        ("Fase 5 · Retroalimentación y mejora", "Cambios al proyecto, liberación, precio en tarifa"),
    ]

    def _compute_sgi_dev_dossier_html(self):
        for project in self:
            if not project.sgi_is_ft:
                project.sgi_dev_dossier_html = False
                project.sgi_dev_dossier_done = project.sgi_dev_dossier_total = 0
                continue
            rows = project._sgi_dev_dossier_rows()
            applicable = [r for r in rows if r[2] is not None]
            done = [r for r in applicable if r[2]]
            project.sgi_dev_dossier_total = len(applicable)
            project.sgi_dev_dossier_done = len(done)
            html = ['<table class="table table-sm" style="font-size:12px;"><thead><tr><th>8.3</th><th>Requisito</th>'
                    '<th>Evidencia</th><th></th></tr></thead><tbody>']
            for clause, label, ok, detail in rows:
                mark = ('<span class="text-success">✔</span>' if ok else
                        '<span class="text-danger">✘</span>' if ok is False else '<span class="text-muted">—</span>')
                html.append('<tr><td>%s</td><td>%s</td><td class="text-muted">%s</td><td>%s</td></tr>'
                            % (escape(clause), escape(label), escape(detail or ''), mark))
            html.append('</tbody></table>')
            project.sgi_dev_dossier_html = Markup(''.join(html))

    def action_sgi_dev_print_dossier(self):
        self.ensure_one()
        return self.env.ref('quimibond_sgi.action_report_dev_dossier').report_action(self)

# -*- coding: utf-8 -*-
import base64
from dateutil.relativedelta import relativedelta

from odoo import models, fields, api


class SgiCron(models.AbstractModel):
    _name = 'sgi.cron'
    _description = "Tareas programadas SGI"

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @api.model
    def _sgi_activity_exists(self, record, summary, user_id):
        """Evita duplicar actividades (idempotencia)."""
        activity_type = self.env.ref('mail.mail_activity_data_todo')
        return bool(self.env['mail.activity'].search_count([
            ('res_model', '=', record._name),
            ('res_id', '=', record.id),
            ('summary', '=', summary),
            ('user_id', '=', user_id),
            ('activity_type_id', '=', activity_type.id),
        ]))

    @api.model
    def _sgi_schedule(self, record, summary, note, user_id):
        if not user_id:
            return
        if self._sgi_activity_exists(record, summary, user_id):
            return
        record.activity_schedule(
            'mail.mail_activity_data_todo',
            summary=summary, note=note or '', user_id=user_id)

    def _sgi_manager_user_id(self):
        group = self.env.ref('quimibond_sgi.group_sgi_manager', raise_if_not_found=False)
        return group.users[:1].id if group and group.users else False

    # ------------------------------------------------------------------
    # 1. Cron diario — No Conformidades
    # ------------------------------------------------------------------
    @api.model
    def cron_nonconformities(self):
        today = fields.Date.context_today(self)
        Param = self.env['ir.config_parameter'].sudo()
        default_days = int(Param.get_param('quimibond_sgi.nc_escalation_days', 5))

        # Marca acciones vencidas (recomputo del store)
        self.env['sgi.action.line'].search([('date_done', '=', False)])._compute_state()

        open_alerts = self.env['quality.alert'].search([
            ('team_id.sgi_sequence_id', '!=', False),
            ('stage_id.sgi_is_closing_stage', '=', False),
            ('stage_id.sgi_is_cancel_stage', '=', False),
        ])
        for alert in open_alerts:
            days = 3 if alert.sgi_origin_type in ('auditoria_externa', 'reclamacion') else default_days
            deadline = fields.Datetime.to_datetime(alert.create_date).date() + relativedelta(days=days)
            no_action = not alert.sgi_action_line_ids.filtered(lambda l: l.progress != '0')

            # Escalamiento por inacción
            if no_action and today >= deadline:
                user_id = alert.user_id.id or self._sgi_manager_user_id()
                self._sgi_schedule(
                    alert,
                    "NC sin acción: %s" % (alert.sgi_folio or alert.name),
                    "La NC lleva más de %d días abierta sin acción registrada." % days,
                    user_id)

            # Verificación de eficacia pendiente
            all_done = alert.sgi_action_line_ids and all(l.date_done for l in alert.sgi_action_line_ids)
            if all_done and not alert.sgi_effectiveness_date:
                user_id = alert.sgi_effectiveness_by.id or alert.user_id.id or self._sgi_manager_user_id()
                self._sgi_schedule(
                    alert,
                    "Verificar eficacia: %s" % (alert.sgi_folio or alert.name),
                    "Todas las acciones terminaron; falta registrar la verificación de eficacia.",
                    user_id)
        return True

    # ------------------------------------------------------------------
    # 2. Cron diario — Documentos
    # ------------------------------------------------------------------
    @api.model
    def cron_documents(self):
        today = fields.Date.context_today(self)
        Doc = self.env['documents.document']

        # Revisión bienal: avisos a 60 y 30 días
        for offset in (60, 30):
            target = today + relativedelta(days=offset)
            docs = Doc.search([
                ('sgi_state', '=', 'vigente'),
                ('sgi_next_review_date', '=', target),
            ])
            for doc in docs:
                self._sgi_schedule(
                    doc,
                    "Revisión bienal en %d días: %s" % (offset, doc.sgi_code or doc.name),
                    "El documento requiere revisión antes de %s." % doc.sgi_next_review_date,
                    doc.sgi_owner_id.id or self._sgi_manager_user_id())

        # Pilotos que vencen en 7 días
        pilot_target = today + relativedelta(days=7)
        pilots = Doc.search([
            ('sgi_state', '=', 'piloto'),
            ('sgi_pilot_end_date', '=', pilot_target),
        ])
        for doc in pilots:
            self._sgi_schedule(
                doc,
                "Piloto por vencer: %s" % (doc.sgi_code or doc.name),
                "La prueba piloto vence el %s." % doc.sgi_pilot_end_date,
                doc.sgi_owner_id.id or self._sgi_manager_user_id())

        # Acuses pendientes > 7 días
        limit_date = fields.Datetime.now() - relativedelta(days=7)
        acks = self.env['sgi.document.ack'].search([
            ('state', '=', 'pendiente'),
            ('create_date', '<=', limit_date),
        ])
        manager_id = self._sgi_manager_user_id()
        for ack in acks:
            user_id = ack.user_id.id or manager_id
            self._sgi_schedule(
                ack.document_id,
                "Acuse pendiente: %s" % (ack.employee_id.name),
                "El acuse de lectura lleva más de 7 días pendiente.",
                user_id)
        return True

    # ------------------------------------------------------------------
    # 3. Cron mensual — NEWS (F-P-G01-16)
    # ------------------------------------------------------------------
    @api.model
    def cron_news(self):
        today = fields.Date.context_today(self)
        first_this_month = today.replace(day=1)
        first_prev_month = first_this_month - relativedelta(months=1)

        requests = self.env['approval.request'].search([
            ('sgi_is_doc_change', '=', True),
            ('request_status', '=', 'approved'),
            ('sgi_applied', '=', True),
            ('write_date', '>=', fields.Datetime.to_datetime(first_prev_month)),
            ('write_date', '<', fields.Datetime.to_datetime(first_this_month)),
        ])
        if not requests:
            return True

        report = self.env.ref('quimibond_sgi.action_report_news', raise_if_not_found=False)
        manager_id = self._sgi_manager_user_id()
        # La actividad se agenda sobre una approval.request (tiene mail.activity.mixin);
        # res.company NO hereda el mixin.
        anchor = requests[:1]
        if report and anchor:
            pdf_content, _ = self.env['ir.actions.report']._render_qweb_pdf(
                report.report_name, requests.ids)
            attachment = self.env['ir.attachment'].create({
                'name': "NEWS_%s.pdf" % first_prev_month.strftime('%Y_%m'),
                'type': 'binary',
                'datas': base64.b64encode(pdf_content),
                'res_model': 'approval.request',
                'res_id': anchor.id,
                'mimetype': 'application/pdf',
            })
            if manager_id:
                anchor.activity_schedule(
                    'mail.mail_activity_data_todo',
                    summary="Revisar y difundir NEWS %s" % first_prev_month.strftime('%m/%Y'),
                    note="Boletín de cambios documentales aprobados del mes (adjunto ID %d)." % attachment.id,
                    user_id=manager_id)
        return True

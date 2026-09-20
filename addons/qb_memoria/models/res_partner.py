# -*- coding: utf-8 -*-
"""Pestaña Memoria del contacto: lo que la memoria de correo sabe de la
empresa. Se resuelve por el partner comercial (``companies.odoo_partner_id``)
y, si no, por RFC. Dos fuentes:

* la **ficha consolidada** (RPC ``memoria_brief``): quién la atiende, lo que
  sabemos (hechos con vigencia), conversaciones resumidas por Claude con su
  estado y pendientes, contactos del grafo;
* las tablas crudas (hilos, pendientes detectados, demanda, contactos) como
  hasta ahora."""
import logging
from datetime import timedelta

from markupsafe import Markup, escape
from psycopg2.extras import Json

from odoo import _, fields, models
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

GMAIL_THREAD_URL = 'https://mail.google.com/mail/u/0/#all/%s'


class ResPartner(models.Model):
    _inherit = 'res.partner'

    memoria_cache = fields.Json(string='Memoria (caché)', copy=False)
    memoria_cached_at = fields.Datetime(string='Memoria actualizada', copy=False)
    memoria_html = fields.Html(string='Memoria', compute='_compute_memoria_html', sanitize=False)

    MEMORIA_TTL_HOURS = 6

    # ── lectura ──────────────────────────────────────────────────────

    def _memoria_partner(self):
        self.ensure_one()
        return self.commercial_partner_id or self

    def _memoria_company_row(self, client, partner):
        select = ('id,name,odoo_partner_id,rfc,relationship_summary,strategic_notes,risk_signals,'
                  'opportunity_signals,monthly_avg,trend_pct,payment_term,total_receivable,total_overdue_odoo')
        rows = client.get('companies', {'select': select, 'odoo_partner_id': 'eq.%s' % partner.id, 'limit': 1})
        if not rows and partner.vat:
            rows = client.get('companies', {'select': select, 'rfc': 'eq.%s' % partner.vat.strip().upper(), 'limit': 1})
        return rows[0] if rows else None

    def _memoria_fetch(self):
        self.ensure_one()
        client = self.env['qb.memoria.client']
        partner = self._memoria_partner()
        company = self._memoria_company_row(client, partner)
        data = {'company': company, 'brief': None, 'threads': [], 'threads_90d': 0, 'waiting_us': 0,
                'pending': [], 'demand': [], 'contacts': []}
        if not company:
            return data
        cid = company['id']
        try:
            data['brief'] = client.rpc('memoria_brief', {'p_company_id': cid}) or None
        except UserError as exc:  # la ficha consolidada es opcional: el resto sigue
            _logger.warning('memoria_brief %s: %s', cid, exc)
            data['brief_error'] = str(exc)
        since = (fields.Date.today() - timedelta(days=90)).isoformat()
        data['threads'] = client.get('threads', {
            'select': 'id,gmail_thread_id,subject,account,status,message_count,last_sender_type,'
                      'hours_without_response,last_activity',
            'company_id': 'eq.%s' % cid, 'order': 'last_activity.desc', 'limit': 15})
        recent = client.get('threads', {'select': 'id,status', 'company_id': 'eq.%s' % cid,
                                        'last_activity': 'gte.%s' % since, 'limit': 1000})
        data['threads_90d'] = len(recent)
        data['waiting_us'] = len([t for t in recent if t.get('status') == 'waiting_us'])
        data['pending'] = client.get('email_pending_actions', {
            'select': 'id,thread_id,tipo,descripcion,deadline,account,status,detected_at,resolved_at',
            'company_id': 'eq.%s' % cid, 'order': 'detected_at.desc', 'limit': 20})
        data['demand'] = client.get('customer_demand_signals', {
            'select': 'product_ref,product_desc,qty,uom,period_label,demand_date,detected_at',
            'company_id': 'eq.%s' % cid, 'order': 'detected_at.desc', 'limit': 10})
        data['contacts'] = client.get('contacts', {
            'select': 'name,email,role,department,last_activity,interaction_count,avg_response_time_hours,'
                      'relationship_score',
            'company_id': 'eq.%s' % cid, 'order': 'last_activity.desc.nullslast', 'limit': 12})
        return data

    def _memoria_store(self, data):
        """Guarda la caché sin pasar por write(): se llama también desde el
        compute, al abrir el contacto."""
        now = fields.Datetime.now()
        self.env.cr.execute(
            "UPDATE res_partner SET memoria_cache = %s, memoria_cached_at = %s WHERE id = %s",
            (Json(data), now, self.id))
        self.invalidate_recordset(['memoria_cache', 'memoria_cached_at', 'memoria_html'])

    def _memoria_refresh_one(self):
        self.ensure_one()
        try:
            data = self._memoria_fetch()
        except Exception as exc:  # noqa: BLE001 — la ficha nunca revienta por la memoria
            _logger.warning('qb_memoria: contacto %s: %s', self.id, exc)
            data = {'error': str(exc)[:500]}
        self._memoria_store(data)
        return data

    def action_memoria_refresh(self):
        for partner in self:
            partner._memoria_refresh_one()
        return True

    # ── render ───────────────────────────────────────────────────────

    def _compute_memoria_html(self):
        ttl = timedelta(hours=self.MEMORIA_TTL_HOURS)
        now = fields.Datetime.now()
        for partner in self:
            if not partner.id:
                partner.memoria_html = False
                continue
            stale = not partner.memoria_cached_at or (now - partner.memoria_cached_at) > ttl
            if stale and not self.env.context.get('memoria_no_fetch'):
                partner._memoria_refresh_one()
            partner.memoria_html = partner._memoria_render(partner.memoria_cache or {}, partner.memoria_cached_at)

    @staticmethod
    def _dt(value):
        return (value or '')[:16].replace('T', ' ')

    @staticmethod
    def _num(value):
        try:
            return '{:,.2f}'.format(float(value))
        except (TypeError, ValueError):
            return ''

    ESTADO_BADGE = {'abierto': 'text-bg-warning', 'cerrado': 'text-bg-success', 'informativo': 'text-bg-secondary'}
    ESPERANDO = {'nosotros': 'esperan respuesta nuestra', 'ellos': 'esperamos a ellos', 'nadie': ''}
    CATEGORIA = {'condiciones_pago': 'Condiciones de pago', 'precio': 'Precios', 'producto': 'Producto',
                 'logistica': 'Logística', 'calidad': 'Calidad', 'contacto_clave': 'Contactos clave',
                 'proceso': 'Proceso', 'preferencia': 'Preferencias', 'riesgo': 'Riesgos', 'otro': 'Otros'}

    def _memoria_render_brief(self, brief):
        """Secciones de la ficha consolidada (memoria_brief): quién atiende,
        lo que sabemos, conversaciones resumidas."""
        e = escape
        parts = []
        if not brief:
            return parts
        # Quién atiende
        enc = [x for x in (brief.get('encargados') or []) if x.get('buzon')]
        if enc:
            items = Markup('').join(Markup('<li><b>%s</b>%s: %s correos (%s %%)</li>') % (
                e(x.get('buzon') or ''), (' · %s' % e(x['area'])) if x.get('area') else '',
                self._num(x.get('n')), x.get('share') if x.get('share') is not None else '—') for x in enc[:8])
            parts.append(Markup('<h5>%s</h5><ul class="mb-2">%s</ul>') % (_('Quién la atiende (según el correo)'), items))
        # Lo que sabemos
        facts = brief.get('hechos') or []
        if facts:
            by_cat = {}
            for f in facts:
                by_cat.setdefault(f.get('categoria') or 'otro', []).append(f)
            blocks = []
            for cat, rows in by_cat.items():
                lis = Markup('').join(Markup('<li>%s%s%s</li>') % (
                    e(f.get('hecho') or ''),
                    Markup(' <span class="text-muted small">(%s)</span>') % e(f['sobre']) if f.get('sobre') and f['sobre'] != 'empresa' else '',
                    Markup(' <span class="text-muted small">desde %s</span>') % e(f['vigente_desde']) if f.get('vigente_desde') else '')
                    for f in rows[:8])
                blocks.append(Markup('<div class="col-12 col-md-6"><b>%s</b><ul class="mb-2">%s</ul></div>') % (
                    self.CATEGORIA.get(cat, cat), lis))
            parts.append(Markup('<h5>%s</h5><div class="row">%s</div>') % (_('Lo que sabemos'), Markup('').join(blocks)))
        # Conversaciones resumidas
        hilos = brief.get('hilos') or []
        if hilos:
            cards = []
            for h in hilos[:12]:
                pend = h.get('pendientes') or []
                pend_html = Markup('<ul class="mb-1 small">%s</ul>') % Markup('').join(Markup('<li>%s%s%s</li>') % (
                    e(p.get('que') or ''), ' (%s)' % e(p['quien']) if p.get('quien') else '',
                    ' · vence %s' % e(p['vence']) if p.get('vence') else '') for p in pend[:4]) if pend else ''
                cards.append(Markup(
                    '<div class="border rounded p-2 mb-2">'
                    '<div class="d-flex justify-content-between"><b>%s</b>'
                    '<span><span class="badge %s">%s</span> <span class="text-muted small">%s</span></span></div>'
                    '<div class="text-muted small">%s · %s · %s msjs · <a href="%s" target="_blank">%s</a></div>'
                    '<p class="mb-1">%s</p>%s</div>') % (
                    e(h.get('tema') or h.get('asunto') or _('(sin tema)')),
                    self.ESTADO_BADGE.get(h.get('estado'), 'text-bg-light'), e(h.get('estado') or ''),
                    self.ESPERANDO.get(h.get('esperando_a') or '', ''),
                    self._dt(h.get('ultimo')), e(h.get('buzon') or ''), h.get('mensajes') or 0,
                    GMAIL_THREAD_URL % e(h.get('gmail_thread_id') or ''), e(h.get('asunto') or ''),
                    e(h.get('resumen') or ''), pend_html))
            st = brief.get('stats') or {}
            parts.append(Markup('<h5>%s <span class="text-muted small">%s</span></h5>%s') % (
                _('Conversaciones (resumen de la memoria)'),
                _('%s resumidas, %s abiertas') % (st.get('hilos_resumidos', len(hilos)), st.get('hilos_abiertos', 0)),
                Markup('').join(cards)))
        return parts

    def _memoria_render(self, data, cached_at):
        e = escape
        if not data:
            return Markup('<p class="text-muted">%s</p>') % _('Sin memoria cargada. Pulsa Actualizar.')
        if data.get('error'):
            return Markup('<p class="text-danger">%s %s</p>') % (_('No se pudo leer la memoria:'), data['error'])
        company = data.get('company')
        if not company:
            return Markup('<p class="text-muted">%s</p>') % _(
                'La memoria no conoce a esta empresa (sin correo ligado por partner ni por RFC).')
        parts = []
        # Resumen
        threads = data.get('threads') or []
        last = threads[0] if threads else None
        parts.append(Markup(
            '<div class="row mb-2">'
            '<div class="col-6 col-md-3"><div class="text-muted small">%s</div><div class="h5 mb-0">%s</div></div>'
            '<div class="col-6 col-md-3"><div class="text-muted small">%s</div><div class="h5 mb-0">%s</div></div>'
            '<div class="col-6 col-md-3"><div class="text-muted small">%s</div><div class="h5 mb-0">%s</div></div>'
            '<div class="col-6 col-md-3"><div class="text-muted small">%s</div><div class="h5 mb-0">%s</div></div>'
            '</div>') % (
            _('Hilos 90 días'), data.get('threads_90d', 0),
            _('Esperan respuesta nuestra'), data.get('waiting_us', 0),
            _('Último correo'), self._dt(last.get('last_activity')) if last else '—',
            _('Contactos conocidos'), len(data.get('contacts') or [])))
        # Notas de relación
        notes = [company.get('relationship_summary'), company.get('strategic_notes')]
        signals = []
        for key, label in (('risk_signals', _('Riesgos')), ('opportunity_signals', _('Oportunidades'))):
            val = company.get(key)
            if isinstance(val, list) and val:
                signals.append(Markup('<b>%s:</b> %s') % (label, ', '.join(str(v) for v in val[:6])))
        if any(notes) or signals:
            body = Markup('').join(Markup('<p>%s</p>') % e(n) for n in notes if n)
            body += Markup('').join(Markup('<p>%s</p>') % s for s in signals)
            parts.append(Markup('<h5>%s</h5>%s') % (_('Relación'), body))
        parts.extend(self._memoria_render_brief(data.get('brief') or {}))
        # Pendientes detectados en correo
        pending = data.get('pending') or []
        opened = [x for x in pending if x.get('status') == 'open']
        if pending:
            rows = Markup('').join(Markup(
                '<tr class="%s"><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>') % (
                'table-warning' if x.get('status') == 'open' else '',
                self._dt(x.get('detected_at'))[:10], e(x.get('tipo') or ''), e(x.get('descripcion') or ''),
                x.get('deadline') or '—', e(x.get('status') or ''))
                for x in pending[:12])
            parts.append(Markup(
                '<h5>%s <span class="badge text-bg-warning">%s</span></h5>'
                '<table class="table table-sm"><thead><tr><th>%s</th><th>%s</th><th>%s</th><th>%s</th><th>%s</th>'
                '</tr></thead><tbody>%s</tbody></table>') % (
                _('Pendientes detectados en correo'), len(opened),
                _('Detectado'), _('Tipo'), _('Qué'), _('Vence'), _('Estado'), rows))
        # Hilos recientes
        if threads:
            rows = Markup('').join(Markup(
                '<tr><td>%s</td><td><a href="%s" target="_blank">%s</a></td><td>%s</td><td class="text-end">%s</td>'
                '<td>%s</td><td>%s</td></tr>') % (
                self._dt(t.get('last_activity')), GMAIL_THREAD_URL % e(t.get('gmail_thread_id') or ''),
                e(t.get('subject') or _('(sin asunto)')), e(t.get('account') or ''),
                t.get('message_count') or 0,
                _('nosotros') if t.get('last_sender_type') == 'internal' else _('ellos'),
                e(t.get('status') or ''))
                for t in threads)
            parts.append(Markup(
                '<h5>%s</h5><table class="table table-sm"><thead><tr><th>%s</th><th>%s</th><th>%s</th>'
                '<th class="text-end">%s</th><th>%s</th><th>%s</th></tr></thead><tbody>%s</tbody></table>'
                '<p class="text-muted small">%s</p>') % (
                _('Hilos recientes'), _('Último'), _('Asunto'), _('Buzón'), _('Msjs'), _('Último en escribir'),
                _('Estado'), rows,
                _('El enlace abre el hilo en Gmail del buzón indicado; solo funciona con acceso a ese buzón.')))
        # Demanda
        demand = data.get('demand') or []
        if demand:
            rows = Markup('').join(Markup(
                '<tr><td>%s</td><td>%s</td><td>%s</td><td class="text-end">%s %s</td><td>%s</td></tr>') % (
                self._dt(d.get('detected_at'))[:10], e(d.get('product_ref') or ''), e(d.get('product_desc') or ''),
                self._num(d.get('qty')), e(d.get('uom') or ''), e(d.get('period_label') or d.get('demand_date') or ''))
                for d in demand)
            parts.append(Markup(
                '<h5>%s</h5><table class="table table-sm"><thead><tr><th>%s</th><th>%s</th><th>%s</th>'
                '<th class="text-end">%s</th><th>%s</th></tr></thead><tbody>%s</tbody></table>') % (
                _('Demanda mencionada en correo'), _('Detectado'), _('Ref'), _('Producto'), _('Cantidad'),
                _('Periodo'), rows))
        # Contactos
        contacts = data.get('contacts') or []
        if contacts:
            rows = Markup('').join(Markup(
                '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td class="text-end">%s</td>'
                '<td class="text-end">%s</td></tr>') % (
                e(c.get('name') or ''), e(c.get('email') or ''), e(c.get('role') or c.get('department') or ''),
                self._dt(c.get('last_activity')), c.get('interaction_count') or 0,
                self._num(c.get('avg_response_time_hours')))
                for c in contacts)
            parts.append(Markup(
                '<h5>%s</h5><table class="table table-sm"><thead><tr><th>%s</th><th>%s</th><th>%s</th><th>%s</th>'
                '<th class="text-end">%s</th><th class="text-end">%s</th></tr></thead><tbody>%s</tbody></table>') % (
                _('Contactos y ritmo de respuesta'), _('Nombre'), _('Correo'), _('Rol'), _('Último correo'),
                _('Interacciones'), _('Horas de respuesta'), rows))
        if len(parts) == 1:
            parts.append(Markup('<p class="text-muted">%s</p>') % _('Empresa conocida, pero sin hilos ni pendientes registrados.'))
        return Markup('').join(parts)

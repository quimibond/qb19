# -*- coding: utf-8 -*-
"""57.101.0 (C1): ficha del indicador en PDF.

Una hoja por indicador: cómo se mide, metas, gráfica de los últimos periodos
con medición (SVG generado aquí; sin JS ni librerías) con las franjas verde /
amarilla / roja de CADA periodo, el semáforo guardado de cada medición y la
causa y acciones de los rojos. Las metas de un periodo son las que juzgaron
la medición: las guardadas al validar (K-04) o las vigentes del periodo
(escalón de trayectoria).

wkhtmltopdf dibuja SVG en línea con ancho y alto explícitos; si en algún
build no lo hiciera, la plantilla cambia a ``<img>`` con
``sgi_sheet_svg_b64`` (el mismo SVG en base64)."""
import base64

from markupsafe import Markup, escape

from odoo import api, models

SHEET_PERIODS = 12
SVG_W, SVG_H = 700, 250
PAD_L, PAD_R, PAD_T, PAD_B = 52, 10, 12, 36
POINT_COLOR = {'verde': '#198754', 'amarillo': '#cc9a06', 'rojo': '#dc3545'}
ZONE_COLOR = {'verde': '#d1e7dd', 'amarillo': '#fff3cd', 'rojo': '#f8d7da'}
INF = float('inf')


def sgi_zones(direction, objective, acceptable, lo=0.0, hi=0.0, tol=0.0):
    """Franjas (desde, hasta, color) de un periodo, con la regla de
    ``sgi_indicator_integrity.sgi_semaphore``."""
    if direction == 'range':
        return [(lo, hi, 'verde'), (lo - tol, lo, 'amarillo'), (hi, hi + tol, 'amarillo'),
                (-INF, lo - tol, 'rojo'), (hi + tol, INF, 'rojo')]
    if direction == 'lower_better':
        return [(-INF, objective, 'verde'), (objective, acceptable, 'amarillo'),
                (acceptable, INF, 'rojo')]
    return [(objective, INF, 'verde'), (acceptable, objective, 'amarillo'),
            (-INF, acceptable, 'rojo')]


def sgi_num(value):
    """Número para la ficha: hasta dos decimales, sin ceros de sobra."""
    if value is None or value is False:
        return ''
    return ('%.2f' % value).rstrip('0').rstrip('.')


def _bounds(values):
    lo, hi = min(values), max(values)
    if lo == hi:
        lo, hi = lo - 1, hi + 1
    pad = (hi - lo) * 0.08
    return lo - pad, hi + pad


def sgi_trend_svg(points, uom=''):
    """SVG (Markup) de la tendencia. ``points``: [{'label', 'value' (None =
    sin dato), 'semaphore', 'zones'}] en orden; ``axis`` (opcional) es la
    etiqueta corta del eje. Todo texto va escapado."""
    if not points:
        return Markup('')
    finite = [p['value'] for p in points if p['value'] is not None]
    finite += [b for p in points for z in p['zones'] for b in z[:2] if b not in (INF, -INF)]
    y0, y1 = _bounds(finite or [0.0])
    plot_w, plot_h = SVG_W - PAD_L - PAD_R, SVG_H - PAD_T - PAD_B
    slot = plot_w / len(points)

    def y(value):
        value = max(y0, min(y1, value))
        return PAD_T + plot_h * (y1 - value) / (y1 - y0)

    out = ['<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" '
           'viewBox="0 0 %d %d" font-family="Arial, sans-serif" font-size="9">'
           % (SVG_W, SVG_H, SVG_W, SVG_H)]
    for i, point in enumerate(points):           # franjas por periodo
        left = PAD_L + slot * i
        for lo, hi, color in point['zones']:
            top, bottom = y(min(hi, y1)), y(max(lo, y0))
            if bottom - top > 0.5:
                out.append('<rect x="%.1f" y="%.1f" width="%.1f" height="%.1f" fill="%s"/>'
                           % (left, top, slot, bottom - top, ZONE_COLOR[color]))
    for k in range(5):                           # eje y
        value = y0 + (y1 - y0) * k / 4
        out.append('<line x1="%d" x2="%d" y1="%.1f" y2="%.1f" stroke="#ccc" stroke-width="0.5"/>'
                   % (PAD_L, SVG_W - PAD_R, y(value), y(value)))
        out.append('<text x="%d" y="%.1f" text-anchor="end">%s</text>'
                   % (PAD_L - 4, y(value) + 3, escape(sgi_num(value))))
    if uom:
        out.append('<text x="4" y="%d">%s</text>' % (PAD_T + 4, escape(uom)))
    segment = []                                 # línea: se corta en «sin dato»
    for i, point in enumerate(points + [{'value': None}]):
        if point.get('value') is None:
            if len(segment) > 1:
                out.append('<polyline fill="none" stroke="#333" stroke-width="1.5" points="%s"/>'
                           % " ".join(segment))
            segment = []
            continue
        segment.append('%.1f,%.1f' % (PAD_L + slot * (i + 0.5), y(point['value'])))
    for i, point in enumerate(points):           # puntos y etiquetas
        cx = PAD_L + slot * (i + 0.5)
        if point['value'] is None:
            out.append('<text x="%.1f" y="%.1f" text-anchor="middle" fill="#6c757d">s/d</text>'
                       % (cx, PAD_T + plot_h / 2))
        else:
            out.append('<circle cx="%.1f" cy="%.1f" r="3.5" fill="%s"/>'
                       % (cx, y(point['value']), POINT_COLOR.get(point['semaphore'], '#6c757d')))
        out.append('<text x="%.1f" y="%d" text-anchor="middle">%s</text>'
                   % (cx, SVG_H - PAD_B + 14, escape(point.get('axis') or point['label'])))
    out.append('</svg>')
    return Markup(''.join(out))


class SgiIndicatorSheet(models.Model):
    """57.101.0 (C1): datos de la ficha del indicador en PDF (periodos, metas
    de cada periodo y gráfica) y el botón «Ficha en PDF»."""
    _inherit = 'sgi.indicator'

    def _sgi_period_label(self, period_date):
        self.ensure_one()
        if self.frequency == 'weekly':
            return "Sem %s" % period_date.strftime('%d/%m/%y')
        return period_date.strftime('%m/%Y')

    def _sgi_period_axis(self, period_date):
        """Etiqueta corta del eje de la gráfica (cabe con 12 periodos)."""
        self.ensure_one()
        return period_date.strftime('%d/%m' if self.frequency == 'weekly' else '%m/%y')

    def _sgi_sheet_measures(self, limit=SHEET_PERIODS):
        """Las últimas ``limit`` mediciones del indicador, de la más vieja a
        la más nueva (los periodos medidos, no el calendario)."""
        self.ensure_one()
        return self.env['sgi.indicator.measure'].search(
            [('indicator_id', '=', self.id)], order='period_date desc', limit=limit,
        ).sorted('period_date')

    def _sgi_sheet_rows(self, limit=SHEET_PERIODS):
        """Un renglón por periodo con su valor (None si es pendiente o sin
        dato), su semáforo guardado y las metas con las que se juzgó."""
        self.ensure_one()
        Measure = self.env['sgi.indicator.measure']
        semaphores = dict(Measure._fields['semaphore'].selection)
        states = dict(Measure._fields['state'].selection)
        rows = []
        for measure in self._sgi_sheet_measures(limit):
            direction, objective, acceptable, lo, hi, tol = measure._sgi_sheet_targets()
            has_value = measure.state in ('capturado', 'validado')
            semaphore = measure.semaphore if has_value else False
            rows.append({
                'measure': measure,
                'label': self._sgi_period_label(measure.period_date),
                'axis': self._sgi_period_axis(measure.period_date),
                'value': measure.value if has_value else None,
                'semaphore': semaphore,
                'semaphore_label': semaphores.get(semaphore) or '—',
                'state_label': states.get(measure.state, measure.state or ''),
                'direction': direction, 'objective': objective, 'acceptable': acceptable,
                'range': (lo, hi, tol), 'frozen': measure.sgi_targets_frozen,
                'zones': sgi_zones(direction, objective, acceptable, lo, hi, tol),
                'red': has_value and semaphore == 'rojo',
            })
        return rows

    def sgi_sheet_svg(self, rows=None):
        """La gráfica de la ficha (Markup con el SVG)."""
        self.ensure_one()
        return sgi_trend_svg(rows if rows is not None else self._sgi_sheet_rows(), self.uom or '')

    def sgi_sheet_svg_b64(self, rows=None):
        """El mismo SVG en base64, por si el PDF lo necesita como imagen."""
        return base64.b64encode(str(self.sgi_sheet_svg(rows)).encode()).decode()

    @api.model
    def sgi_sheet_num(self, value):
        return sgi_num(value)

    def action_print_sheet(self):
        """Botón «Ficha en PDF» del indicador."""
        return self.env.ref('quimibond_sgi.action_report_indicator_sheet').report_action(
            self, config=False)


class SgiIndicatorMeasureSheet(models.Model):
    """57.101.0 (C1): metas con las que se juzga cada medición en la ficha."""
    _inherit = 'sgi.indicator.measure'

    def _sgi_sheet_targets(self):
        """(sentido, objetivo, aceptable, mínimo, máximo, tolerancia) con los
        que se juzga la medición: los guardados al validar (K-04) o los
        vigentes del periodo."""
        self.ensure_one()
        if self.sgi_targets_frozen:
            return (self.sgi_frozen_direction or 'higher_better', self.sgi_frozen_objective,
                    self.sgi_frozen_acceptable, self.sgi_frozen_range_min,
                    self.sgi_frozen_range_max, self.sgi_frozen_range_tolerance)
        indicator = self.indicator_id
        return (indicator.direction or 'higher_better', self.target_objective,
                self.target_acceptable, indicator.range_min, indicator.range_max,
                indicator.range_tolerance)


class SgiProcessSheet(models.Model):
    """57.101.0 (C1): indicadores de la ficha por proceso."""
    _inherit = 'sgi.process'

    def _sgi_sheet_indicators(self):
        self.ensure_one()
        return self.env['sgi.indicator'].search(
            [('process_id', '=', self.id), ('active', '=', True)], order='code')

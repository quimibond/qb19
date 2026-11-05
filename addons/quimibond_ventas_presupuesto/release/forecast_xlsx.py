# -*- coding: utf-8 -*-
"""Lector del forecast de Ventas (F-P-A28-13, «forecast.xlsx») — Python puro.

El libro trae una hoja por cliente o planta (TQ-1, SHAWMUT, FXI CD JUAREZ…)
con este layout (diseño v3, sección 1.4):

- fila de semanas: 1…52 en columnas (de C a BB casi siempre; en COPO y MACLIN
  la semana 1 está en B, en ZWISSTEX y PIELES en D);
- bloques de año apilados hacia abajo: una fila con el año (2026) en A o B,
  la fila «FECHA DE ENTREGA», la fila «PRODUCTO / PO» con la OC por semana,
  los renglones de producto (nuestro código en A, la parte del cliente en B)
  y un renglón TOTAL;
- subbloques por planta dentro del año (SHAWMUT: «SILAO», «TENNESSEE»);
- renglones de PO intercalados después de un producto (ZWISSTEX) y renglones
  de conversión o de rendimiento pegados al bloque, que no son pronóstico.

La unidad no está en la hoja del cliente: viene en la columna B de la hoja
«PRONOSTICO <año>» (m, yd, kg) junto con marcas que no son unidad
(«Muestra», «Pokayoke», «MAQUILA»).

Se leen las hojas de cliente y no PRONOSTICO 2026/2027 porque esas son
fórmulas con rangos corridos y, en 2027, copias de 2026 (sección 1.4).
Nada se inventa: una celda de texto en una semana («por confirmar») no es
cantidad, queda como aviso.
"""
import re
import unicodedata
from datetime import date, datetime

SUMMARY_PREFIX = 'PRONOSTICO'
# Hojas del libro que no son de un cliente.
NOT_CLIENT_SHEETS = ('PRONOSTICO', 'PRONÓSTICO', 'CONVERSI', 'CONVERCION', 'MUESTRAS',
                     'PRESUPUESTO', 'PRECIOS')
LABELS_DATES = ('FECHA DE ENTREGA', 'FECHA ENTREGA')
LABELS_SKIP = ('TOTAL', 'RENDIMIENTO', 'SEMANA', 'CONVERSION', 'CONVERSIÓN', 'KILOS', 'METROS',
               'MES', 'CLIENTE', '# QUIMIBOND')
UNITS = {'M': 'm', 'MT': 'm', 'MTS': 'm', 'METROS': 'm', 'YD': 'yd', 'YDS': 'yd', 'YARDAS': 'yd',
         'KG': 'kg', 'KGS': 'kg', 'KILOS': 'kg'}
# Nuestro código de producto: empieza con letra, sin espacios, 6+ caracteres
# (WJ053Q22JNT150, IK20BL155, NN060Q66JNT163RE, PSITPBOMBAYAZ/JI).
CODE_RE = re.compile(r'^[A-Z][A-Z0-9/\-\.]{5,}$')
MIN_WEEKS = 40


def norm(text):
    """Mayúsculas, sin acentos ni signos: «Saltillo Lamination (León)» →
    «SALTILLO LAMINATION LEON»."""
    text = unicodedata.normalize('NFKD', str(text or ''))
    text = ''.join(c for c in text if not unicodedata.combining(c)).upper()
    return re.sub(r'\s+', ' ', re.sub(r'[^A-Z0-9 ]', ' ', text)).strip()


def is_client_sheet(title):
    upper = norm(title)
    return not any(upper.startswith(norm(p)) for p in NOT_CLIENT_SHEETS)


def _text(value):
    if value is None:
        return ''
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def _num(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _as_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def _year(value):
    number = _num(value)
    if number is not None and number.is_integer() and 2020 <= number <= 2045:
        return int(number)
    return None


def iso_monday(year, week):
    """Lunes de la semana ISO ``week`` de ``year`` (el forecast numera así:
    la semana 2 de 2026 empieza el 5 de enero)."""
    return date.fromisocalendar(year, week, 1)


def find_week_columns(rows, max_header_rows=8):
    """{índice de columna (0 = A): número de semana} de la fila de semanas.
    ``rows`` es la lista de filas (listas de valores). Vacío si no hay."""
    for row in rows[:max_header_rows]:
        weeks = {}
        for idx, value in enumerate(row):
            number = _num(value)
            if number is not None and number.is_integer() and 1 <= number <= 53:
                weeks[idx] = int(number)
        if len(weeks) >= MIN_WEEKS and sorted(weeks.values())[:2] == [1, 2]:
            return weeks
    return {}


def summary_units(rows):
    """Unidad y marca por (cliente, producto) de la hoja PRONOSTICO <año>.

    Columna A = cliente (solo en el primer renglón del bloque), B = unidad o
    marca, C = producto. La unidad vale para los renglones siguientes del
    mismo cliente hasta que cambie. Devuelve ({cliente_normalizado: unidad},
    {(cliente_normalizado, código): marca})."""
    units, tags = {}, {}
    seen = {}
    client = None
    current_unit = None
    for row in rows:
        cells = list(row) + [None] * 3
        a, b, c = cells[0], cells[1], cells[2]
        if _text(a) and not _year(a) and norm(a) not in ('CLIENTE',):
            if norm(a) != client:
                client = norm(a)
                current_unit = None
        code = norm(c).replace(' ', '') if c else ''
        if client and code:
            seen[(client, code)] = seen.get((client, code), 0) + 1
        mark = _text(b)
        if mark:
            unit = UNITS.get(norm(mark).replace(' ', ''))
            if unit:
                current_unit = unit
            elif client and code:
                tags[(client, code)] = mark
        if client and current_unit:
            units.setdefault(client, current_unit)
        if client and code and current_unit:
            units[(client, code)] = current_unit
    # Un producto que aparece dos veces en el bloque (CONTITECH: el mismo código normal y
    # «Engomado 2cm») no se puede marcar por renglón: la marca se quita.
    tags = {key: mark for key, mark in tags.items() if seen.get(key, 0) == 1}
    return units, tags


def match_client(sheet_title, plant, summary_clients):
    """El cliente de PRONOSTICO que corresponde a una hoja (+ planta).

    «SHAWMUT» + «SILAO» → «SHAWMUT SILAO»; «SALTILLO LAMINATION LEON» →
    «SALTILLO LAMINATION LEON» (en el resumen va entre paréntesis);
    «Woodbridge Inoac» → «WOODBRIDGE INOAC TECHNICAL PRODUCTS…» (prefijo)."""
    key = norm('%s %s' % (sheet_title, plant or ''))
    base = norm(sheet_title)
    if key in summary_clients:
        return key
    for candidate in summary_clients:
        if candidate.startswith(key) or key.startswith(candidate):
            return candidate
    for candidate in summary_clients:
        if candidate.startswith(base) or base.startswith(candidate):
            return candidate
    first = base.split(' ')[0] if base else ''
    matches = [c for c in summary_clients if c.split(' ')[0] == first]
    return matches[0] if len(matches) == 1 else None


def _label(row):
    """Etiqueta de la fila (columna A o B) normalizada."""
    for value in row[:2]:
        if isinstance(value, str) and value.strip():
            return norm(value)
    return ''


def parse_sheet(title, rows, units=None, tags=None, years=None):
    """Renglones del pronóstico de una hoja de cliente.

    Devuelve (renglones, avisos). Cada renglón: sheet, plant, year, row (1 =
    fila 1), code, part, week, monday, delivery, po, qty, unit, tag."""
    units = units or {}
    tags = tags or {}
    warnings = []
    week_cols = find_week_columns(rows)
    if not week_cols:
        return [], ['%s: no se encontró la fila de semanas 1…52.' % title]
    first_week_col = min(week_cols)
    summary_clients = [k for k in units if isinstance(k, str)]
    out = []
    year = None
    year_from_dates = False
    plant = None
    dates = {}
    block_po = {}
    block_po_label = None
    last_product = None
    for index, raw in enumerate(rows):
        row = list(raw) + [None] * (max(week_cols) + 1 - len(raw))
        rownum = index + 1
        # Año del bloque: el año en A o B, sin nada más en la fila.
        found_year = next((_year(v) for v in row[:2] if _year(v)), None)
        week_values = {c: row[c] for c in week_cols
                       if row[c] is not None and not (isinstance(row[c], str) and not row[c].strip())}
        if found_year and not any(_num(v) for c, v in week_values.items()):
            year, plant, dates, block_po, block_po_label, last_product = (
                found_year, None, {}, {}, None, None)
            year_from_dates = False
            continue
        label = _label(row)
        is_dates_row = label in LABELS_DATES or any(label.startswith(l) for l in LABELS_DATES)
        if is_dates_row and (year is None or year_from_dates):
            # Hoja sin filas de año (ENTEX): cada bloque toma el año de sus fechas de entrega.
            years_seen = [d.year for d in (_as_date(v) for v in week_values.values()) if d]
            if years_seen:
                new_year = max(set(years_seen), key=years_seen.count)
                if new_year != year:
                    year, plant, block_po, block_po_label, last_product = new_year, None, {}, None, None
                year_from_dates = True
        if year is None:
            continue
        a_text = norm(row[0]).replace(' ', '') if isinstance(row[0], str) else ''
        if is_dates_row:
            dates = {c: _as_date(v) for c, v in week_values.items() if _as_date(v)}
            continue
        is_po_row = label.startswith('PRODUCTO') or label == 'PO' or label.startswith('PO ')
        if not is_po_row and isinstance(row[1], str) and first_week_col > 1:
            b_label = norm(row[1])
            is_po_row = b_label == 'PO' or b_label.startswith('PO ') and not row[0]
        if is_po_row:
            pos = {c: _text(v) for c, v in week_values.items() if _text(v)}
            header = label.startswith('PRODUCTO') or (row[0] and norm(row[0]).startswith('PRODUCTO'))
            label_po = _text(row[1]) if first_week_col > 1 else ''
            if header or last_product is None:
                block_po = pos
                block_po_label = label_po or None
            elif last_product is not None:
                # PO intercalado: es del producto de arriba (ZWISSTEX).
                for item in out:
                    if item['_product_row'] == last_product and not item['po']:
                        item['po'] = pos.get(item['_col']) or label_po or None
            continue
        if any(label.startswith(s) for s in LABELS_SKIP):
            continue
        is_code = bool(a_text) and CODE_RE.match(a_text) and not any(
            a_text.startswith(norm(s).replace(' ', '')) for s in LABELS_SKIP)
        if not is_code:
            # Planta: texto solo, a lo más un número suelto (SILAO; «TENNESSEE» trae un 22 al lado).
            numbers = [v for v in week_values.values() if _num(v)]
            if label and len(numbers) <= 1 and len(label) <= 40 and not label[0].isdigit():
                plant = label.title()
            continue
        if years and year not in years:
            last_product = rownum
            continue
        code = a_text
        part = _text(row[1]) if first_week_col > 1 else ''
        client_key = match_client(title, plant, summary_clients)
        unit = units.get((client_key, code)) or units.get(client_key) if client_key else None
        tag = tags.get((client_key, code)) if client_key else None
        last_product = rownum
        for col, value in sorted(week_values.items()):
            qty = _num(value)
            if qty is None:
                warnings.append('%s fila %d semana %d: «%s» no es cantidad.' % (
                    title, rownum, week_cols[col], _text(value)))
                continue
            if qty <= 0:
                continue
            week = week_cols[col]
            try:
                monday = iso_monday(year, week)
            except ValueError:
                warnings.append('%s fila %d: la semana %d no existe en %d.' % (
                    title, rownum, week, year))
                continue
            out.append({
                'sheet': title, 'plant': plant, 'year': year, 'row': rownum,
                'code': code, 'part': part or None, 'week': week, 'monday': monday,
                'delivery': dates.get(col), 'po': block_po.get(col) or block_po_label,
                'qty': qty, 'unit': unit, 'tag': tag,
                '_product_row': rownum, '_col': col,
            })
    for item in out:
        item.pop('_product_row', None)
        item.pop('_col', None)
    return out, warnings


def parse_workbook(workbook, years=None, sheets=None):
    """Lee todas las hojas de cliente de un libro abierto con openpyxl
    (``data_only=True``). ``years``: años a leer (None = todos); ``sheets``:
    nombres de hoja (None = todas las de cliente).

    Devuelve {'rows': [...], 'warnings': [...], 'sheets': [...]}."""
    titles = workbook.sheetnames
    units, tags = {}, {}
    for title in titles:
        if norm(title).startswith(SUMMARY_PREFIX):
            ws_units, ws_tags = summary_units(list(workbook[title].iter_rows(values_only=True)))
            for key, value in ws_units.items():
                units.setdefault(key, value)
            for key, value in ws_tags.items():
                tags.setdefault(key, value)
    rows, warnings, read = [], [], []
    for title in titles:
        if not is_client_sheet(title) or (sheets and title not in sheets):
            continue
        sheet_rows, sheet_warnings = parse_sheet(
            title, list(workbook[title].iter_rows(values_only=True)), units, tags, years)
        rows.extend(sheet_rows)
        warnings.extend(sheet_warnings)
        read.append(title)
    return {'rows': rows, 'warnings': warnings, 'sheets': read}

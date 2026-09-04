# -*- coding: utf-8 -*-
"""Catalogo de lineas del Estado de flujo de efectivo NIF B-2.

Cada linea tiene una clave estable (``key``) que usan las reglas de
configuracion (``cash.flow.rule``), el motor de calculo (``cash.flow.engine``)
y el handler del reporte. Las claves se agrupan en secciones; el orden de esta
lista es el orden de presentacion.

Convencion de signos: todo importe es *efecto en efectivo*: positivo entra,
negativo sale. Las partidas virtuales (``addback``) se presentan con el signo
con que se suman de vuelta al resultado.
"""

# (key, section, label, method)
#   method: 'indirect' | 'direct' | 'both'
LINES = [
    # ---- Metodo indirecto: operacion ------------------------------------
    ('result', 'ind_result', 'Resultado antes de impuestos', 'indirect'),
    ('nc_depreciation', 'ind_noncash', 'Depreciación y amortización', 'indirect'),
    ('nc_depreciation_ctr', 'ind_noncash', 'Depreciación acumulada vs. gasto (diferencia, revisar)', 'indirect'),
    ('nc_asset_result', 'ind_noncash', '(Utilidad) pérdida en venta de activo fijo', 'indirect'),
    ('nc_casualty', 'ind_noncash', 'Pérdidas por siniestro', 'indirect'),
    ('nc_bad_debt', 'ind_noncash', 'Estimación de cuentas incobrables', 'indirect'),
    ('nc_fx_cash', 'ind_noncash', 'Diferencias cambiarias sobre efectivo (reclasificadas)', 'indirect'),
    ('nc_interest', 'ind_noncash', 'Intereses devengados (reclasificados a financiamiento)', 'indirect'),
    ('nc_lease', 'ind_noncash', 'Arrendamiento financiero devengado (reclasificado a financiamiento)', 'indirect'),
    ('nc_rou', 'ind_noncash', 'Reconocimiento de arrendamientos NIF D-5 (neto, sin flujo)', 'indirect'),
    ('wc_receivables', 'ind_wc', 'Clientes', 'indirect'),
    ('wc_inventory', 'ind_wc', 'Inventarios', 'indirect'),
    ('wc_other_receivables', 'ind_wc', 'Otras cuentas por cobrar y pagos anticipados', 'indirect'),
    ('wc_tax_receivable', 'ind_wc', 'Impuestos por recuperar', 'indirect'),
    ('wc_payables', 'ind_wc', 'Proveedores', 'indirect'),
    ('wc_customer_advances', 'ind_wc', 'Anticipos de clientes', 'indirect'),
    ('wc_taxes_payable', 'ind_wc', 'Impuestos por pagar y retenciones', 'indirect'),
    ('wc_payroll', 'ind_wc', 'Nómina y prestaciones', 'indirect'),
    ('unclassified', 'ind_wc', 'Sin clasificar (revisar)', 'indirect'),
    # ---- Metodo indirecto: inversion -------------------------------------
    ('inv_acquisitions', 'ind_investing', 'Adquisición de activo fijo y depósitos en garantía', 'indirect'),
    ('inv_disposals', 'ind_investing', 'Venta y bajas de activo fijo', 'indirect'),
    # ---- Metodo indirecto: financiamiento --------------------------------
    ('fin_loans_received', 'ind_financing', 'Préstamos recibidos', 'indirect'),
    ('fin_loans_paid', 'ind_financing', 'Préstamos pagados', 'indirect'),
    ('fin_lease', 'ind_financing', 'Arrendamiento financiero pagado', 'indirect'),
    ('fin_interest', 'ind_financing', 'Intereses pagados', 'indirect'),
    ('fin_related', 'ind_financing', 'Partes relacionadas', 'indirect'),
    ('fin_equity', 'ind_financing', 'Capital y dividendos', 'indirect'),
    # ---- Efecto cambiario (indirecto) ------------------------------------
    ('fx_effect', 'ind_fx', 'Efecto por cambios en el valor del efectivo', 'indirect'),
    # ---- Metodo directo: operacion ---------------------------------------
    ('d_customers', 'dir_operating', 'Cobros a clientes', 'direct'),
    ('d_suppliers', 'dir_operating', 'Pagos a proveedores', 'direct'),
    ('d_payroll', 'dir_operating', 'Nómina y prestaciones pagadas', 'direct'),
    ('d_taxes', 'dir_operating', 'Impuestos y cuotas pagados (SAT, IMSS, ISN)', 'direct'),
    ('d_bank_fees', 'dir_operating', 'Comisiones bancarias', 'direct'),
    ('d_interest_received', 'dir_operating', 'Intereses cobrados', 'direct'),
    ('d_other_income', 'dir_operating', 'Otros ingresos cobrados', 'direct'),
    ('d_other', 'dir_operating', 'Otros (revisar)', 'direct'),
    # ---- Metodo directo: inversion ---------------------------------------
    ('d_assets_bought', 'dir_investing', 'Activo fijo comprado', 'direct'),
    ('d_assets_sold', 'dir_investing', 'Activo fijo vendido', 'direct'),
    ('d_insurance', 'dir_investing', 'Indemnizaciones de seguros cobradas', 'direct'),
    # ---- Metodo directo: financiamiento ----------------------------------
    ('d_loans_received', 'dir_financing', 'Préstamos recibidos', 'direct'),
    ('d_loans_paid', 'dir_financing', 'Préstamos pagados', 'direct'),
    ('d_lease', 'dir_financing', 'Arrendamientos pagados', 'direct'),
    ('d_interest', 'dir_financing', 'Intereses pagados', 'direct'),
    ('d_related', 'dir_financing', 'Partes relacionadas', 'direct'),
    ('d_equity', 'dir_financing', 'Capital y dividendos', 'direct'),
    # ---- Efecto cambiario (directo) --------------------------------------
    ('d_fx', 'dir_fx', 'Efecto por cambios en el valor del efectivo', 'direct'),
]

SECTIONS = [
    ('ind_result', 'Resultado del periodo'),
    ('ind_noncash', 'Partidas sin efecto en efectivo'),
    ('ind_wc', 'Cambios en capital de trabajo'),
    ('ind_investing', 'Actividades de inversión'),
    ('ind_financing', 'Actividades de financiamiento'),
    ('ind_fx', 'Efecto por cambios en el valor del efectivo'),
    ('dir_operating', 'Actividades de operación'),
    ('dir_investing', 'Actividades de inversión'),
    ('dir_financing', 'Actividades de financiamiento'),
    ('dir_fx', 'Efecto por cambios en el valor del efectivo'),
]

LINE_LABELS = {key: label for key, _section, label, _method in LINES}
LINE_SECTION = {key: section for key, section, _label, _method in LINES}
LINE_METHOD = {key: method for key, _section, _label, method in LINES}
SECTION_LABELS = dict(SECTIONS)

INDIRECT_KEYS = [key for key, _s, _l, m in LINES if m == 'indirect']
DIRECT_KEYS = [key for key, _s, _l, m in LINES if m == 'direct']

# Secciones que forman el "incremento neto" de cada metodo (sin el efecto
# cambiario, que la NIF B-2 presenta aparte).
INDIRECT_OPERATING_SECTIONS = ('ind_result', 'ind_noncash', 'ind_wc')
INDIRECT_NET_SECTIONS = INDIRECT_OPERATING_SECTIONS + ('ind_investing', 'ind_financing')
DIRECT_NET_SECTIONS = ('dir_operating', 'dir_investing', 'dir_financing')

# Tipos de cuenta de resultados (para la red de seguridad "sin clasificar").
PL_ACCOUNT_TYPES = (
    'income', 'income_other', 'expense', 'expense_other',
    'expense_depreciation', 'expense_direct_cost',
)


def line_selection(method=None):
    """Valores de seleccion para los campos ``line_key`` de las reglas."""
    return [
        (key, '%s / %s' % (SECTION_LABELS[section], label))
        for key, section, label, line_method in LINES
        if method is None or line_method == method
    ]

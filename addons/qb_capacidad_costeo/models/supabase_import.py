# -*- coding: utf-8 -*-
"""Importador de configuración desde Supabase (Quimibond Intelligence).

Toda la configuración de costeo ya vive curada en la capa silver de
Supabase — centros de costo, asignación de cuentas de overhead, rentas
contractuales por lote, el maestro de pesos (2,758 productos: maestro de
ingeniería > CVU medido > gramaje > BOM), conversiones kg↔m (CVU real),
cuentas variables y fab_weight_share. Este importador la jala vía REST
(PostgREST) con las credenciales que el sync ya tiene en Odoo.sh
(ir.config_parameter quimibond_intelligence.supabase_url / service_key)
y la vuelca en los modelos de config del módulo. Idempotente: corre las
veces que sea; cron semanal la mantiene fresca.

Reglas de no-pisado: un peso editado a mano en Odoo (source=manual) solo
se sobreescribe si Supabase también trae manual (el maestro de Jessica).
"""
import logging

from odoo import api, fields, models
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

# Los códigos de centro difieren ligeramente entre Supabase y los seeds
CENTRO_ALIAS = {
    'INSPECCION_EMPAQUE': 'INSP_EMPAQUE',
    'ADMIN': 'ADMINISTRACION',
}

SOURCE_MAP = {
    'manual': 'manual', 'cvu': 'cvu', 'ref_gramaje': 'ref_gramaje',
    'bom_weight': 'bom', 'odoo_weight': 'odoo_weight',
    'import_twin': 'import_twin', 'kg_native': 'kg_native',
}


class QbCosteoSupabaseImport(models.TransientModel):
    _name = 'qb.costeo.supabase.import'
    _description = 'Importar configuración de costeo desde Supabase'

    resultado = fields.Text(readonly=True)

    # ------------------------------------------------------------------
    # REST helpers
    # ------------------------------------------------------------------
    @api.model
    def _sb_credentials(self):
        icp = self.env['ir.config_parameter'].sudo()
        url = (icp.get_param('quimibond_intelligence.supabase_url') or '').rstrip('/')
        key = icp.get_param('quimibond_intelligence.supabase_service_key') or ''
        if not url or not key:
            raise UserError(
                'Faltan los parámetros quimibond_intelligence.supabase_url / '
                'supabase_service_key (los mismos que usa el sync).')
        return url, key

    @api.model
    def _sb_get(self, table, params=None):
        """GET paginado a PostgREST. Devuelve la lista completa de filas."""
        try:
            import httpx
        except ImportError:
            raise UserError('El paquete python httpx no está disponible '
                            '(es dependencia del sync — revisar entorno).')
        url, key = self._sb_credentials()
        headers = {'apikey': key, 'Authorization': 'Bearer %s' % key}
        rows, offset, page = [], 0, 1000
        query = dict(params or {}, select='*')
        with httpx.Client(timeout=60) as client:
            while True:
                resp = client.get(
                    '%s/rest/v1/%s' % (url, table), params=query,
                    headers=dict(headers,
                                 Range='%s-%s' % (offset, offset + page - 1)))
                if resp.status_code >= 400:
                    raise UserError('Supabase %s: HTTP %s — %s'
                                    % (table, resp.status_code, resp.text[:300]))
                batch = resp.json()
                rows.extend(batch)
                if len(batch) < page:
                    return rows
                offset += page

    # ------------------------------------------------------------------
    # Import steps
    # ------------------------------------------------------------------
    @api.model
    def run_import(self):
        """Importa toda la configuración. Devuelve el log resumen."""
        log = []
        log.append(self._import_centros())
        log.append(self._import_rentas())
        log.append(self._import_workcenter_config())
        log.append(self._import_cuentas())
        log.append(self._import_factor_config())
        log.append(self._import_pesos())
        log.append(self._import_fichas())
        # Con cuentas nuevas clasificadas, refrescar el matching de una vez
        self.env['qb.costeo.cuenta.class'].cron_refresh_account_matching()
        log.append('Matching de cuentas refrescado.')
        log.append('\nSiguiente paso: Configuración → Recalcular costeo '
                   '(mes anterior) para regenerar factores y costos.')
        resumen = '\n'.join(log)
        _logger.info('qb.costeo.supabase.import:\n%s', resumen)
        return resumen

    @api.model
    def _centro_by_code(self, sb_code):
        code = CENTRO_ALIAS.get(sb_code, sb_code)
        return self.env['qb.costeo.centro'].with_context(
            active_test=False).search([('code', '=', code)], limit=1)

    @api.model
    def _import_centros(self):
        """cost_center_config → qb.costeo.centro (+ departamentos por patrón
        de nómina, + auto-link de workcenters por nombre)."""
        rows = self._sb_get('cost_center_config')
        Centro = self.env['qb.costeo.centro']
        Department = self.env['hr.department']
        created = updated = 0
        for row in rows:
            centro = self._centro_by_code(row['code'])
            uom = (row.get('output_uom') or '').lower()
            vals = {
                'name': row['name'],
                'nature': row['nature'],
                'active': row.get('active', True),
            }
            if uom:
                vals['driver_principal'] = 'peso' if uom == 'kg' else 'largo'
            if row.get('notes'):
                vals['notes'] = row['notes']
            if centro:
                centro.write(vals)
                updated += 1
            else:
                centro = Centro.create(dict(
                    vals, code=CENTRO_ALIAS.get(row['code'], row['code']),
                    driver_principal=vals.get('driver_principal', 'largo')))
                created += 1
            # Departamentos RH por tokens del patrón de nómina (%TEJIDO%|...)
            pattern = row.get('nomina_ref_pattern') or ''
            departments = Department.browse()
            for token in pattern.split('|'):
                token = token.strip().strip('%')
                if len(token) >= 4:
                    departments |= Department.search(
                        [('name', 'ilike', token)])
            if departments:
                centro.department_ids |= departments
        return ('Centros: %s creados, %s actualizados (de %s en Supabase).'
                % (created, updated, len(rows)))

    @api.model
    def _import_rentas(self):
        """rent_lot_assignment → renta_contractual_mxn por centro (solo
        asignaciones vigentes, sumadas con su % de asignación)."""
        rows = self._sb_get('rent_lot_assignment')
        totals = {}
        for row in rows:
            if row.get('effective_to'):
                continue
            code = row['cost_center_code']
            totals[code] = totals.get(code, 0.0) + (
                float(row['monthly_amount_mxn'])
                * float(row.get('allocation_pct') or 100.0) / 100.0)
        n = 0
        for code, monto in totals.items():
            centro = self._centro_by_code(code)
            if centro:
                centro.renta_contractual_mxn = monto
                n += 1
        return 'Rentas contractuales: %s centros actualizados.' % n

    @api.model
    def _import_workcenter_config(self):
        """workcenter_cost_config → throughput nominal + auto-link de
        mrp.workcenter por patrón de nombre (ej. %CIRCULAR% → TEJIDO).
        Al dar de alta workcenters nuevos que matcheen, el cron semanal
        los liga solo — cero manual."""
        rows = self._sb_get('workcenter_cost_config')
        Workcenter = self.env['mrp.workcenter']
        linked = 0
        for row in rows:
            centro = self._centro_by_code(row['cost_center_code'])
            if not centro:
                continue
            std = row.get('std_kg_per_machine_hour')
            if std:
                centro.std_output_per_hour = float(std)
            pattern = (row.get('workcenter_name_pattern') or '').strip('%')
            if pattern:
                found = Workcenter.search([('name', 'ilike', pattern)])
                new = found - centro.workcenter_ids
                if new:
                    centro.workcenter_ids |= new
                    linked += len(new)
        return ('Workcenters: config de %s centros importada, %s máquinas '
                'ligadas por patrón.' % (len(rows), linked))

    @api.model
    def _import_cuentas(self):
        """overhead_account_assignment + costing_variable_accounts →
        qb.costeo.cuenta.class. Las cuentas variables (luz/gas/agua) entran
        como bucket energia con driver peso; el resto como overhead directo
        al centro asignado."""
        assignments = self._sb_get('overhead_account_assignment')
        variable_patterns = [r['account_pattern'] for r
                             in self._sb_get('costing_variable_accounts')]
        Class = self.env['qb.costeo.cuenta.class']

        def is_variable(code):
            return any(code.startswith(p.rstrip('%')) for p in variable_patterns)

        n = 0
        for row in assignments:
            if row.get('effective_to'):
                continue
            code = row['account_code']
            pattern = code if code.endswith('%') else code + '%'
            centro = self._centro_by_code(row['cost_center_code'])
            if centro and centro.nature == 'admin':
                # Asignada a admin = aislada del pool fabril a propósito
                # (ej. 504.01.0035 gastos de importación: viven en el
                # landed cost del producto, no en fabricación).
                bucket, variable, driver = 'no_costeo', False, False
            elif is_variable(code):
                bucket, variable, driver = 'energia', True, 'peso'
            else:
                bucket, variable, driver = 'overhead_fab', False, 'directo'
            vals = {
                'code_pattern': pattern,
                'bucket': bucket,
                'es_variable': variable,
                'centro_id': centro.id if centro else False,
                'driver': driver,
                'allocation_pct': float(row.get('allocation_pct') or 100.0),
                'notes': row.get('notes') or 'Importado de Supabase '
                                             '(overhead_account_assignment).',
            }
            existing = Class.with_context(active_test=False).search(
                [('code_pattern', '=', pattern)], limit=1)
            if existing:
                existing.write(vals)
            else:
                Class.create(vals)
            n += 1
        # Variables sin asignación de centro (si las hubiera) → energia general
        for p in variable_patterns:
            if not Class.with_context(active_test=False).search(
                    [('code_pattern', '=', p)], limit=1):
                Class.create({'code_pattern': p, 'bucket': 'energia',
                              'es_variable': True, 'driver': 'peso',
                              'notes': 'Importado de costing_variable_accounts.'})
                n += 1
        return 'Cuentas: %s clasificaciones importadas/actualizadas.' % n

    @api.model
    def _import_factor_config(self):
        """costing_config → qb.costeo.factor.config (fab_weight_share, etc.)"""
        rows = self._sb_get('costing_config')
        Config = self.env['qb.costeo.factor.config']
        n = 0
        for row in rows:
            rec = Config.search([('key', '=', row['key'])], limit=1)
            vals = {'value': float(row['value'])}
            if row.get('notes'):
                vals['descripcion'] = row['notes']
            if rec:
                rec.write(vals)
            else:
                Config.create(dict(vals, key=row['key']))
            n += 1
        return 'Parámetros globales: %s importados (fab_weight_share, etc.).' % n

    @api.model
    def _import_pesos(self):
        """product_kg_per_unit (2,758) + product_uom_conversion (769) →
        qb.producto.peso. Los ids son odoo_product_id = product.product.id,
        así que el match es directo. Un peso editado a mano en Odoo solo se
        pisa si Supabase también trae source=manual (maestro de ingeniería)."""
        kg_rows = self._sb_get('product_kg_per_unit')
        conv = {r['odoo_product_id']: float(r['m_per_kg'])
                for r in self._sb_get('product_uom_conversion')}
        Peso = self.env['qb.producto.peso']
        existing = {p.product_id.id: p
                    for p in Peso.with_context(active_test=False).search([])}
        product_ids = [r['odoo_product_id'] for r in kg_rows]
        alive = set(self.env['product.product'].browse(product_ids).exists().ids)

        to_create = []
        created = updated = skipped = 0
        for row in kg_rows:
            pid = row['odoo_product_id']
            if pid not in alive:
                skipped += 1
                continue
            source = SOURCE_MAP.get(row.get('source'), 'manual')
            vals = {
                'kg_per_unit': float(row['kg_per_unit']),
                'source': source,
            }
            if pid in conv:
                vals['m_per_kg'] = conv[pid]
            rec = existing.get(pid)
            if rec:
                if rec.source == 'manual' and source != 'manual':
                    skipped += 1  # respeta el override local
                    continue
                rec.write(vals)
                updated += 1
            else:
                to_create.append(dict(vals, product_id=pid))
        if to_create:
            Peso.create(to_create)
            created = len(to_create)
        return ('Pesos: %s creados, %s actualizados, %s omitidos '
                '(override local o producto inexistente) de %s en Supabase; '
                '%s conversiones kg↔m.'
                % (created, updated, skipped, len(kg_rows), len(conv)))

    @api.model
    def _import_fichas(self):
        """Fichas técnicas: genera desde la nomenclatura y superpone el
        gramaje/ancho CURADO de Supabase (product_uom_conversion trae
        gramaje_g_m2 y ancho_m verificados contra CVU/maestro). Las fichas
        manuales no se pisan."""
        Ficha = self.env['qb.producto.ficha']
        Ficha.action_generar_fichas()
        rows = self._sb_get('product_uom_conversion')
        fichas = {f.product_id.id: f
                  for f in Ficha.with_context(active_test=False).search([])}
        n = 0
        for row in rows:
            ficha = fichas.get(row['odoo_product_id'])
            if not ficha or ficha.source == 'manual':
                continue
            vals = {}
            if row.get('gramaje_g_m2'):
                vals['gramaje_g_m2'] = float(row['gramaje_g_m2'])
            if row.get('ancho_m'):
                vals['ancho_m'] = float(row['ancho_m'])
            if row.get('m_per_kg'):
                vals['rendimiento_m_kg'] = float(row['m_per_kg'])
            if vals:
                vals['source'] = 'supabase'
                ficha.write(vals)
                n += 1
        return ('Fichas técnicas: generadas para vendibles; %s con '
                'gramaje/ancho curado de Supabase.' % n)

    # ------------------------------------------------------------------
    # UI / cron
    # ------------------------------------------------------------------
    def action_importar(self):
        self.ensure_one()
        self.resultado = self.run_import()
        return {
            'type': 'ir.actions.act_window',
            'res_model': self._name,
            'res_id': self.id,
            'view_mode': 'form',
            'target': 'new',
        }

    @api.model
    def cron_import_weekly(self):
        """Semanal: la config de Supabase (pesos nuevos, cuentas, centros)
        entra sola — y después recalcula el período para que los números
        del módulo queden alineados sin tocar nada."""
        try:
            self.run_import()
        except UserError as exc:
            _logger.warning('Import Supabase omitido: %s', exc)
            return
        self.env['qb.costo.producto'].action_recompute_period()

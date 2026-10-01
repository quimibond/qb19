# -*- coding: utf-8 -*-
"""Bloque 3 de formularios (propuesta de formatos aprobada por Jose el
2026-10-01; ``docs/sgi/transicion/formatos-bloque-3.md``).

Tres pasos, cada uno con su migración y en este orden:

1. **57.70.0 — Duplicados y datos malos** (propuesta §3): pasa las ligas de
   las actividades al formato que se conserva, da de baja el duplicado,
   quita «controlado» al registro lleno F-P-V01-04, libera F-P-E01-01 (la
   luminaria pasa a SST) y da de alta F-P-A28-13 como «Formulario de Odoo».
2. **57.71.0 — Responsable SGI = dueño del proceso** (propuesta §2).
3. **57.72.0 — Clave nueva D-02** con la del Dropbox como clave anterior.

Reglas de todas las migraciones de datos: idempotentes; dejan en el log el
valor anterior de lo que cambian; cada documento se localiza por id **y** se
verifica su clave antes de tocarlo (si no coincide, se salta con aviso: otra
base no recibe cambios equivocados); solo la empresa del SGI (D-03).

**Dar de baja ≠ archivar.** En Documentos (Odoo 18+), archivar manda el
documento a la papelera y la autolimpieza lo **borra** pasados
``documents.deletion_delay`` días (30 en producción). Para no perder datos,
el duplicado queda **activo y obsoleto** («Motivo de obsolescencia» con el
duplicado que lo sustituye) y con estado de migración «Baja tramitada».
"""
import logging

from odoo import api, fields, models

_logger = logging.getLogger(__name__)

# --- Paso 1 (57.70.0) ------------------------------------------------------
# (id duplicado, clave, id que se conserva o None, clave, motivo). El motivo
# queda en «Motivo de obsolescencia» y en el chatter.
SGI_B3_MERGES = (
    (4026, 'F-IT-P-P04-07-01', 3902, 'F-IT-P-C05-07-01',
     "Duplicado de F-IT-P-C05-07-01 (calibración del equipo Wesco): IT-P-P04-07 "
     "no existe y la calibración es de C5."),
    (3752, 'F-P-A23-08', 3774, 'F-P-A16-07',
     "Duplicado de F-P-A16-07 (nota de venta): P-A23 no tiene procedimiento "
     "vigente; P-A16 sí."),
    (4023, 'F-P-P04-02', 3920, 'F-P-C05-02',
     "Fusionado en F-P-C05-02 (solicitud de pruebas al laboratorio): P-P04 no "
     "tiene procedimiento vigente; la liga de C1.11 pasó a F-P-C05-02."),
    (3725, 'F-P-A13-01', 3867, 'F-P-A01-49',
     "Duplicado de F-P-A01-49 (lista de incidencias): P-A13 no tiene "
     "procedimiento vigente; F-P-A01-49 tiene actividad (S4.10) y menú."),
    (3943, 'F-P-C17-05', 3698, 'F-IT-P-A05-01-01',
     "La bitácora de vales de salida es la lista de traslados internos de Odoo; "
     "queda un solo formato, el vale F-IT-P-A05-01-01."),
    (3989, 'F-IT-P-P01-01-03', 3999, 'F-P-P02-01',
     "Duplicado de F-P-P02-01 en C4.14: la carda es un paso de entretelas y su "
     "orden ya imprime F-P-P02-01 (57.61.0)."),
    (4001, 'F-P-P01-01', None, None,
     "Orden de trabajo sin actividad, en la carpeta de Mantenimiento: si es la "
     "de mantenimiento duplica a F-P-M01-01; si es de producción, la orden vive "
     "en las claves por tipo de operación (F-P-P02-01, F-IT-P-P01-02-02, "
     "F-IT-P-P01-12-01)."),
)
# (numeral de la actividad, id del formato, clave) que se agrega a la actividad.
SGI_B3_LINKS = (
    ('C2.43', 3875, 'F-P-A28-04'),
    ('C6.12', 3698, 'F-IT-P-A05-01-01'),
    # Propuesta §1 #8: la evaluación de auditores internos también en E2.37.
    ('E2.37', 4063, 'F-IT-P-G03-01-01'),
)
# (id, clave, motivo): registros llenos que figuraban como formato controlado.
SGI_B3_UNCONTROL = (
    (5152, 'F-P-V01-04',
     "Es un reporte de visita LLENO (ficha FT-033-2026), no el formato en blanco: "
     "el formato es F-P-A28-04. Deja de ser documento controlado; no se borra."),
)
# (id, clave del Dropbox equivocada, clave corregida, motivo, numeral de la
# actividad a la que se liga si no tiene formatos).
SGI_B3_RECODE = (
    (4060, 'F-P-E01-01', 'F-P-S01-02',
     "La evaluación de luminarias (NOM-025, iluminación) es un estudio de higiene "
     "de SST, no la matriz de aspectos ambientales. F-P-E01-01 es la matriz que "
     "cita P-E01; la luminaria pasa a la familia P-S01 (identificación y "
     "evaluación de riesgos).",
     'E2.31'),
)
# Formatos citados que ya viven en Odoo y no tenían documento (propuesta §1).
# El PDF de sgi.sales.budget ya imprime F-P-A28-13 como clave alterna; la
# encuesta de satisfacción (F-P-A28-11) ya tiene menú en SGI → Dirección.
SGI_B3_ODOO_FORMS = (
    {
        'code': 'F-P-A28-13',
        'name': "F-P-A28-13 Pronóstico de ventas",
        'process': 'C2',
        'menu': 'quimibond_ventas_presupuesto.menu_sale_sgi_sales_forecast',
        'target': "Ventas → Presupuesto y pronóstico → Pronósticos",
        'activity': 'C2.40',
        'map_model': 'sgi.sales.budget',
        'reason': "Alta del bloque 3 (propuesta de formatos §1 #5 y #6): el pronóstico "
                  "vive en Odoo (sgi.sales.budget) y su PDF ya imprime esta clave. Una "
                  "sola clave de pronóstico para confección e industrial.",
    },
    {
        'code': 'F-P-A28-11',
        'name': "F-P-A28-11 Encuesta de satisfacción del cliente",
        'process': 'E2',
        'menu': 'quimibond_sgi.menu_sgi_satisfaction',
        'target': "SGI → Dirección → Satisfacción del cliente (encuesta anual)",
        'activity': 'E2.12',
        'map_model': None,
        'reason': "Alta del bloque 3 (propuesta de formatos §1 #1): la encuesta anual de "
                  "satisfacción vive en Encuestas y en SGI → Dirección → Satisfacción del "
                  "cliente. Falta configurarla en Ajustes del SGI (hoy "
                  "quimibond_sgi.satisfaction_survey_id = 0).",
    },
)

# --- Paso 2 (57.71.0) ------------------------------------------------------
# Tipos que cuentan como «formato» para el responsable (propuesta §2: 378).
SGI_FORMAT_DOC_TYPES = ('formato', 'formato_it', 'dat', 'anexo', 'formulario_odoo')


class DocumentsDocumentBloque3(models.Model):
    _inherit = 'documents.document'

    # --- utilidades ---------------------------------------------------------
    @api.model
    def _sgi_b3_company(self, company=None):
        return company or self.env['sgi.config']._sgi_company()

    @api.model
    def _sgi_b3_doc(self, doc_id, codes, company):
        """Documento ``doc_id`` (activo o archivado) si su clave o su clave
        anterior es una de ``codes`` y es de la empresa del SGI; si no, vacío
        con aviso en el log."""
        if isinstance(codes, str):
            codes = (codes,)
        doc = self.sudo().with_context(active_test=False).browse(doc_id).exists()
        if not doc:
            _logger.warning("SGI bloque 3: no existe el documento %s (%s); se salta.",
                            doc_id, "/".join(codes))
            return doc
        if doc.sgi_code not in codes and doc.sgi_previous_code not in codes:
            _logger.warning("SGI bloque 3: el documento %s tiene la clave %s (anterior %s), no "
                            "%s; se salta.", doc_id, doc.sgi_code, doc.sgi_previous_code,
                            "/".join(codes))
            return doc.browse()
        process_company = doc.sgi_process_id.company_id
        if (doc.company_id and doc.company_id != company) or \
                (process_company and process_company != company):
            _logger.warning("SGI bloque 3: el documento %s (%s) no es de %s; se salta.",
                            doc_id, doc.sgi_code, company.display_name)
            return doc.browse()
        return doc

    @api.model
    def _sgi_b3_activity(self, number, company):
        return self.env['sgi.process.activity'].sudo().with_context(active_test=False).search(
            [('number', '=', number), ('company_id', '=', company.id)])

    @api.model
    def _sgi_b3_other_references(self, doc):
        """Otros campos (guardados) que apuntan a ``doc`` fuera de las
        actividades y los mapeos de formato: {modelo.campo: n}. Solo se
        informan; el documento no se borra, así que la liga no se rompe."""
        found = {}
        skip = {('sgi.process.activity', 'format_document_ids'),
                ('sgi.format.map', 'document_id'), ('sgi.format.map', 'document_alt_id')}
        for model_name in self.env.registry:
            Model = self.env[model_name]
            if Model._abstract or Model._transient or not Model._auto:
                continue
            for field in Model._fields.values():
                if field.type not in ('many2one', 'many2many') or not field.store \
                        or field.comodel_name != 'documents.document' \
                        or (model_name, field.name) in skip:
                    continue
                try:
                    count = Model.sudo().with_context(active_test=False).search_count(
                        [(field.name, 'in', doc.ids)])
                except Exception:  # noqa: BLE001 - solo informa
                    continue
                if count:
                    found['%s.%s' % (model_name, field.name)] = count
        return found

    # --- paso 1: duplicados -------------------------------------------------
    @api.model
    def _sgi_b3_merge_duplicates(self, table=SGI_B3_MERGES, company=None):
        """Da de baja cada duplicado de ``table``: primero sus ligas en
        actividades pasan al documento que se conserva (si ya lo tenían, solo
        se quita el duplicado), luego queda obsoleto, con motivo y «Baja
        tramitada». Activo: no va a la papelera. Se salta (con aviso) el que
        imprime un mapeo de formato. Devuelve {id: {'actividades': [...],
        'antes': (...)}} de lo que cambió."""
        company = self._sgi_b3_company(company)
        Activity = self.env['sgi.process.activity'].sudo().with_context(active_test=False)
        Map = self.env['sgi.format.map'].sudo().with_context(active_test=False)
        done = {}
        for dup_id, dup_code, keep_id, keep_code, reason in table:
            dup = self._sgi_b3_doc(dup_id, dup_code, company)
            if not dup:
                continue
            keep = self.browse()
            if keep_id:
                keep = self._sgi_b3_doc(keep_id, keep_code, company)
                if not keep:
                    _logger.warning("SGI bloque 3: falta el documento que se conserva (%s) para %s; "
                                    "se salta.", keep_code, dup_code)
                    continue
            if dup.sgi_state == 'obsoleto':
                _logger.info("SGI bloque 3: %s (%d) ya está obsoleto («%s»); sin cambios.",
                             dup_code, dup.id, dup.sgi_obsolete_reason or '')
                continue
            maps = Map.search(['|', ('document_id', '=', dup.id), ('document_alt_id', '=', dup.id)])
            if maps:
                _logger.warning("SGI bloque 3: %s (%d) lo imprime el mapeo de formato %s; no se da de "
                                "baja (MAST decide).", dup_code, dup.id, maps.ids)
                continue
            moved = []
            for activity in Activity.search([('format_document_ids', 'in', dup.ids)]):
                commands = [(3, dup.id)]
                if keep and keep not in activity.format_document_ids:
                    commands.append((4, keep.id))
                before = ", ".join(activity.format_document_ids.mapped('sgi_code'))
                activity.write({'format_document_ids': commands})
                after = ", ".join(activity.format_document_ids.mapped('sgi_code'))
                moved.append("%s: %s → %s" % (activity.number, before or "vacío", after or "vacío"))
                _logger.info("SGI bloque 3: actividad %s (%d) formatos: %s → %s.",
                             activity.number, activity.id, before or "vacío", after or "vacío")
            others = self._sgi_b3_other_references(dup)
            if others:
                _logger.info("SGI bloque 3: %s (%d) sigue referido (no se toca): %s.",
                             dup_code, dup.id, others)
            before = (dup.sgi_state, dup.sgi_migration_state, dup.active)
            vals = {'sgi_state': 'obsoleto', 'sgi_obsolete_reason': reason}
            # Clase D exige «No aplica» (L-012): ahí el estado de migración no cambia.
            if dup.sgi_migration_class != 'd':
                vals['sgi_migration_state'] = 'baja'
            dup.write(vals)
            dup.message_post(body="Bloque 3 de formatos: dado de baja (obsoleto, sigue activo). %s "
                                  "Antes: estado %s, migración %s." % (reason, before[0], before[1]))
            _logger.info("SGI bloque 3: %s (%d) → obsoleto y baja (antes: estado %s, migración %s, "
                         "activo %s). Motivo: %s", dup_code, dup.id, before[0], before[1],
                         before[2], reason)
            done[dup.id] = {'actividades': moved, 'antes': before}
        return done

    @api.model
    def _sgi_b3_link_formats(self, table=SGI_B3_LINKS, company=None):
        """Agrega el formato a la actividad (sin quitar los que tenga).
        Devuelve {numeral: (antes, después)} de lo que cambió."""
        company = self._sgi_b3_company(company)
        done = {}
        for number, doc_id, code in table:
            doc = self._sgi_b3_doc(doc_id, code, company)
            if not doc:
                continue
            activities = self._sgi_b3_activity(number, company)
            if not activities:
                _logger.warning("SGI bloque 3: no existe la actividad %s; %s no se liga.", number, code)
                continue
            for activity in activities:
                if doc in activity.format_document_ids:
                    _logger.info("SGI bloque 3: %s ya tiene %s.", number, code)
                    continue
                before = ", ".join(activity.format_document_ids.mapped('sgi_code'))
                activity.write({'format_document_ids': [(4, doc.id)]})
                after = ", ".join(activity.format_document_ids.mapped('sgi_code'))
                done[number] = (before or False, after)
                _logger.info("SGI bloque 3: actividad %s (%d) formatos: %s → %s.",
                             number, activity.id, before or "vacío", after)
        return done

    @api.model
    def _sgi_b3_uncontrol(self, table=SGI_B3_UNCONTROL, company=None):
        """Quita «controlado» (y el estado, que solo llevan los controlados) a
        los registros llenos de ``table``. No los borra ni los archiva."""
        company = self._sgi_b3_company(company)
        done = {}
        for doc_id, code, reason in table:
            doc = self._sgi_b3_doc(doc_id, code, company)
            if not doc:
                continue
            if not doc.sgi_is_controlled:
                _logger.info("SGI bloque 3: %s (%d) ya no es controlado.", code, doc.id)
                continue
            Map = self.env['sgi.format.map'].sudo().with_context(active_test=False)
            if Map.search_count(['|', ('document_id', '=', doc.id), ('document_alt_id', '=', doc.id)]):
                _logger.warning("SGI bloque 3: %s (%d) lo imprime un mapeo de formato; sigue "
                                "controlado (MAST decide).", code, doc.id)
                continue
            activities = self.env['sgi.process.activity'].sudo().with_context(
                active_test=False).search([('format_document_ids', 'in', doc.ids)])
            if activities:
                _logger.warning("SGI bloque 3: %s (%d) está ligado a %s; sigue controlado (MAST "
                                "decide).", code, doc.id, ", ".join(activities.mapped('number')))
                continue
            before = (doc.sgi_is_controlled, doc.sgi_state)
            doc.write({'sgi_is_controlled': False, 'sgi_state': False})
            doc.message_post(body="Bloque 3 de formatos: deja de ser documento controlado. %s Antes: "
                                  "controlado, estado %s." % (reason, before[1]))
            _logger.info("SGI bloque 3: %s (%d) deja de ser controlado (antes: controlado %s, estado "
                         "%s).", code, doc.id, before[0], before[1])
            done[doc.id] = before
        return done

    @api.model
    def _sgi_b3_recode(self, table=SGI_B3_RECODE, company=None):
        """Corrige la clave del Dropbox de un documento mal clasificado: la
        clave y la clave anterior pasan a la corregida (así la clave
        equivocada queda libre para el formato que sí la lleva, y la búsqueda
        por clave anterior no lo confunde). La clave equivocada queda en el
        nombre del archivo, el chatter y el seguimiento. Si la actividad de
        ``number`` no tiene formatos, se liga."""
        company = self._sgi_b3_company(company)
        Doc = self.sudo().with_context(active_test=False)
        done = {}
        for doc_id, old, new, reason, number in table:
            doc = self._sgi_b3_doc(doc_id, (old, new), company)
            if not doc:
                continue
            if doc.sgi_previous_code == new:
                _logger.info("SGI bloque 3: %d ya tiene la clave corregida %s.", doc.id, new)
            else:
                taken = Doc.search(['|', ('sgi_code', '=', new), ('sgi_previous_code', '=', new),
                                    ('id', '!=', doc.id)])
                if taken:
                    _logger.warning("SGI bloque 3: la clave %s ya la usa %s; %d no cambia.",
                                    new, taken.ids, doc.id)
                    continue
                before = (doc.sgi_code, doc.sgi_previous_code)
                doc.write({'sgi_code': new, 'sgi_previous_code': new})
                doc.message_post(body="Bloque 3 de formatos: clave %s → %s (clave anterior %s → %s). "
                                      "%s" % (before[0], new, before[1], new, reason))
                _logger.info("SGI bloque 3: %d clave %s → %s, clave anterior %s → %s.",
                             doc.id, before[0], new, before[1], new)
                done[doc.id] = before
            if number:
                for activity in self._sgi_b3_activity(number, company):
                    if doc in activity.format_document_ids:
                        continue
                    if activity.format_document_ids:
                        _logger.info("SGI bloque 3: %s ya tiene formatos (%s); %s no se liga (MAST "
                                     "decide).", number, ", ".join(
                                         activity.format_document_ids.mapped('sgi_code')), new)
                        continue
                    activity.write({'format_document_ids': [(4, doc.id)]})
                    _logger.info("SGI bloque 3: actividad %s (%d) formatos: vacío → %s.",
                                 number, activity.id, doc.sgi_code)
        return done

    @api.model
    def _sgi_b3_register_odoo_forms(self, table=SGI_B3_ODOO_FORMS, company=None):
        """Da de alta como «Formulario de Odoo» (sin archivo: lo que se llena
        es la pantalla de Odoo) cada clave de ``table`` que no exista (ni
        como clave ni como clave anterior, activa o archivada). Liga la
        actividad si no tiene formatos y el mapeo del modelo que ya imprimía
        la clave sin documento. Devuelve {clave: id}."""
        company = self._sgi_b3_company(company)
        Doc = self.sudo().with_context(active_test=False)
        Type = self.env['sgi.document.type'].sudo()
        done = {}
        for spec in table:
            code = spec['code']
            existing = Doc.search(['|', ('sgi_code', '=', code), ('sgi_previous_code', '=', code)])
            if existing:
                _logger.info("SGI bloque 3: %s ya existe (%s); no se da de alta.", code, existing.ids)
                continue
            process = self.env['sgi.process'].sudo().search(
                [('code', '=', spec['process']), ('company_id', '=', company.id)], limit=1)
            dtype = Type.search([('code', '=', 'formulario_odoo'),
                                 ('company_id', 'in', (company.id, False))],
                                order='company_id', limit=1)
            if not process or not dtype:
                _logger.warning("SGI bloque 3: falta el proceso %s o el tipo «Formulario de Odoo»; "
                                "%s no se da de alta.", spec['process'], code)
                continue
            menu = self.env.ref(spec['menu'], raise_if_not_found=False) if spec.get('menu') else None
            doc = Doc.create({
                'name': spec['name'],
                'type': 'binary',
                'sgi_is_controlled': True,
                'sgi_doc_type_id': dtype.id,
                'sgi_code': code,
                'sgi_previous_code': code,
                'sgi_revision': 0,
                'sgi_state': 'vigente',
                'sgi_issue_date': fields.Date.context_today(self),
                'sgi_process_id': process.id,
                'sgi_migration_class': 'a',
                'sgi_migration_state': 'migrado',
                'sgi_migration_target': spec.get('target'),
                'sgi_odoo_menu_id': menu.id if menu else False,
            })
            doc.message_post(body=spec.get('reason') or "Alta del bloque 3 de formatos.")
            done[code] = doc.id
            _logger.info("SGI bloque 3: alta de %s (%d) como Formulario de Odoo de %s, menú %s.",
                         code, doc.id, process.code, menu.complete_name if menu else "sin menú")
            for activity in self._sgi_b3_activity(spec.get('activity'), company) \
                    if spec.get('activity') else []:
                if activity.format_document_ids:
                    _logger.info("SGI bloque 3: %s ya tiene formatos; %s no se liga.",
                                 activity.number, code)
                    continue
                activity.write({'format_document_ids': [(4, doc.id)]})
                _logger.info("SGI bloque 3: actividad %s (%d) formatos: vacío → %s.",
                             activity.number, activity.id, code)
            if spec.get('map_model'):
                Map = self.env['sgi.format.map'].sudo()
                for fmap in Map.search([('model_name', '=', spec['map_model'])]):
                    if fmap.sgi_code_alt == code and not fmap.document_alt_id:
                        fmap.write({'document_alt_id': doc.id})
                        _logger.info("SGI bloque 3: mapeo %d (%s) documento alternativo: vacío → %s.",
                                     fmap.id, spec['map_model'], code)
                    elif fmap.sgi_code == code and not fmap.document_id:
                        fmap.write({'document_id': doc.id})
                        _logger.info("SGI bloque 3: mapeo %d (%s) documento: vacío → %s.",
                                     fmap.id, spec['map_model'], code)
        return done

    @api.model
    def _sgi_formatos_bloque3(self, company=None):
        """Paso 1 completo, en el orden de la propuesta: ligas, bajas, registro
        lleno, clave equivocada y altas."""
        company = self._sgi_b3_company(company)
        return {
            'merges': self._sgi_b3_merge_duplicates(company=company),
            'links': self._sgi_b3_link_formats(company=company),
            'uncontrol': self._sgi_b3_uncontrol(company=company),
            'recode': self._sgi_b3_recode(company=company),
            'forms': self._sgi_b3_register_odoo_forms(company=company),
        }

    # --- paso 2: responsable SGI = dueño del proceso -------------------------
    @api.model
    def _sgi_owner_from_process(self, company=None, ids=None, types=SGI_FORMAT_DOC_TYPES):
        """Responsable SGI de cada formato vigente (o en piloto) = el usuario
        del dueño de su proceso (``sgi_process_id.owner_id.user_id``) si está
        activo y es interno. Solo toca los que hoy tiene el Jefe MAST (el
        custodio que puso 56.28.0): lo que alguien ya reasignó se respeta. Si
        el dueño no tiene usuario, el formato se queda con MAST. Fuera P-I01 y
        su familia. ``ids`` limita el alcance (pruebas). Devuelve
        ``{'changed': {proceso: (usuario, [ids])}, 'no_user': {proceso: [ids]},
        'mast': {proceso: [ids]}}``."""
        company = self._sgi_b3_company(company)
        result = {'changed': {}, 'no_user': {}, 'mast': {}}
        custodian = self.env['res.users'].sudo().browse(
            self.env['sgi.cron'].sudo()._sgi_manager_user_id() or []).exists()
        if not custodian:
            _logger.warning("SGI responsables: no hay Jefe MAST (custodio); no se cambia nada.")
            return result
        domain = [('sgi_is_controlled', '=', True), ('active', '=', True),
                  ('sgi_state', 'in', ('piloto', 'vigente')),
                  ('sgi_doc_type_id.code', 'in', list(types)),
                  ('sgi_owner_id', '=', custodian.id),
                  ('sgi_process_id.company_id', '=', company.id)] \
            + self._sgi_dropbox_excluded_domain()
        if ids is not None:
            domain.append(('id', 'in', list(ids)))
        docs = self.sudo().search(domain, order='id')
        for process in docs.mapped('sgi_process_id').sorted('code'):
            pdocs = docs.filtered(lambda d, p=process: d.sgi_process_id == p)
            employee = process.sudo().owner_id
            user = employee.user_id
            if user and user.active and not user.share and user != custodian:
                pdocs.write({'sgi_owner_id': user.id})
                result['changed'][process.code] = (user.id, pdocs.ids)
                _logger.info("SGI responsables: %s: %d formato(s) %s → %s (dueño %s). Ids: %s",
                             process.code, len(pdocs), custodian.name, user.name,
                             employee.name, pdocs.ids)
            elif user == custodian:
                result['mast'][process.code] = pdocs.ids
                _logger.info("SGI responsables: %s: %d formato(s) se quedan con %s (es la dueña "
                             "del proceso).", process.code, len(pdocs), custodian.name)
            else:
                result['no_user'][process.code] = pdocs.ids
                _logger.warning("SGI responsables: %s: el dueño %s no tiene usuario activo e "
                                "interno; %d formato(s) se quedan con %s: %s", process.code,
                                employee.name or "(sin dueño)", len(pdocs), custodian.name,
                                ", ".join(pdocs.mapped('sgi_code')))
        return result

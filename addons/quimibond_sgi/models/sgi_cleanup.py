# -*- coding: utf-8 -*-
"""Limpieza antes de producción (CEO, 2026-09-24; 19.0.45.0.0).

Regla: menús, acciones y vistas sin uso se borran; registros con datos se
archivan o se religan. Aquí vive lo que la migración y las pruebas comparten:

- El árbol de menús del SGI tiene CINCO entradas (más las dos que cuelgan de
  la app Calidad). `_sgi_menu_tree_offenders()` devuelve lo que sobra; la
  prueba falla si reaparece un menú fuera de ellas.
- Los documentos, riesgos e indicadores de los procesos P-*/MP-* viejos se
  religan al proceso nuevo (`_sgi_relink_from_archived`). Primera regla: un
  documento que está en «Procedimientos que sustituye» de un proceso nuevo
  va a ese proceso, con su familia (formatos que cuelgan de él), aunque el
  mapa diga otro. El mapa aplica solo a lo demás.
- Cuando un proceso nuevo pasa a «vigente», sus documentos sustituidos pasan
  solos a obsoletos: así los procedimientos viejos se retiran por ola,
  proceso por proceso, y nunca conviven dos «vigentes» para la misma gente.
"""
import logging

from odoo import models
from odoo.exceptions import ValidationError

_logger = logging.getLogger(__name__)

# Las cinco entradas del SGI. Todo menú bajo el raíz debe descender de una.
SGI_MENU_ENTRIES = (
    'quimibond_sgi.menu_sgi_panel',              # Inicio
    'quimibond_sgi.menu_sgi_processes',          # Procesos
    'quimibond_sgi.menu_sgi_improvement_group',  # Mejora
    'quimibond_sgi.menu_sgi_direction',          # Dirección
    'quimibond_sgi.menu_sgi_admin',              # Administración SGI
)
# Cuelgan de la app Calidad; el raíz del SGI es solo su respaldo si esa app
# no existe (ver ir.ui.menu._sgi_attach_quality_menus).
SGI_MENU_QUALITY_ENTRIES = (
    'quimibond_sgi.menu_sgi_automotive',
    'quimibond_sgi.menu_sgi_dashboards',
)

# xmlids retirados en 19.0.45.0.0. La prueba falla si alguno vuelve a existir.
SGI_REMOVED_XMLIDS = (
    # Menús: los hijos de Datos técnicos y luego el padre (parent_id es
    # ondelete=restrict: el orden importa al borrar en la migración)
    'quimibond_sgi.menu_sgi_activities',
    'quimibond_sgi.menu_sgi_activity_roles',
    'quimibond_sgi.menu_sgi_deliverables',
    'quimibond_sgi.menu_sgi_activity_chain',
    'quimibond_sgi.menu_sgi_flows',
    'quimibond_sgi.menu_sgi_my_activities',
    'quimibond_sgi.menu_sgi_my_actions',
    'quimibond_sgi.menu_sgi_my_procedures',
    'quimibond_sgi.menu_sgi_admin_data',
    # Menús: los otros dos «Mis…»
    'quimibond_sgi.menu_sgi_my_measures',
    'quimibond_sgi.menu_sgi_my_acks',
    # Menús de Diagnóstico que el CEO retiró
    'quimibond_sgi.menu_sgi_analysis_trend',
    'quimibond_sgi.menu_sgi_nc_concentrado',
    # Acciones que solo colgaban de esos menús
    'quimibond_sgi.sgi_activity_role_action',
    'quimibond_sgi.sgi_deliverable_action',
    'quimibond_sgi.sgi_activity_link_action',
    'quimibond_sgi.sgi_process_flow_action',
    'quimibond_sgi.sgi_process_activity_action_mine',
    'quimibond_sgi.sgi_action_line_action_mine',
    'quimibond_sgi.sgi_document_action_my_procedures',
    'quimibond_sgi.sgi_measure_action_mine',
    'quimibond_sgi.sgi_document_ack_action_mine',
    'quimibond_sgi.sgi_activity_exec_stat_action_trend',
    'quimibond_sgi.sgi_nc_action_concentrado',
    # Acciones huérfanas (sin menú ni botón)
    'quimibond_sgi.sgi_process_action_panel',
    'quimibond_sgi.sgi_audit_finding_action',
    'quimibond_sgi.sgi_sales_budget_line_action',
    # Vistas que solo usaban esas acciones
    'quimibond_sgi.sgi_nc_concentrado_view_list',
    'quimibond_sgi.sgi_activity_exec_stat_view_graph',
    # 47.1.0: el Diagnóstico dejó de ser un wizard HTML; su acción y su
    # formulario viejos se retiran (el menú abre la lista de hallazgos)
    'quimibond_sgi.sgi_diagnostic_action',
    'quimibond_sgi.sgi_diagnostic_view_form',
)

# Mapa de religado (CEO, 2026-09-24): proceso viejo → proceso nuevo. Aplica
# a lo que NO esté en los documentos sustituidos de un proceso nuevo.
SGI_RELINK_MAP = {
    'MP-DIS': 'C1', 'P-DYD': 'C1',
    'P-VEN': 'C2', 'P-LOG': 'C2',            # embarques y OTIF pasaron a C2
    'P-PLA': 'C3',
    'MP-MFG': 'C4', 'P-PTAC': 'C4', 'P-PENT': 'C4', 'P-TIN': 'C4',
    'MP-CAL': 'C5', 'P-LAB': 'C5', 'P-INS': 'C5',
    'P-AMP': 'C6', 'P-APT': 'C6',
    'P-COM': 'S1',
    'P-FAC': 'S2', 'P-CXC': 'S2',
    'MP-ADM': 'S3', 'P-FIN': 'S3',          # MP-ADM: default, se revisa uno por uno
    'P-RH': 'S4',
    'MP-MTO': 'S5', 'P-MTO': 'S5',
    'P-TI': 'S6',
    'P-DIR': 'E1',
    'P-SGI': 'E2',
}
# Procesos viejos cuyos documentos se listan en el log para revisión manual.
SGI_RELINK_REVIEW = ('MP-ADM',)


class SgiMenuCleanup(models.Model):
    _inherit = 'ir.ui.menu'

    def _sgi_menu_tree_offenders(self):
        """Menús bajo el raíz del SGI que no descienden de una de las cinco
        entradas (ni de las dos de Calidad cuando cuelgan del raíz por
        respaldo). Vacío = el árbol está limpio."""
        root = self.env.ref('quimibond_sgi.menu_sgi_root', raise_if_not_found=False)
        if not root:
            return self.browse()
        allowed = self.browse()
        for xmlid in SGI_MENU_ENTRIES + SGI_MENU_QUALITY_ENTRIES:
            menu = self.env.ref(xmlid, raise_if_not_found=False)
            if menu:
                allowed |= menu
        menus = self.sudo().with_context(active_test=False).search(
            [('parent_id', 'child_of', root.id), ('id', '!=', root.id)])
        offenders = self.browse()
        for menu in menus:
            top = menu
            while top.parent_id and top.parent_id != root:
                top = top.parent_id
            if top not in allowed:
                offenders |= menu
        return offenders

    def _sgi_menu_dangling_actions(self):
        """Menús bajo el raíz del SGI cuya acción ya no existe. Odoo no vacía
        `action` cuando el menuitem deja de traerla y el menú abre con «El
        registro no existe» (pasó con Inicio en 46.0.0)."""
        root = self.env.ref('quimibond_sgi.menu_sgi_root', raise_if_not_found=False)
        if not root:
            return self.browse()
        menus = self.sudo().with_context(active_test=False).search(
            [('parent_id', 'child_of', root.id)])
        return menus.filtered(lambda m: m.action and not m.action.exists())


class SgiProcessCleanup(models.Model):
    _inherit = 'sgi.process'

    # --- Punto 5: al poner vigente el proceso, lo que sustituye queda obsoleto
    def write(self, vals):
        res = super().write(vals)
        if vals.get('state') == 'vigente':
            self._sgi_obsolete_replaced_documents()
        return res

    def _sgi_obsolete_replaced_documents(self):
        """Los documentos en «Procedimientos que sustituye» de un proceso
        vigente pasan a obsoletos. Idempotente: solo toca los vigentes."""
        for process in self.filtered(lambda p: p.state == 'vigente'):
            docs = process.replaced_document_ids.filtered(lambda d: d.sgi_state == 'vigente')
            if not docs:
                continue
            for doc in docs:
                doc.sudo().write({'sgi_state': 'obsoleto'})
                doc.message_post(body=(
                    "Obsoleto: lo sustituye el proceso %s, que entró en vigor." % process.display_name))
            process.message_post(body=(
                "Al entrar en vigor quedaron obsoletos %d documento(s) sustituido(s): %s." % (
                    len(docs), ", ".join(docs.mapped(lambda d: d.sgi_code or d.name)))))
            _logger.info("SGI 45: %s vigente → %d documento(s) sustituido(s) obsoleto(s).",
                         process.code, len(docs))
        return True

    # --- Religado de los procesos viejos al mapa nuevo
    def _sgi_relink_from_archived(self, mapping=None, review_codes=None):
        """Religa documentos, riesgos e indicadores de los procesos archivados
        al proceso nuevo. Devuelve un resumen {clave: conteo} y escribe en el
        log qué se movió; los documentos de `review_codes` se listan uno por
        uno para revisión manual. No archiva nada.

        Se arma primero el plan completo y se escribe por destino: una clave
        (todas sus revisiones) va a UN solo proceso, porque la restricción
        `_check_sgi_code_family` no permite que una clave viva en dos
        procesos, ni siquiera un instante durante la migración."""
        mapping = SGI_RELINK_MAP if mapping is None else mapping
        review_codes = SGI_RELINK_REVIEW if review_codes is None else review_codes
        Process = self.sudo().with_context(active_test=False)
        Document = self.env['documents.document'].sudo().with_context(active_test=False)
        Risk = self.env['sgi.risk'].sudo().with_context(active_test=False)
        Indicator = self.env['sgi.indicator'].sudo().with_context(active_test=False)
        summary = {'documentos': 0, 'documentos_regla_sustituidos': 0,
                   'documentos_por_familia': 0, 'riesgos': 0, 'indicadores': 0,
                   'sin_destino': 0, 'no_movidos': 0}

        new_by_code = {p.code: p for p in Process.search([('active', '=', True)])}
        old_by_code = {p.code: p for p in Process.search(
            [('active', '=', False), ('code', 'in', list(mapping))])}

        # 1. Plan por mapa: documento → proceso nuevo.
        plan = {}
        origin = {}  # doc id → clave del proceso viejo (para el log)
        for old_code, new_code in mapping.items():
            old = old_by_code.get(old_code)
            target = new_by_code.get(new_code)
            if not old:
                continue
            if not target:
                _logger.warning("SGI 45: el proceso nuevo %s no existe; %s se queda como está.",
                                new_code, old_code)
                summary['sin_destino'] += 1
                continue
            for doc in Document.search([('sgi_process_id', '=', old.id)]):
                plan[doc.id] = target
                origin[doc.id] = old_code
            risks = Risk.search([('process_id', '=', old.id)])
            if risks:
                risks.write({'process_id': target.id})
                summary['riesgos'] += len(risks)
            indicators = Indicator.search([('process_id', '=', old.id)])
            if indicators:
                indicators.write({'process_id': target.id})
                summary['indicadores'] += len(indicators)
            _logger.info("SGI 45: %s → %s: %d documento(s), %d riesgo(s), %d indicador(es).",
                         old_code, new_code, len([d for d in origin if origin[d] == old_code]),
                         len(risks), len(indicators))

        # 2. Primera regla: lo sustituido (y su familia) va a quien lo
        #    sustituye, diga lo que diga el mapa.
        forced = {}
        for process in new_by_code.values():
            replaced = process.replaced_document_ids
            if not replaced:
                continue
            family = replaced | Document.search(
                [('sgi_parent_document_id', 'in', replaced.ids)])
            for doc in family:
                forced.setdefault(doc.id, process)
        for doc_id, dest in forced.items():
            current = plan.get(doc_id) or Document.browse(doc_id).sgi_process_id
            if current != dest:
                summary['documentos_regla_sustituidos'] += 1
            plan[doc_id] = dest

        # 3. Una clave, un destino: todas las revisiones de una clave viajan
        #    juntas. Gana la regla de sustituidos; si no, un proceso nuevo
        #    donde ya viva alguna revisión; si no, el destino de la vigente;
        #    si no, el primero del plan.
        docs = Document.browse(list(plan))
        seen_codes = set()
        for doc in docs.filtered(lambda d: d.sgi_is_controlled and d.sgi_code):
            key = (doc.sgi_code, doc.company_id.id)
            if key in seen_codes:
                continue
            seen_codes.add(key)
            siblings = doc | doc._sgi_same_code_docs().filtered('sgi_is_controlled')
            dest = None
            for sib in siblings:
                if sib.id in forced:
                    dest = forced[sib.id]
                    break
            if dest is None:
                for sib in siblings:
                    if sib.id not in plan and sib.sgi_process_id.active:
                        dest = sib.sgi_process_id
                        break
            if dest is None:
                for sib in siblings:
                    if sib.id in plan and sib.sgi_state == 'vigente':
                        dest = plan[sib.id]
                        break
            if dest is None:
                dest = next(plan[sib.id] for sib in siblings if sib.id in plan)
            for sib in siblings:
                if plan.get(sib.id) != dest:
                    if sib.id in plan:
                        _logger.info("SGI 45: %s [%s] sigue a su familia: → %s en vez de %s.",
                                     sib.sgi_code, sib.sgi_revision_label or '-', dest.code,
                                     plan[sib.id].code)
                    else:
                        _logger.info("SGI 45: %s [%s] (en %s) sigue a su familia → %s.",
                                     sib.sgi_code, sib.sgi_revision_label or '-',
                                     sib.sgi_process_id.display_name or 'sin proceso', dest.code)
                    summary['documentos_por_familia'] += 1
                    plan[sib.id] = dest

        # 4. Lo que no cumpliría la nomenclatura de su tipo con el proceso
        #    nuevo se queda (con toda su clave) y se avisa: se corrige a mano.
        blocked = set()
        for doc in Document.browse(list(plan)):
            dtype = doc.sgi_doc_type_id
            if not doc.sgi_is_controlled or not doc.sgi_code or not dtype or not dtype.code_required:
                continue
            if not dtype._sgi_code_ok(doc.sgi_code, plan[doc.id]):
                blocked.add((doc.sgi_code, doc.company_id.id))
                _logger.warning("SGI 45: NO MOVIDO %s [%s] %s: la clave no cumple la nomenclatura "
                                "del tipo «%s» con el proceso %s.", doc.sgi_code,
                                doc.sgi_state or '-', doc.name, dtype.name, plan[doc.id].code)
        for doc_id in list(plan):
            doc = Document.browse(doc_id)
            if (doc.sgi_code, doc.company_id.id) in blocked:
                del plan[doc_id]
                summary['no_movidos'] += 1

        # 5. Escribir por destino, de una vez. Si una restricción previa de la
        #    base (una clave repartida en dos tipos, por ejemplo) rechaza el
        #    grupo, se intenta documento por documento y lo que no pasa se
        #    queda y se avisa; el build nunca se cae por datos viejos.
        by_dest = {}
        for doc_id, dest in plan.items():
            by_dest.setdefault(dest, []).append(doc_id)
        for dest, ids in by_dest.items():
            group = Document.browse(ids).filtered(lambda d, dest=dest: d.sgi_process_id != dest)
            if not group:
                continue
            for doc in group:
                if origin.get(doc.id) in review_codes:
                    _logger.info("SGI 45: REVISAR %s → %s: %s [%s] %s", origin[doc.id],
                                 dest.code, doc.sgi_code or '-', doc.sgi_state or '-', doc.name)
            moved = self._sgi_relink_write(group, dest, summary)
            if moved:
                dest.message_post(body=(
                    "Limpieza 19.0.45.0.0: se religaron %d documento(s) de los procesos "
                    "anteriores a este proceso." % moved))
        return summary

    def _sgi_relink_write(self, group, dest, summary):
        """Escribe el grupo en un savepoint; si falla, uno por uno."""
        try:
            with self.env.cr.savepoint():
                group.write({'sgi_process_id': dest.id})
        except ValidationError as exc:
            _logger.warning("SGI 45: el grupo de %d documento(s) hacia %s no pasó completo (%s); "
                            "se intenta uno por uno.", len(group), dest.code,
                            str(exc).splitlines()[0])
            moved = 0
            for doc in group:
                try:
                    with self.env.cr.savepoint():
                        doc.write({'sgi_process_id': dest.id})
                    moved += 1
                except ValidationError as exc2:
                    summary['no_movidos'] += 1
                    _logger.warning("SGI 45: NO MOVIDO %s [%s] %s → %s: %s", doc.sgi_code or '-',
                                    doc.sgi_state or '-', doc.name, dest.code,
                                    str(exc2).splitlines()[0])
            summary['documentos'] += moved
            return moved
        summary['documentos'] += len(group)
        return len(group)

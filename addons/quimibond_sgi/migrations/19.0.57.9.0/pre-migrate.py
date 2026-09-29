# -*- coding: utf-8 -*-
"""57.9.0 (entrega 2, e2-dependencias; A-010 con D-10, A-014). Solo SQL sobre
``ir_model_data`` (más marcar dos módulos para instalar), idempotente y sin
borrar nada. El procedimiento está en ``migrations/mudanza.py``.

- **quimibond_sgi_studio** (A-010, D-10): la regla de aprobación de Studio del
  rol «Aprueba» (``sgi.activity.role.approval_rule_id``,
  ``approval_conflict_rule_ids``, la herencia de ``studio.approval.rule`` con
  ``sgi_role_id`` y el cierre de avisos al archivar una regla). Producción el
  2026-09-29 (MCP): 11 roles con ``approval_rule_id`` y 11 reglas activas con
  ``sgi_role_id`` (creadas ese día); 1,072 roles, todos ``approval_kind =
  'boton'``. Los datos se quedan en sus columnas; el satélite las vuelve a
  declarar.
- **quimibond_sgi_knowledge** (A-014): DOC-5, instructivo en Conocimiento.
  Producción el 2026-09-29: 0 actividades con ``instruction_article_id``, 0
  documentos con ``sgi_article_id`` y ningún otro campo de ``sgi.*``,
  ``documents.document``, ``hr.employee`` o ``hr.job`` apunta a
  ``knowledge.article`` (condición de la decisión 5).

Los dos satélites se marcan para instalar en este mismo update si están
``web_studio`` y ``knowledge`` (en producción, los dos instalados). Sin la
dependencia no hay datos que mudar y el satélite no se instala.

Reversa: ``UPDATE ir_model_data SET module = 'quimibond_sgi' WHERE module IN
('quimibond_sgi_studio', 'quimibond_sgi_knowledge')`` con el satélite sin
instalar y el código de 57.8.0.
"""
import importlib.util
import os

_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'mudanza.py')
_spec = importlib.util.spec_from_file_location('quimibond_sgi_mudanza_57_9', _path)
mudanza = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mudanza)

STUDIO = {
    'modelos': ['studio.approval.rule'],  # el núcleo deja de heredarlo: ir.model, sgi_role_id, id, display_name
    'campos': ['sgi.activity.role.approval_rule_id', 'sgi.activity.role.approval_conflict_rule_ids'],
}
KNOWLEDGE = {
    'modelos': ['sgi.instruction.publish'],
    'campos': ['sgi.process.activity.instruction_article_id',
               'sgi.process.activity.instruction_article_stale',
               'documents.document.sgi_article_id'],
    'nombres': ['sgi_process_activity_view_form_knowledge',
                'report_knowledge_instruction_document',
                'action_report_knowledge_instruction'],
}


def migrate(cr, version):
    if not version:
        return
    tag = "SGI 57.9.0"
    mudanza.mover(cr, 'quimibond_sgi_studio', tag + " (Studio)", **STUDIO)
    mudanza.mover(cr, 'quimibond_sgi_knowledge', tag + " (Conocimiento)", **KNOWLEDGE)
    mudanza.faltantes(cr, 'quimibond_sgi_knowledge', KNOWLEDGE['nombres'], tag)
    mudanza.instalar(cr, 'quimibond_sgi_studio', ['web_studio'], tag)
    mudanza.instalar(cr, 'quimibond_sgi_knowledge', ['knowledge'], tag)

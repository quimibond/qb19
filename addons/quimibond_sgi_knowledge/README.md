# Quimibond SGI - Instructivos en Conocimiento

Satélite de `quimibond_sgi` (`auto_install` con `knowledge`). Salió del
núcleo en 57.9.0 (auditoría A-014).

- **Qué trae:** DOC-5. `sgi.process.activity.instruction_article_id` (y
  `instruction_article_stale`), `documents.document.sgi_article_id`, el
  asistente `sgi.instruction.publish` y el reporte PDF del artículo.
- **Producción el 2026-09-29 (MCP):** 0 actividades con artículo, 0
  documentos con artículo y ningún otro campo de un modelo `sgi.*`,
  `documents.document`, `hr.employee` o `hr.job` apunta a
  `knowledge.article` (condición de la decisión 5 para sacarlo del núcleo).
- **Mudanza:** `quimibond_sgi/migrations/19.0.57.9.0/pre-migrate.py` pasa sus
  XML IDs a este módulo (`ir_model_data.module`, sin borrar nada) y lo marca
  para instalar en el mismo update.
- **Pruebas:** `tests/test_instruction_knowledge.py` (antes
  `quimibond_sgi/tests/test_pr6_external.py`, test_05).

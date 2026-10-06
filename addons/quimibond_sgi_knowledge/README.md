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

## 1.2.0 (2026-10-06): el SGI en Conocimiento

Plan: `docs/superpowers/plans/2026-10-06-sgi-conocimiento.md`. El CEO aceptó
las opciones por omisión de las decisiones 1 a 6 y de N1 a N6
(`docs/audit/decisiones.md`, 2026-10-06). **No toca `quimibond_sgi`.**

- **Espacio «SGI» en Conocimiento** (`knowledge.article._sgi_kb_seed`, desde
  el `<function>` de `data/sgi_knowledge_data.xml` en cada instalación y
  actualización): raíz «SGI» (todo usuario interno lee, el Jefe MAST
  escribe), «Cómo usar el sistema» con los 9 manuales (los 7 de `docs/sgi`,
  más `primeros-pasos.md` y `glosario.md`), un artículo por proceso de la
  empresa del SGI (su dueño escribe) y «Reglamentos». Los manuales son
  material de consulta, no documentos controlados.
- **Fuente de verdad = código (opción A):** el HTML de los manuales lo genera
  `python3 tools/sgi_knowledge_html.py` en `data/manuales/` (el CI corre
  `--check`). Cada siembra refresca un artículo solo si nadie lo editó: se
  guarda la huella del **texto plano** después de escribir
  (`sgi_seed_hash`); si el texto actual ya no coincide, no se toca y queda un
  mensaje una vez por versión. Un artículo archivado no se reactiva ni se
  recrea.
- **N1:** el espacio «SGI» de 2025 (raíz de espacio de trabajo sin clave de
  siembra) se renombra «SGI (estructura 2025, sin uso)» con una nota; no se
  mueve, no se archiva y no cambian sus permisos.
- **Ayuda** (SGI → Inicio → Ayuda) abre el manual del rol: Jefe MAST,
  Dirección, Auditor, RH, dueño de proceso o captura de eficiencias, y si no,
  el del operador. Ligas «Abrir en Conocimiento» / «Publicar desde
  Conocimiento» en la ficha del documento, «Artículo en Conocimiento» en la
  del proceso y «Leer en Conocimiento» en la tarjeta y las listas de Mi
  procedimiento (solo cuando el instructivo ya está publicado; campo de
  pantalla, fuera de la huella y del PDF).
- **Importador** (`sgi.knowledge.import`, SGI → Administración → Transición
  → Importar documentos a Conocimiento, y la acción «Importar a Conocimiento
  (SGI)» en la lista de Documentos; solo Jefe MAST): por lotes (10 por
  omisión), idempotente por clave, cada documento en su savepoint. Crea un
  borrador bajo el proceso (o «Reglamentos») con el texto del PDF
  (`index_content` de attachment_indexation o el lector de PDF de Odoo; más
  de 40 páginas u 8 MB, solo la liga), el PDF vigente adjunto, la liga
  `documents.document.sgi_article_id` (N3) y `instruction_article_id` en las
  actividades que ya usan la clave. **Nunca** escribe `instruction_id`, ni la
  revisión, el estado, el archivo o el nombre del documento. Salta la familia
  P-I01 (N6, «excluido por L-001») y lo restringido (`access_internal` distinto
  de `view`). El resumen va al asistente y al chatter de la raíz.
- **N4:** el borrador importado está desincronizado del padre, con permiso
  interno «Solo miembros» y como miembros el dueño del proceso y el Jefe MAST
  (el cron diario agrega al Jefe MAST nuevo). Al publicar vuelve a heredar
  del padre (lo leen todos) y se bloquea.
- **Publicar (N5, mismo camino que DOC-5):** `sgi.instruction.publish` sirve
  para instructivo, control operacional, protocolo y reglamento. Copia del
  vigente los campos de la revisión del flujo formal más la clave anterior,
  su fecha y el documento padre; conserva el título (el MIID no ve un cambio
  de título; solo sube la revisión de un CO); puestos: los de las actividades
  que usan la clave, si no los del vigente y, en CO, protocolos y
  reglamentos, los de las actividades del proceso; re-apunta **todas** las
  actividades que usaban una revisión anterior. «Pedir publicación» (dueño)
  deja una actividad al Jefe MAST (clave `kb_publicar`).
- **Huellas:** ligar `instruction_article_id` ya no marca el procedimiento vivo
  como modificado (se agregó a `_SGI_MEASURE_FIELDS`). Las pruebas
  `test_kb_hashes` comprueban que sembrar, importar y ligar no cambian la
  huella de Mi procedimiento ni la del MIID, y que publicar un CO solo sube
  su revisión en el MIID.
- **Cron** «SGI: Conocimiento (miembros y artículos que cambiaron)», diario
  03:40 UTC: agrega miembros (nunca quita: quitar a quien dejó de ser Jefe
  MAST o dueño lo hace a mano el administrador de Conocimiento) y avisa al
  Jefe MAST de cada artículo publicado que cambió (clave `kb_cambio`).
- **Migración** `migrations/19.0.1.2.0/post-migrate.py`: vuelve a sembrar,
  sincroniza miembros y deja una actividad al Jefe MAST
  (`kb_conocimiento_120`). No importa documentos.
- **Advertencia:** en Conocimiento, «Mover a la papelera» borra el artículo a
  los días. El código nunca usa la papelera ni archiva.
- **Cifras de producción (2026-10-06):** 226 artículos; el espacio de 2025 es
  el id 102 (20 artículos casi vacíos); 63 documentos vigentes de los cuatro
  tipos (49 IT, 5 CO, 5 PROT, 4 reglamentos), 6 de la familia P-I01 (57
  importables); 16 documentos ligados a 17 actividades.
- **Pruebas:** `test_kb_seed`, `test_kb_import`, `test_kb_hashes`,
  `test_kb_publish`, `test_kb_ligas`, `test_kb_usted`. Corren en el build de
  la rama con `--test-tags /quimibond_sgi_knowledge,/quimibond_sgi`.
- **Por verificar en el build** (sección 9 del plan): el formato de la ruta
  `/knowledge/article/<id>`, que abrir un manual en el editor no reescriba el
  cuerpo, que `index_content` de los PDF esté lleno y el lector de PDF de
  Odoo 19, y que la ficha de Conocimiento abra bien desde la lista
  «Conocimiento del SGI».
- **Desviaciones respecto al plan:** (1) N1 se revisa en cada siembra (no
  solo al crear la raíz): cualquier raíz de espacio de trabajo llamada
  exactamente «SGI» y sin clave de siembra se renombra; así la prueba corre
  igual en una base donde la raíz ya existe. (2) Ligar `instruction_article_id`
  se agregó a `_SGI_MEASURE_FIELDS` de la actividad: sin eso, el importador
  marcaba el procedimiento vivo como modificado (G14) en cada proceso con IT
  ligado. (3) Los campos SGI del artículo (`sgi_kind`, `sgi_seed_key`,
  `sgi_document_code`…) solo los escriben el sistema o el Jefe MAST
  (`sgi_bypass_allowed`, sin contexto de excepción). (4) Miembros de la raíz:
  todo usuario interno activo de Jefe MAST, incluidos los que lo tienen por
  implicación (Administrador SGI); pueden ser más de dos. (5) El bloqueo al
  publicar aplica a los artículos del espacio SGI; un artículo de DOC-5 fuera
  del espacio solo recibe la clave. (6) Un documento nuevo (sin revisión
  anterior) ya no lleva «(Rev. NN)» en el nombre: las revisiones siguientes
  copian el título. (7) «Importar documentos a Conocimiento» va con secuencia
  25 en Transición (20 ya era «Empresa en documentos controlados»). (8) La lista
  «Conocimiento del SGI» abre con «Primero» y «Por publicar».

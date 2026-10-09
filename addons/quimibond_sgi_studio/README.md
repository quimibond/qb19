# Quimibond SGI - Aprobaciones de Studio

Satélite de `quimibond_sgi` (`auto_install` con `web_studio`). Salió del
núcleo en 57.9.0 (auditoría A-010, decisión D-10 de Jose).

- **Qué trae:** el tipo «Botón de Odoo» del rol «Aprueba»: la regla
  `studio.approval.rule` que bloquea el botón del documento, con las
  personas del puesto como aprobadores (`sgi.activity.role.approval_rule_id`,
  `studio.approval.rule.sgi_role_id`), la detección de otra regla en el mismo
  botón y «Adoptar regla», las aprobaciones dadas (`studio.approval.entry`) y
  el cierre de avisos al archivar una regla (antes `sgi_approval_rule_archive`).
- **Qué se queda en el núcleo:** el tipo, el documento, el botón, la
  condición, las solicitudes de Aprobaciones, las firmas de Sign, el cron
  nocturno, el menú «Aprobaciones del SGI» y las aprobaciones de Studio en
  Mis pendientes (leídas solo si Studio está instalado).
- **Mudanza:** `quimibond_sgi/migrations/19.0.57.9.0/pre-migrate.py` pasa los
  XML IDs de los 3 campos a este módulo (`ir_model_data.module`, sin borrar
  nada) y lo marca para instalar en el mismo update. Producción el
  2026-09-29: 11 roles con regla y 11 reglas activas con rol.
- **Pruebas:** `tests/test_approval_studio.py` y
  `tests/test_approval_rule_archive.py` (se mudaron del núcleo).
- **19.0.1.0.6 (2026-10-09): nadie aprueba lo que él mismo pidió.** La
  entrada de aprobación de Studio (`studio.approval.entry`) de una regla con
  rol del SGI no se crea si quien aprueba es quien pidió el registro
  (`sgi.activity.role._sgi_requester_conflict`, campos explícitos como
  «Solicitó» o «Elaboró», nunca `create_uid`); el mensaje dice a quién le
  toca: el titular o el **suplente** nombrado en el rol (57.143.0 del núcleo),
  que la regla incluye entre sus aprobadores. Prueba:
  `tests/test_approval_studio.py::TestApprovalStudioRequester`.
- **19.0.1.0.3 (2026-10-01): ninguna regla guarda un campo que su documento
  no tiene.** `studio.approval.rule` limpia `domain` al crear y escribir con
  `sgi_sanitize_domain` del núcleo, y la regla del rol se arma con
  `_sgi_clean_approval_domain()`. Motivo: las reglas 65 (`sgi.audit.program`),
  66 (`sgi.ppap`) y 67 (`sgi.control.plan`) heredaron `[('company_id', '=', 1)]`
  de su rol y Studio reventaba al abrir esas fichas. La migración
  (`migrations/19.0.1.0.3/post-migrate.py`) revisa todas las reglas, quita
  solo las hojas inválidas y registra antes → después; en producción, 65, 66
  y 67 quedan sin condición y 64 (`budget.analytic`) no cambia. Prueba:
  `test_approval_studio.test_07`.

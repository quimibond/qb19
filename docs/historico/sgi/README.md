# Documentos históricos del SGI

Archivados el 2026-09-30 (auditoría de documentación, hallazgos K-001, K-002,
K-005…K-011). **Ninguno describe el SGI actual.** Se conservan como historia
del diseño; no se corrigen. La documentación vigente está en `docs/sgi/`
(manuales por rol, administración, transición y técnica generada) y en
`addons/quimibond_sgi/README.md` y `CHANGELOG.md`.

| Documento | Versión que describe | Por qué se archivó | Qué se rescató y dónde |
|---|---|---|---|
| `SGI_BLUEPRINT.md` | antes de 19.0.1 (jul-2026) | Plan original. El principio 5 (claves del Dropbox en pantalla) va contra la decisión 1; «cero Studio» ya se resolvió con el satélite `quimibond_sgi_studio` | Principios 1, 3, 4 y 6 como historia del diseño |
| `SGI_MANUAL_TECNICO.md` | 19.0.4.1.0 | 16 dependencias en vez de las actuales, 4 grupos, «el Auditor implica Usuario» (falso), 10 crons, ramas y reglas de versión viejas | Lo técnico ahora se genera: `docs/sgi/tecnica/` (`tools/sgi_docs.py`) |
| `SGI_MANUAL_USUARIO.md` | ≈19.0.13 | 11 rutas de menú que no existen, búsqueda por clave del Dropbox, checklist de auditoría en Encuestas | Manuales por rol en `docs/sgi/usuarios/` (FAQ y regla de convivencia incluidas) |
| `SGI_DEUDA_TECNICA.md` | 19.0.14.0.0 | Mezcla cerrados y abiertos; «sin `ir.rule`» ya es falso | Los abiertos (B.16, B.18, B.19, C.22, C.24, C.25, D.29) se cotejan en `docs/audit/99-consolidado.md` |
| `SGI_EVALUACION_ESTRUCTURA.md` | 19.0.14.1.0 | Menú objetivo superado por la decisión 2 | Tabla de «bucles que cierran» como historia |
| `SGI_AUDITORIA_OLAS_AB.md` | 19.0.9.1.0 | Cerrado | — |
| `SGI_PENDIENTES_PROGRAMACION.md` | hasta 53.1.0 | Los 29 puntos están hechos | «Listo cuando» como casos de prueba de los manuales |
| `SGI_DIAGRAMA_FLUJOS.html` | 19.0.18.0.0 | 17 crons, grupos en cadena lineal (falso), menús de antes de la decisión 2 | — |
| `AUDITORIA_FUNCIONAL_SGI_2026-09-28.md` | 56.6.2/56.7.0 (prod. 56.1.0) | Recorrido por persona ya superado por las entregas 1–8 | Plantilla de los manuales por rol. Estado de sus correcciones: FUNC-C16 (Dirección sin Jefe MAST) decidido y hecho en la entrega 4; FUNC-C19 (herencias propias) hecho en 57.30.0; las demás, en `docs/audit/99-consolidado.md` |

## Identificadores «G» (no confundir)

Tres auditorías usaron la letra G con significados distintos. Al citarlos:

| Prefijo al citar | Documento | Rango |
|---|---|---|
| `OLAS-G11…OLAS-G21` | `SGI_AUDITORIA_OLAS_AB.md` | olas A y B (ago-2026) |
| `FUNC-G1…FUNC-G13`, `FUNC-C1…FUNC-C19` | `AUDITORIA_FUNCIONAL_SGI_2026-09-28.md` | recorrido por rol (28-sep-2026) |
| `G-001…` (y A-, B-, C-… K-) | `docs/audit/*.md` | auditoría completa (29-sep-2026) |

Regla para lo que venga: cada auditoría con prefijo propio y fecha.

## Otras carpetas de este histórico

- `../quimibond_sgi_data/`: XML del mapa de procesos y de auditorías que el
  módulo cargaba antes de instalarse vacío (57.4.0 y 57.6.0). El mapa vive ahora en
  `quimibond_sgi_mapa`.
- `../quimibond_sgi_tools/`: scripts de shell de la carga documental de 2026.
- `../intelligence/`: documentos de `quimibond_intelligence` que ya no
  aplican (ver su propio índice).

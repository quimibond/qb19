# Documentación técnica del SGI

Para programadores y para el administrador del sistema. Lo que se puede sacar
del código **se genera** y no se escribe a mano:

```bash
python3 tools/sgi_docs.py           # regenera los archivos de abajo
python3 tools/sgi_docs.py --check   # sale con 1 si algo quedó viejo; avisa campos nuevos sin help
```

Corre con la librería estándar (AST y XML), sin Odoo. Regenerar y comprometer
el resultado en el mismo commit que cambia modelos, vistas, menús, crons,
grupos o parámetros. Todavía **no** corre en el CI (conectarlo requiere
cambiar `.github/workflows/ci.yml`; queda como paso aparte, primero como
informativo).

| Archivo (generado) | Qué trae |
|---|---|
| `modelo-de-datos.md` | Modelos propios y extendidos, con descripción, docstring, número de campos y archivo |
| `diccionario/<modelo>.md` | Campos (tipo, etiqueta, `help`, requerido, relación, cálculo, grupos, archivo:línea) y métodos públicos |
| `vistas-y-acciones.md` | Árbol de menús, acciones (con o sin ayuda de pantalla vacía) y vistas por modelo |
| `seguridad.md` | Grupos con lo que implican, permisos del CSV y reglas de registro |
| `crons.md` | Acciones planificadas con su método y lo que hace |
| `parametros.md` | Parámetros de arranque (`sgi.config._SGI_DEFAULT_PARAMS`) |
| `integraciones.md` | Módulos fuera del SGI que nombran modelos `sgi.*` |

## Lo que todavía no se genera

Referencia escrita a mano hasta la 57.30.0, en
`docs/historico/sgi/README_quimibond_sgi_hasta_57.30.0.md` (verificar contra
el código antes de confiar en un detalle):

- Carga del mapa por API: `sgi.process.load_payload(payload, dry_run=False)`
  y `export_payload` (secciones «Carga por API» y «Exportar el mapa»).
- Lógica de indicadores, meta con trayectoria, plan de acción en rojo y
  fórmula configurable.
- Alcance multiempresa: el SGI es de una sola empresa (D-03,
  `sgi.config._sgi_company()`, parámetro `quimibond_sgi.sgi_company_id`).

Reglas de build que aplican al SGI: `CLAUDE.md` de la raíz. Despliegue y
verificaciones: `docs/RUNBOOK_DESPLIEGUE.md`, sección SGI.

<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.format.mixin`

**Mixin: clave de formato SGI** (AbstractModel).

Agrega al modelo la clave del formato SGI que sustituye (pantalla y PDF).

Archivos: `addons/quimibond_sgi/models/sgi_format_map.py`.

## Campos (1)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_format_banner` | Char | Formato SGI |  |  |  | compute `_compute_sgi_format_banner`, sin guardar |  | `addons/quimibond_sgi/models/sgi_format_map.py:455` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `sgi_format_info` | 'F-P-A28-04 · Rev. 03' \| 'F-P-A28-04' (sin doc vigente) \| False. Clave y revisión salen del documento ligado al mapeo (C-006). |

<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.sign.record.mixin`

**Firmas de Sign ligadas al registro** (AbstractModel).

Archivos: `addons/quimibond_sgi/models/sgi_sign_record.py`.

## Campos (4)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_can_sign` | Boolean |  | El usuario puede pedir firma desde este registro: es Usuario SGI y tiene acceso a la app Firma electrónica (I-014, auditoría 2026-09). |  |  | compute `_compute_sgi_can_sign`, sin guardar |  | `addons/quimibond_sgi/models/sgi_sign_record.py:24` |
| `sgi_sign_count` | Integer |  |  |  |  | compute `_compute_sgi_sign_requests`, sin guardar |  | `addons/quimibond_sgi/models/sgi_sign_record.py:21` |
| `sgi_sign_request_ids` | Many2many | Solicitudes de firma |  |  | `sign.request` | compute `_compute_sgi_sign_requests`, sin guardar |  | `addons/quimibond_sgi/models/sgi_sign_record.py:19` |
| `sgi_signed` | Boolean | Firmado |  |  |  | compute `_compute_sgi_sign_requests`, sin guardar |  | `addons/quimibond_sgi/models/sgi_sign_record.py:22` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `action_sgi_sign` | — |
| `action_sgi_view_sign_requests` | — |

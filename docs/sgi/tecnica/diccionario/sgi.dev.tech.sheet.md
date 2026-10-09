<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.tech.sheet`

**Ficha técnica interna del desarrollo** (Model). Hereda de: `mail.thread`, `sgi.dev.doc.mixin`.

Orden: `id desc`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py`.

## Campos (8)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `approved_at` | Datetime | Aprobada el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:157` |
| `approved_by_id` | Many2one | Aprobó (Ventas, C1.15) |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:156` |
| `can_sign` | Boolean |  | El usuario tiene un puesto que firma y aún no firmó. |  |  | compute `_compute_signed`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:155` |
| `line_ids` | One2many | Características |  |  | `sgi.dev.tech.sheet.line` |  |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:151` |
| `name` | Char | Ficha |  |  |  | compute `_compute_name`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:150` |
| `pending_sign_count` | Integer |  |  |  |  | compute `_compute_signed`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:154` |
| `sign_ids` | One2many | Firmas |  |  | `sgi.dev.tech.sheet.sign` |  |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:152` |
| `signed_count` | Integer |  |  |  |  | compute `_compute_signed`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:153` |

## Métodos públicos (3)

| Método | Qué hace (docstring) |
|---|---|
| `action_approve` | Aprobación de C1.15 (Administrador de Ventas por la regla nativa de Studio): la ficha queda vigente con su PDF. Las firmas que falten se imprimen en blanco; quién firmó y quién aprobó queda en el reg… |
| `action_sign` | Firma los renglones de los puestos del usuario (o de los que suple como jefe directo) que aún no están firmados. |
| `create` | — |

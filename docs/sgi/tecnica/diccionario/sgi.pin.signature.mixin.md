<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.pin.signature.mixin`

**Firma con PIN en una tableta de planta** (AbstractModel).

Firma con PIN en una tableta de planta: la tableta y la hora. Cada modelo dice quién firmó con ``_sgi_pin_employee``.

Archivos: `addons/quimibond_sgi/models/sgi_pin.py`.

## Campos (3)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_pin_signature` | Char | Firma con PIN | Quién firmó con su PIN, en qué tableta y cuándo. |  |  | compute `_compute_sgi_pin_signature`, sin guardar |  | `addons/quimibond_sgi/models/sgi_pin.py:73` |
| `sgi_pin_signed_at` | Datetime | Firmado con PIN el | Fecha y hora en que la persona firmó con su PIN. |  |  |  |  | `addons/quimibond_sgi/models/sgi_pin.py:70` |
| `sgi_pin_tablet_id` | Many2one | Firmado con PIN en la tableta | Tableta de planta (cuenta compartida) donde la persona firmó con su PIN. |  | `sgi.floor.tablet` |  |  | `addons/quimibond_sgi/models/sgi_pin.py:66` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `create` | — |
| `write` | — |

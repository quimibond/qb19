<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.loto.energy`

**Fuente de energía del bloqueo** (Model).

Orden: `loto_id, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_loto.py`.

## Campos (5)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `energy_type` | Selection | Energía |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_loto.py:158` |
| `isolation_point` | Char | Punto de bloqueo | Interruptor, válvula, tablero, candado de palanca… |  |  |  |  | `addons/quimibond_sgi/models/sgi_loto.py:160` |
| `loto_id` | Many2one | Bloqueo |  | sí | `sgi.loto` |  |  | `addons/quimibond_sgi/models/sgi_loto.py:155` |
| `method` | Char | Cómo se aísla | Abrir y bloquear, purgar, calzar… |  |  |  |  | `addons/quimibond_sgi/models/sgi_loto.py:162` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_loto.py:157` |

## Métodos públicos (3)

| Método | Qué hace (docstring) |
|---|---|
| `create` | — |
| `unlink` | — |
| `write` | — |

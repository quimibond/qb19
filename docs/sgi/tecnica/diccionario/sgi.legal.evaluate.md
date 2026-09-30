<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.legal.evaluate`

**Registrar evaluación de cumplimiento legal** (TransientModel).

Asistente para registrar una evaluación de cumplimiento de un requisito legal.

Archivos: `addons/quimibond_sgi/models/sgi_legal.py`.

## Campos (4)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `evidence` | Text | Evidencia revisada |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_legal.py:266` |
| `next_date` | Date | Próxima evaluación |  |  |  | compute `_compute_next_date`, guardado |  | `addons/quimibond_sgi/models/sgi_legal.py:269` |
| `requirement_id` | Many2one |  |  | sí | `sgi.legal.requirement` |  |  | `addons/quimibond_sgi/models/sgi_legal.py:259` |
| `result` | Selection | Resultado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_legal.py:260` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_confirm` | — |

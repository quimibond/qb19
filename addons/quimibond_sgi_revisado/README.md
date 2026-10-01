# Quimibond SGI - Puente Revisado

Satélite de `quimibond_sgi` (`auto_install` con `mrp_revisado_telas`).

- **Qué agrega:** vistas de pivote y gráfica del registro de revisado
  (`mrp.revision.log`) para el Pareto de defectos por causa (etiquetas
  `TEJIDO-*`), en el menú «Pareto de defectos de revisado».
- **Indicador MA-03 «Calidad PQ»** (desde 4.2.0): el modo de cálculo
  `calidad_pq` (rollos revisados sin defecto ÷ revisados), su detalle, su
  evidencia y los avisos del Diagnóstico. Antes vivía en el núcleo.
- **Pareto ordenado** (desde 4.3.0, revisión de vistas V-M13): pivote y
  gráfica de la causa más frecuente a la menos frecuente, filtro «Fecha de
  revisado» y ayuda en pantalla vacía.
- **No cambia** `mrp_revisado_telas`.
- **Depende de:** `quimibond_sgi`, `mrp_revisado_telas`.
- **Pruebas:** `tests/test_calidad_pq.py`.

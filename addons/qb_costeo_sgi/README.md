# qb_costeo_sgi — puente cotizador ↔ SGI

Se instala solo (`auto_install`) cuando están `qb_costeo`, `qb_cotizador` y
`quimibond_sgi`. Pruebas solo en el build de Odoo.sh
(`--test-tags /qb_costeo_sgi`): el CI no instala el SGI.

- Cotización ligada al proyecto de desarrollo (C1): toma del proyecto lo ya
  capturado, sin recaptura.
- Revisión del desarrollo ⇒ recálculo; si el costo cambia, el proyecto se
  detiene hasta la aprobación del puesto que aprueba.
- Fichas C1.05 / C1.06 medidas con la cotización.

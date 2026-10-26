# quimibond_sat_sgi — Pendientes del SAT en Mis pendientes del SGI

Puente entre `quimibond_sat` y `quimibond_sgi` (se instala solo cuando están
los dos). Lo que no cuadra entre el SAT y Odoo llega como **aviso** a *Mis
pendientes* de quien ocupa el puesto responsable de la actividad del mapa de
procesos que lo resuelve. Si nadie ocupa el puesto, al Jefe MAST.

| Aviso (clave) | Actividad del SGI | Qué junta |
|---|---|---|
| `sat_cancelado_proveedor` | S1.21 Registrar la factura del proveedor desde el SAT | Facturas de proveedor publicadas con su CFDI cancelado en el SAT |
| `sat_69b` | S1.21 | Facturas publicadas de proveedores en la lista 69-B |
| `sat_falta_odoo` | S1.21 | CFDI recibidos que no están en Odoo (de hace más de 5 días) |
| `sat_diferencia_proveedor` | S1.21 | Monto o moneda distintos, o ligados a una factura en borrador |
| `sat_rep_proveedor` | S1.25 Revisar que cada pago tenga su complemento del proveedor | Pagos sin complemento o con complementos de más |
| `sat_rep_emitido` | S2.21 Timbrar y enviar el complemento de pago | Cobros sin complemento, de más o sin pago en Odoo |
| `sat_iva_mes` | S3.14 Calcular los pagos provisionales de ISR e IVA | Meses recientes (actual y dos anteriores) cuyo IVA del SAT no cuadra con la contabilidad (*IVA del mes*, quimibond_sat 1.25.0) |
| `sat_cancelar_emitida` | S2.13 Cancelar y refacturar | Facturas emitidas canceladas en un lado y vigentes en el otro |

- Un aviso por tipo, con el número de pendientes en el resumen y los primeros
  15 en la nota. Usa el motor de avisos del SGI (`sgi.cron._sgi_schedule`):
  clave estable, se actualiza en vez de duplicarse y se cierra solo cuando ya
  no hay pendientes (`_sgi_sweep`).
- **Ir** abre la lista del SAT (Conciliar o Pagos) ya filtrada.
- Cron diario «SGI: Pendientes del SAT» (13:23 UTC). Solo corre si la empresa
  del SGI tiene *Sincronizar CFDI con Syntage*.
- La actividad se busca por su numeral (`S1.21`, …) en el mapa de procesos de
  la empresa del SGI; si no existe, ese tipo no avisa.

Las pruebas (`tests/test_sat_sgi.py`) corren en el build de Odoo.sh: el CI no
instala el SGI (Enterprise).

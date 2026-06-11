/** @odoo-module */
import { patch } from "@web/core/utils/patch";
import { FormController } from "@web/views/form/form_controller";
import { useService } from "@web/core/utils/hooks";
import { onMounted, onWillDestroy } from "@odoo/owl";

patch(FormController.prototype, {
    setup() {
        super.setup(...arguments);
        this.iotLongpollingService = useService("iot_longpolling");
        this.ormService = useService("orm"); // 🔒 CORREGIDO: Importamos el servicio ORM de Odoo 19

        onMounted(() => {
            if (this.model.root.resModel === 'mrp.revisado.wizard') {
                this._connectToRevisadoScale();
            }
        });

        onWillDestroy(() => {
            if (this.revisadoScaleListener) {
                this.revisadoScaleListener = null;
            }
        });
    },

   _connectToRevisadoScale() {
        const root = this.model.root;
        if (root.data.weighing_mode === 'iot' && root.data.iot_device_id) {
            
            // 🔒 SOLUCIÓN MAESTRA: Extraemos el ID numérico real sin importar el formato de Odoo 19
            let iotDeviceId = null;
            const rawData = root.data.iot_device_id;

            if (Array.isArray(rawData)) {
                iotDeviceId = rawData[0];
            } else if (rawData && typeof rawData === 'object') {
                iotDeviceId = rawData.id || (rawData.resIds && rawData.resIds[0]);
            } else {
                iotDeviceId = rawData;
            }

            // Forzamos a que sea un entero plano válido antes de enviarlo al ORM
            const finalId = parseInt(iotDeviceId, 10);

            if (finalId && !isNaN(finalId)) {
                this.ormService.read('iot.device', [finalId], ['iot_id', 'identifier'])
                .then((result) => {
                    if (result && result.length > 0) {
                        const dev = result[0];
                        this.revisadoScaleListener = (data) => {
                            if (data.status === 'success' && data.value !== undefined) {
                                if (root.data.peso_actual !== data.value) {
                                    root.update({ peso_actual: parseFloat(data.value) });
                                }
                            }
                        };
                        this.iotLongpollingService.addListener(dev.iot_id[1], dev.identifier, this.revisadoScaleListener);
                    }
                });
            }
        }
    }
});
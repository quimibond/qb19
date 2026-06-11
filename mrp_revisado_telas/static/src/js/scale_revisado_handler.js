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
            
            // 🔒 SOLUCIÓN: Validamos si viene como arreglo [id, name] o como entero directo
            const iotDeviceId = Array.isArray(root.data.iot_device_id) 
                ? root.data.iot_device_id[0] 
                : root.data.iot_device_id;

            if (iotDeviceId) {
                this.ormService.read('iot.device', [iotDeviceId], ['iot_id', 'identifier'])
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
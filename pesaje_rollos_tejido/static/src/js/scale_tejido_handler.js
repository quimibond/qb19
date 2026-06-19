/** @odoo-module */
import { patch } from "@web/core/utils/patch";
import { FormController } from "@web/views/form/form_controller";
import { useService } from "@web/core/utils/hooks";
import { onMounted, onWillDestroy } from "@odoo/owl";

patch(FormController.prototype, {
    setup() {
        super.setup(...arguments);
        this.ormService = useService("orm");
        this.tejidoScaleInterval = null;

        onMounted(() => {
            const modelName = this.model.root.resModel;
            if (modelName === 'mrp.weigh.roll.wizard' || modelName === 'mrp.subproduct.wizard') {
                this._connectToTejidoScale();
            }
        });

        onWillDestroy(() => {
            // Detener el reloj de consulta inmediatamente al cerrar la ventana
            if (this.tejidoScaleInterval) {
                clearInterval(this.tejidoScaleInterval);
                this.tejidoScaleInterval = null;
            }
        });
    },

    _connectToTejidoScale() {
        const root = this.model.root;
        if (root.data.weighing_mode === 'iot' && root.data.iot_device_id) {
            
            let iotDeviceId = null;
            const rawData = root.data.iot_device_id;

            if (Array.isArray(rawData)) {
                iotDeviceId = rawData[0];
            } else if (rawData && typeof rawData === 'object') {
                iotDeviceId = rawData.id || (rawData.resIds && rawData.resIds[0]);
            } else {
                iotDeviceId = rawData;
            }

            const finalId = parseInt(iotDeviceId, 10);

            if (finalId && !isNaN(finalId)) {
                this.ormService.read('iot.device', [finalId], ['identifier'])
                .then((result) => {
                    if (result && result.length > 0) {
                        const dev = result[0];
                        const identifier = dev.identifier;

                        if (identifier) {
                            // 🔒 CONSULTA DIRECTA Y PURA: Pregunta al Virtual IoT Box cada 1000ms (1 segundo)
                            this.tejidoScaleInterval = setInterval(() => {
                                fetch(`http://127.0.0.1:8069/iot/scale/${identifier}/get_value`)
                                .then(response => response.json())
                                .then(data => {
                                    if (data && data.status === 'success' && data.value !== undefined) {
                                        const targetField = 'weight';
                                        if (root.data[targetField] !== data.value) {
                                            root.update({ [targetField]: parseFloat(data.value) });
                                        }
                                    }
                                })
                                .catch(err => {
                                    // Silencioso: Si la báscula parpadea, no truena la pantalla de Odoo
                                    console.log("Esperando ráfaga de la báscula Rhino...");
                                });
                            }, 1000);
                        }
                    }
                });
            }
        }
    }
});
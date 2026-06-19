/** @odoo-module */
import { patch } from "@web/core/utils/patch";
import { FormController } from "@web/views/form/form_controller";
import { useService } from "@web/core/utils/hooks";
import { onMounted, onWillDestroy } from "@odoo/owl";

patch(FormController.prototype, {
    setup() {
        super.setup(...arguments);
        this.iotLongpolling = useService("iot_longpolling");
        this.ormService = useService("orm");

        onMounted(() => {
            const modelName = this.model.root.resModel;
            if (modelName === 'mrp.weigh.roll.wizard' || modelName === 'mrp.subproduct.wizard') {
                this._connectToTejidoScale();
            }
        });

        onWillDestroy(() => {
            // 🔒 REMOCIÓN SEGURA: Usamos la API pública de alto nivel removeDevice
            if (this.cleanIotIp && this.cleanIdentifier && this.tejidoScaleListener) {
                this.iotLongpolling.removeDevice(this.cleanIotIp, this.cleanIdentifier, this.tejidoScaleListener);
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
                // 🔒 CORRECCIÓN CRÍTICA: Añadimos 'iot_ip' en la lista de campos a leer por el ORM
                this.ormService.read('iot.device', [finalId], ['iot_ip', 'identifier'])
                .then((result) => {
                    if (result && result.length > 0) {
                        const dev = result[0];
                        
                        // Obtenemos la IP limpia (ya sea string directo o del array Many2one de Odoo)
                        this.cleanIotIp = "localhost";
                        this.cleanIdentifier = dev.identifier;

                        this.tejidoScaleListener = (data) => {
                            if (data && data.status === 'success' && data.value !== undefined) {
                                const targetField = 'weight'; // Ambos wizards (mrp.weigh.roll.wizard y mrp.subproduct.wizard) usan 'weight'
                                if (root.data[targetField] !== data.value) {
                                    root.update({ [targetField]: parseFloat(data.value) });
                                }
                            }
                        };

                        if (this.cleanIotIp && this.cleanIdentifier) {
                            // 🔒 SOLUCIÓN DEFINITIVA: Usamos la API pública addDevice. 
                            // Esta función mapea internamente los callbacks sin causar errores iterables de Owl.
                            this.iotLongpolling.addDevice(this.cleanIotIp, this.cleanIdentifier, this.tejidoScaleListener);
                        }
                    }
                });
            }
        }
    }
});
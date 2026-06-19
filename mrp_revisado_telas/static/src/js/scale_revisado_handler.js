/** @odoo-module */
import { patch } from "@web/core/utils/patch";
import { FormController } from "@web/views/form/form_controller";
import { useService } from "@web/core/utils/hooks";
import { onMounted, onWillDestroy } from "@odoo/owl";

patch(FormController.prototype, {
    setup() {
        super.setup(...arguments);
        this.iotLongpollingService = useService("iot_longpolling");
        this.ormService = useService("orm");

        onMounted(() => {
            if (this.model.root.resModel === 'mrp.revisado.wizard') {
                this._connectToRevisadoScale();
            }
        });

        onWillDestroy(() => {
            // 🔒 REMOCIÓN SEGURA: API pública removeDevice para limpiar el canal sin fallos de ciclo de vida
            if (this.cleanIotIp && this.cleanIdentifier && this.revisadoScaleListener) {
                this.iotLongpollingService.removeDevice(this.cleanIotIp, this.cleanIdentifier, this.revisadoScaleListener);
            }
        });
    },

    _connectToRevisadoScale() {
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
                // 🔒 CORRECCIÓN CRÍTICA: Añadimos 'iot_ip' explícitamente en el orm.read
                this.ormService.read('iot.device', [finalId], ['iot_ip', 'identifier'])
                .then((result) => {
                    if (result && result.length > 0) {
                        const dev = result[0];
                        
                        // Formateamos la IP de manera limpia (evitando nombres de texto plano de Odoo)
                        this.cleanIotIp = "localhost";
                        this.cleanIdentifier = dev.identifier;

                        this.revisadoScaleListener = (data) => {
                            if (data && data.status === 'success' && data.value !== undefined) {
                                if (root.data.peso_actual !== data.value) {
                                    root.update({ peso_actual: parseFloat(data.value) });
                                }
                            }
                        };

                        if (this.cleanIotIp && this.cleanIdentifier) {
                            // 🔒 SOLUCIÓN DEFINITIVA: addDevice se encarga de la iteración nativa en Odoo 19
                            this.iotLongpollingService.addDevice(this.cleanIotIp, this.cleanIdentifier, this.revisadoScaleListener);
                        }
                    }
                });
            }
        }
    }
});
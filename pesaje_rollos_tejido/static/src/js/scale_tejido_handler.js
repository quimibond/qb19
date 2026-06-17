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
            if (this.tejidoDeviceWrapper && this.iotLongpolling) {
                try {
                    // Pasamos el listener envuelto en un arreglo también para la remoción segura
                    this.iotLongpolling.removeListener([this.tejidoDeviceWrapper]);
                } catch (err) {
                    console.error("Error al remover el listener de pesaje:", err);
                }
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
                this.ormService.read('iot.device', [finalId], ['iot_id', 'identifier'])
                .then((result) => {
                    if (result && result.length > 0) {
                        const dev = result[0];
                        const iotIp = Array.isArray(dev.iot_id) ? dev.iot_id[1] : dev.iot_id;

                        if (iotIp && dev.identifier) {
                            
                            this.tejidoDeviceWrapper = {
                                iot_ip: iotIp,
                                identifier: dev.identifier,
                                callback: (data) => {
                                    if (data && data.status === 'success' && data.value !== undefined) {
                                        const targetField = 'weight'; // Simplificado ya que ambos modelos usan 'weight'
                                        if (root.data[targetField] !== data.value) {
                                            root.update({ [targetField]: parseFloat(data.value) });
                                        }
                                    }
                                }
                            };

                            try {
                                // 🛠️ CORRECCIÓN CRÍTICA ODOO 19:
                                // Se envuelve el wrapper en un Array [] para que sea iterable por el Core de Odoo.
                                this.iotLongpolling.addListener([this.tejidoDeviceWrapper]);
                            } catch (e) {
                                console.error("Error crítico al inicializar el listener de la báscula:", e);
                            }
                        }
                    }
                });
            }
        }
    }
});
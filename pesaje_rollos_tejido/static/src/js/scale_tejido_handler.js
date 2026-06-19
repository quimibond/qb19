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
                            // 🔒 ESTÁNDAR ODOO 19: JSON-RPC estructurado al ruteador del Proxy
                            this.tejidoScaleInterval = setInterval(() => {
                                fetch("http://127.0.0.1:8069/hw_proxy/perform_action", {
                                    method: "POST",
                                    headers: { "Content-Type": "application/json" },
                                    body: JSON.stringify({
                                        jsonrpc: "2.0",
                                        params: {
                                            action: "read_scale",
                                            device_identifier: identifier
                                        }
                                    })
                                })
                                .then(response => response.json())
                                .then(data => {
                                    if (data && data.result) {
                                        // Validamos las dos posibles respuestas del core de Odoo 19
                                        const weightValue = data.result.weight !== undefined ? data.result.weight : data.result.value;
                                        if (weightValue !== undefined && !isNaN(weightValue)) {
                                            const targetField = 'weight';
                                            if (root.data[targetField] !== weightValue) {
                                                root.update({ [targetField]: parseFloat(weightValue) });
                                            }
                                        }
                                    }
                                })
                                .catch(err => {
                                    console.log("Conectando con el Driver de la Rhino...");
                                });
                            }, 1000);
                        }
                    }
                });
            }
        }
    }
});
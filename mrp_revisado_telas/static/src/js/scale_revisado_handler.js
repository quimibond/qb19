/** @odoo-module */
import { patch } from "@web/core/utils/patch";
import { FormController } from "@web/views/form/form_controller";
import { useService } from "@web/core/utils/hooks";
import { onMounted, onWillDestroy } from "@odoo/owl";

patch(FormController.prototype, {
    setup() {
        super.setup(...arguments);
        this.ormService = useService("orm");
        this.revisadoScaleInterval = null;

        onMounted(() => {
            if (this.model.root.resModel === 'mrp.revisado.wizard') {
                this._connectToRevisadoScale();
            }
        });

        onWillDestroy(() => {
            if (this.revisadoScaleInterval) {
                clearInterval(this.revisadoScaleInterval);
                this.revisadoScaleInterval = null;
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
                this.ormService.read('iot.device', [finalId], ['identifier'])
                .then((result) => {
                    if (result && result.length > 0) {
                        const dev = result[0];
                        const identifier = dev.identifier;

                        if (identifier) {
                            // 🔒 ESTÁNDAR ODOO 19: JSON-RPC estructurado al ruteador del Proxy
                            this.revisadoScaleInterval = setInterval(() => {
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
                                        const weightValue = data.result.weight !== undefined ? data.result.weight : data.result.value;
                                        if (weightValue !== undefined && !isNaN(weightValue)) {
                                            if (root.data.peso_actual !== weightValue) {
                                                root.update({ peso_actual: parseFloat(weightValue) });
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
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

        // Definimos el callback global para recibir el peso de forma segura sin CORS
        window.onTejidoScaleRead = (data) => {
            if (data && data.weight !== undefined) {
                const root = this.model.root;
                const targetField = 'weight';
                if (root.data[targetField] !== data.weight) {
                    root.update({ [targetField]: parseFloat(data.weight) });
                }
            }
        };

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
            delete window.onTejidoScaleRead;
        });
    },

    _connectToTejidoScale() {
        if (this.model.root.data.weighing_mode === 'iot' && this.model.root.data.iot_device_id) {
            // Consulta por inyección de script (Bypass de CORS absoluto)
            this.tejidoScaleInterval = setInterval(() => {
                const oldScript = document.getElementById('tejido_scale_jsonp');
                if (oldScript) oldScript.remove();

                const script = document.createElement('script');
                script.id = 'tejido_scale_jsonp';
                // Usamos el endpoint nativo del proxy local que acepta callbacks
                script.src = `http://127.0.0.1:8069/hw_proxy/scale_read?callback=onTejidoScaleRead&_=${new Date().getTime()}`;
                document.body.appendChild(script);
            }, 1000);
        }
    }
});
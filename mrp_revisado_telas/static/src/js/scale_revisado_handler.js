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

        window.onRevisadoScaleRead = (data) => {
            if (data && data.weight !== undefined) {
                const root = this.model.root;
                if (root.data.peso_actual !== data.weight) {
                    root.update({ peso_actual: parseFloat(data.weight) });
                }
            }
        };

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
            delete window.onRevisadoScaleRead;
        });
    },

    _connectToRevisadoScale() {
        if (this.model.root.data.weighing_mode === 'iot' && this.model.root.data.iot_device_id) {
            this.revisadoScaleInterval = setInterval(() => {
                const oldScript = document.getElementById('revisado_scale_jsonp');
                if (oldScript) oldScript.remove();

                const script = document.createElement('script');
                script.id = 'revisado_scale_jsonp';
                script.src = `http://127.0.0.1:8069/hw_proxy/scale_read?callback=onRevisadoScaleRead&_=${new Date().getTime()}`;
                document.body.appendChild(script);
            }, 1000);
        }
    }
});
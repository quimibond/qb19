/** @odoo-module **/
// 56.6.2: el botón nativo «Nuevo» (arriba a la izquierda) de la lista de
// «Mis actividades» hace lo mismo que el del kanban (atributo on_create):
// abre la propuesta de actividad nueva con el puesto y los procesos de la
// pantalla, en vez de crear un renglón suelto de sgi.activity.role. La lista
// no tiene on_create en Odoo; esta vista (js_class) solo cambia esa acción.
import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";
import { listView } from "@web/views/list/list_view";
import { ListController } from "@web/views/list/list_controller";

export const SGI_NEW_ACTIVITY_ACTION = "quimibond_sgi.sgi_activity_change_action_new";

export class SgiMyActivitiesListController extends ListController {
    setup() {
        super.setup();
        this.sgiAction = useService("action");
    }

    async createRecord() {
        await this.sgiAction.doAction(SGI_NEW_ACTIVITY_ACTION, {
            additionalContext: this.props.context,
            onClose: () => this.model.load(),
        });
    }
}

registry.category("views").add("sgi_my_activities_list", {
    ...listView,
    Controller: SgiMyActivitiesListController,
});

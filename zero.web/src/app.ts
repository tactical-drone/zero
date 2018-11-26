import { computedFrom, observable, BindingEngine, autoinject, ICollectionObserverSplice, bindable } from "aurelia-framework";
import { IoNodeServices } from 'services/IoNodeService';

import "kendo-ui-core";

import { IoApiReturn } from 'core/api/IoApiReturn';

@autoinject
export class app {
    constructor(nodeService: IoNodeServices, bindingEngine: BindingEngine) {
        this.nodeServices = nodeService;
        /*this.fetchLogs();*/

        /*let subscription = bindingEngine.collectionObserver(this.logs).subscribe(this.collectionChanged.bind(this));*/
        let token = 'Bearer ' + this.nodeServices.zcfg.scfg.token;
        this.dataSource = new kendo.data.DataSource({
            transport: {
                read: {
                    url: "http://localhost:14256/api/node/logs",
                    type: "GET",
                    dataType: "json",
                    beforeSend: function(xhr) {
                         xhr.setRequestHeader('Authorization', token);
                    }                    
                },                 
            },
            schema: {
                data: "rows"
            }
        });     
    }

    @observable nodeServices: IoNodeServices;

    @observable
    logs: IoApiReturn[] = [];

    //@bindable
    dataSource: kendo.data.DataSource;

    attached() {
        //kendo.jQuery(this.pager).kendoPager({
        //    dataSource: this.dataSource
        //});
        
        //$("#logView").kendoListBox({
        //    dataSource: this.dataSource
        //});
    }

    @computedFrom(nameof.full(this.nodeServices.response, -2))
    get message(): string {
        return this.nodeServices.response.message;
    }

    //collectionChanged(splices: Array<ICollectionObserverSplice<IoApiReturn>>) {
    //    for (var i = 0; i < splices.length; i++) {
    //        var splice: ICollectionObserverSplice<string> = splices[i];

    //        var valuesAdded = this.logs.slice(splice.index, splice.index + splice.addedCount);
    //        if (valuesAdded.length > 0) {
    //            console.log(`The following values were inserted at ${splice.index}: ${JSON.stringify(valuesAdded)}`);
    //            this.logs = splices;
    //        }

    //        if (splice.removed.length > 0) {
    //            console.log(`The following values were removed from ${splice.index}: ${JSON.stringify(splice.removed)}`);
    //        }
    //    }
    //}

    async fetchLogs() {
        while (true) {
            await this.sleep(1000);
            this.nodeServices.getLogs().then(response => {
                this.logs.push(response);
            });
        }

    }

    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    //binds
    url: string;
    port: number;
}

import { computedFrom, observable, BindingEngine, autoinject, ICollectionObserverSplice, bindable } from "aurelia-framework";
import { IoNodeServices } from 'services/IoNodeService';

import "kendo-ui-core";

import { IoApiReturn } from 'core/api/IoApiReturn';

@autoinject
export class app {
    constructor(nodeService: IoNodeServices, bindingEngine: BindingEngine) {
        this.nodeServices = nodeService;
        this.fetchLogs();

        /*let subscription = bindingEngine.collectionObserver(this.logs).subscribe(this.collectionChanged.bind(this));*/
        let token = 'Bearer ' + this.nodeServices.zcfg.scfg.token;
        this.dataSource = new kendo.data.DataSource({
            transport: {
                update: {
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
    
    nodeServices: IoNodeServices;
    
    logs: kendo.data.ObservableArray = new kendo.data.ObservableArray([]);

    dataSource: kendo.data.DataSource;

    attached() {
        //kendo.jQuery(this.pager).kendoPager({
        //    dataSource: this.dataSource
        //});
        
        //$("#logView").kendoListBox({
        //    dataSource: this.dataSource
        //});

        this.dataSource.add({ logMsg: "Entry 4" });
    }

    @computedFrom(nameof.full(this.nodeServices.response, -2))
    get message(): string {
        return this.nodeServices.response.message;
    }

    async fetchLogs() {
        while (true) {
            await this.sleep(1);
            this.nodeServices.getLogs().then(response => {
                //this.logs.push.apply(this.logs, response.rows);                

                this.dataSource.data().push.apply(this.dataSource.data(), response.rows);                
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

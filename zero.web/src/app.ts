import { computedFrom, observable, BindingEngine, autoinject, ICollectionObserverSplice, bindable } from "aurelia-framework";
import { IoNodeServices } from 'services/IoNodeService';

import "kendo-ui-core";

import { IoApiReturn } from 'core/api/IoApiReturn';

@autoinject
export class app {
    constructor(nodeService: IoNodeServices, bindingEngine: BindingEngine) {
        this.nodeServices = nodeService;
        this.nodeServices.stream(() => { return this.nodeServices.getLogs(); }, this.logs, 500);

        /*let subscription = bindingEngine.collectionObserver(this.logs).subscribe(this.collectionChanged.bind(this));*/
        let token = 'Bearer ' + this.nodeServices.zcfg.scfg.token;
        this.dataSource = this.nodeServices.kendoDataSource("/node/logs");

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

    @computedFrom(nameof.full(this.nodeServices.apiReponse, -2))
    get message(): string {
        return this.nodeServices.apiReponse.message;
    }

    //binds
    url: string;
    port: number;
}

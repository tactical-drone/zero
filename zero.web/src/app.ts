import { computedFrom, observable, BindingEngine, autoinject, ICollectionObserverSplice, bindable } from "aurelia-framework";
import { IoNodeServices } from 'services/IoNodeService';

import "kendo-ui-core";

@autoinject
export class app {

    constructor(nodeService: IoNodeServices, bindingEngine: BindingEngine) {
        this.nodeServices = nodeService;
        this.nodeServices.streamUnShift(() => { return this.nodeServices.getLogs(); }, this.logs, this.logSleepTime);

        /*let subscription = bindingEngine.collectionObserver(this.logs).subscribe(this.collectionChanged.bind(this));*/
        
        this.dataSource = this.nodeServices.kendoDataSource("/node/logs");

    }

    logSleepTime:number = 500;
    
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
    }

    @computedFrom(nameof.full(this.nodeServices.apiReponse, -2))
    get message(): string {
        return this.nodeServices.apiReponse.message;
    }

    //binds
    url: string;

    rows = [
        { collapsible: false, size: '25%'},
        { collapsible: false, scrollable: false},
        { collapsible: false, size: '5%'}        
    ];
}

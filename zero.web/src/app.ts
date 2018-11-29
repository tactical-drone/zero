import { computedFrom, observable, BindingEngine, autoinject, ICollectionObserverSplice, bindable } from "aurelia-framework";
import { IoNodeServices } from 'services/IoNodeService';

import "kendo-ui-core";

@autoinject
export class app {

    constructor(nodeService: IoNodeServices, bindingEngine: BindingEngine) {
        this.nodeServices = nodeService;

        //this.nodeServices.streamUnShift(() => { return this.nodeServices.getLogs(); }, this.logs, this.logSleepTime);

        /*let subscription = bindingEngine.collectionObserver(this.logs).subscribe(this.collectionChanged.bind(this));*/
        
        this.dataSource = this.nodeServices.kendoDataSource("/node/logs");

        this.getTransactions();
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

    async getTransactions() {
        while (true) {
            await this.nodeServices.queryTransactionStream(15600, this.tagQuery)
                .then(async response => {
                    this.logs.unshift.apply(this.logs, response.rows);
                    if (this.logs.length > 60) {
                        for (var i = this.logs.length; i > 58; i--) {
                            await this.nodeServices.sleep(0);
                            this.logs.pop();
                        }
                    }
                });
            await  this.nodeServices.sleep(500);
        }        
    }

    async stopNeighbor() {
        await this.nodeServices.stopListner(this.stopNeighborId);
    }

    //binds
    url: string = 'tcp://192.168.1.2:15600';
    tagQuery: string = '9999';
    stopNeighborId: number = 15600;

    rows = [
        { collapsible: false, size: '260px'},
        { collapsible: false, size: '500px', scrollable: false},
    ];
}

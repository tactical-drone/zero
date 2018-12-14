import { computedFrom, observable, BindingEngine, autoinject, ICollectionObserverSplice, bindable } from "aurelia-framework";
import { IoNodeServices } from 'services/IoNodeService';

import "kendo-ui-core";

@autoinject
export class app {

    constructor(nodeService: IoNodeServices, bindingEngine: BindingEngine) {
        this.nodeServices = nodeService;

        //this.nodeServices.streamUnShift(() => { return this.nodeServices.getLogs(); }, this.logs, this.logSleepTime);

        /*let subscription = bindingEngine.collectionObserver(this.logs).subscribe(this.collectionChanged.bind(this));*/

        //this.dataSource = this.nodeServices.kendoDataSource("/node/logs");

        this.queryTags();
    }

    logSleepTime: number = 500;

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
        //$("#pager").kendoPager({
        //    dataSource: this.dataSource
        //});
    }

    @computedFrom(nameof.full(this.nodeServices.apiReponse, -2))
    get message(): string {
        return this.nodeServices.apiReponse.message;
    }

    async queryTags() {
        while (true) {
            await this.nodeServices.queryTransactionStream(this.curNeighborId, this.tagQuery)
                .then(response => {
                    if (response != null && response.rows != null && response.rows.length > 0) {
                        this.logs.unshift.apply(this.logs, response.rows);
                        if (this.logs.length > 60) {
                            for (var i = this.logs.length; i > 58; i--) {
                                this.logs.pop();
                            }
                        }
                    }                    
                });
            await this.nodeServices.sleep(500);
        }
    }


    async startNeighbor() {

        this.nodeServices.createNode(this.url).then(response => {
            if(response.rows != null)
                this.curNeighborId = response.rows;
        });
    }

    async stopNeighbor() {
        await this.nodeServices.stopListner(this.curNeighborId);
    }

    //binds
    url: string = 'tcp://192.168.1.2:15600';
    tagQuery: string = '';
    curNeighborId: number = 15600;

    vericalRows = [
        { collapsible: false, resizable: false, scrollable: false, size: '180px', min: '180px' },
        { collapsible: false, resizable: false, scrollable: false },
        { collapsible: false, resizable: false, scrollable: false, size: '30px', min: '30px' }
    ];
}

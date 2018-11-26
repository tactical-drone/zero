import {IoConfiguration} from "../config/IoConfiguration";
import {HttpClient,json} from 'aurelia-fetch-client';
import { Container } from 'aurelia-framework';
import { IoApiReturn } from "./IoApiReturn";

import * as Promise from "bluebird";

export class IoApi {
    httpClient: HttpClient;
    zcfg: IoConfiguration;

    apiReponse: IoApiReturn = undefined;

    //Used to cancel spin loops
    spinners: boolean = true;

    constructor() {
        this.httpClient = Container.instance.get(HttpClient);
        this.zcfg = Container.instance.get(IoConfiguration);
        this.apiReponse = new IoApiReturn(false,"We are the borg!", undefined);
        this.httpClient.configure(config => {
            config
                .useStandardConfiguration()
                .withDefaults({
                    headers: {
                        'Accept': 'application/json',
                        'Authorization': 'Bearer ' + this.zcfg.scfg.token
                    }
                });
        });
    }
    
    kendoDataSource(baseUrl: string): kendo.data.DataSource {

        let token = 'Bearer ' + this.zcfg.scfg.token;

        return new kendo.data.DataSource({
            transport: {
                read: {
                    url: this.zcfg.apiUrl +  baseUrl,
                    type: "GET",
                    dataType: "json",
                    beforeSend: function (xhr) {
                        xhr.setRequestHeader('Authorization', token);
                    }
                },
            },
            schema: {
                data: "rows"
            }
        }); 
    }

    kendoDataStream(baseUrl: string): kendo.data.DataSource {

        let token = 'Bearer ' + this.zcfg.scfg.token;

        return new kendo.data.DataSource({
            transport: {
                update: {
                    url: this.zcfg.apiUrl + baseUrl,
                    type: "GET",
                    dataType: "json",
                    beforeSend: function (xhr) {
                        xhr.setRequestHeader('Authorization', token);
                    }
                },
            },
            schema: {
                data: "rows"
            }
        });
    }

    async stream(apiCall: (response)=>Promise<IoApiReturn>, buffer: kendo.data.ObservableArray, rate: number) {
        while (this.spinners) {
            await this.sleep(rate);
            await apiCall(null).then(response => {
                buffer.push.apply(buffer, response.rows);                                
            });
        }
    }

    //ProcessIncomingItems()
    //{
    //        var dataSource = ItemsViewModel.get('Items');

    //        var grid = $('#ItemsGrid').data('kendoGrid');

    //        var row = grid.select();

    //        var item = grid.dataItem(row);

    //        if (dataSource.data().length > (grid.pager.page() * grid.pager.pageSize())) {
    //        $.merge(dataSource.data(), data);

    //        if ((dataSource.data().length / grid.pager.pageSize()) > grid.pager.totalPages()) grid.pager.page(grid.pager.page());
    //    }
    //    else {
    //        dataSource.data().push.apply(dataSource.data(), data);
    //    }

    //    if (item != null) {
    //        grid.selectDataItem(item);
    //    }
    //}

    async post(baseUrl: string, parms: any): Promise<IoApiReturn>{
        return await this.httpClient.fetch(this.zcfg.apiUrl + baseUrl,
                {
                    method: 'post',
                    body: json(parms)
                })
            .then(response => response.json())
            .then(response => this.apiReponse = response);
    }

    async get(baseUrl: string, parms: any = null) :Promise<IoApiReturn> {
         return await this.httpClient.fetch(this.zcfg.apiUrl + baseUrl,
                {
                    method: 'get'
                })
            .then(response => response.json())
            .then(response => this.apiReponse = response);
    }

    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}
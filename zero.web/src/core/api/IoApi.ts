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
                    mode: 'cors',
                    headers: {
                        'Accept': 'application/json',
                        'Content-Type': 'application/json',
                        'Authorization': 'Bearer ' + this.zcfg.scfg.token
                    }
                });
        });

        this.get("/bootstrap/kind").then(response => {
            this.zcfg.apiKind = response.rows;
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

    async streamShift(apiCall: (response)=>Promise<IoApiReturn>, buffer: kendo.data.ObservableArray, rate: number) {
        while (this.spinners) {
            await this.sleep(rate);
            await apiCall(null).then(response => {
                //Array.prototype.push.apply(buffer, response.rows);
                buffer.push.apply(buffer, response.rows);                                
            });
        }
    }

    async streamUnShift(apiCall: (response) => Promise<IoApiReturn>, buffer: kendo.data.ObservableArray, rate: number) {
        while (this.spinners) {
            await this.sleep(rate);
            await apiCall(null).then(async response => {                
                buffer.unshift.apply(buffer, response.rows);
                if (buffer.length > 60) {
                    for (var i = buffer.length; i > 58; i--) {
                        await this.sleep(0);
                        buffer.pop();
                    }
                }                
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

    async post(baseUrl: string, params: any): Promise<IoApiReturn>{
        return await this.httpClient.fetch(this.zcfg.apiUrl + baseUrl,
                {
                    method: 'post',
                    body: json(params)
                })
            .then(response => {
                //if (response.bodyUsed)
                    return response.json();
                //return new IoApiReturn(false, "No data returned", undefined);
            })
            .then(response => this.apiReponse = response);
    }

    async get(baseUrl: string, params: any = null) :Promise<IoApiReturn> {
         return await this.httpClient.fetch(this.zcfg.apiUrl + baseUrl,
                {
                    method: 'get'
                })
             .then(response => {
                 //if (response.bodyUsed)
                     return response.json();
                 //return new IoApiReturn(false, "No data returned", undefined);
             })
            .then(response => this.apiReponse = response);
    }

    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}
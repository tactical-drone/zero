import {IoConfiguration} from "../config/IoConfiguration";
import {HttpClient,json} from 'aurelia-fetch-client';
import { Container } from 'aurelia-framework';
import { IoApiReturn } from "./IoApiReturn";

export class IoApi {
    httpClient: HttpClient;
    zcfg: IoConfiguration;

    response: IoApiReturn = undefined;

    constructor() {
        this.httpClient = Container.instance.get(HttpClient);
        this.zcfg = Container.instance.get(IoConfiguration);
        this.response = new IoApiReturn(false,"We are the borg!", undefined);
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

    post(baseUrl: string, parms: any) {
        return this.httpClient.fetch(this.zcfg.apiUrl + baseUrl,
                {
                    method: 'post',
                    body: json(parms)
                })
            .then(response => response.json())
            .then(response => this.response = response);
    }

    async get(baseUrl: string, parms: any = null) {
         return await this.httpClient.fetch(this.zcfg.apiUrl + baseUrl,
                {
                    method: 'get'
                })
            .then(response => response.json())
            .then(response => this.response = response);
    }
}
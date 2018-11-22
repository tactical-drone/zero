import {IoConfiguration} from "../config/IoConfiguration";
import {HttpClient,json} from 'aurelia-fetch-client';
import { Container, observable } from 'aurelia-framework';
import { IoApiReturn } from "./IoApiReturn";

class Promise<T0 extends IoApiReturn>{}

export class IoApi {
    httpClient: HttpClient;
    zcfg: IoConfiguration;

    response: IoApiReturn = undefined;

    constructor() {
        this.httpClient = Container.instance.get(HttpClient);
        this.zcfg = Container.instance.get(IoConfiguration);
        this.response = new IoApiReturn(false,"We are the borg!");
        this.httpClient.configure(config => {
            config
                .useStandardConfiguration()
                .withDefaults({
                    headers: {
                        'Accept': 'application/json',
                        'Authorization': 'Bearer ' + this.zcfg.scfg.token
                    },
                });
        });
    }

    post(baseUrl: string, parms: any): Promise<IoApiReturn> {
        return this.httpClient.fetch(this.zcfg.apiUrl + baseUrl,
                {
                    method: 'post',
                    body: json(parms)
                })
            .then(response => response.json())
            .then(response => this.response = response);
    }
}
import { inject } from 'aurelia-framework';
import { HttpClient } from 'aurelia-fetch-client';

@inject('serverConfig', HttpClient)
export class app {
    data;
    message = 'We are the borg.';
    http;
    serverConfig;

    constructor(serverConfig, http) {
        this.serverConfig = serverConfig;
        this.http = http;
        this.http.configure(config => {
            config
                .useStandardConfiguration()
                .withDefaults({
                    mode: 'cors',
                    headers: {
                        'Accept': 'application/json',
                        'Authorization': 'Bearer ' + this.serverConfig.token,
                    },                    
                });
        });   
        this.loadData();
    }

    loadData() {
        this.http.fetch('http://localhost:14256/api')
            .then(response => response.json())
            .then(result =>this.message = result);
    }
}

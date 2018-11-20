import { inject } from 'aurelia-framework';
import { HttpClient } from 'aurelia-fetch-client';

@inject('serverConfig')
export class app {
    data;
    message = 'We are the borg.';
    http;
    serverConfig;

    constructor(serverConfig) {
        this.serverConfig = serverConfig;
        this.http = new HttpClient();
        this.http.configure(config => {
            config
                .useStandardConfiguration()
                .withBaseUrl('/api')
                .withDefaults({
                    mode: 'cors',
                    headers: { 
                        'Accept': 'application/json',
                        'Authorization': 'Bearer ' + this.serverConfig.token
                    },                    
                });
        });   
//this.loadData();
    }

    loadData() {
        this.http.fetch('http://localhost:14256/api')
            .withDefaults({
                mode: 'no-cors',
                headers: {
                    'Accept': 'application/json',
                    'Authorization': 'Bearer ' + this.serverConfig.token
                },
            })
            .then(response => response.json())
            .then(result => {
                this.message = result;
            });
    }
}

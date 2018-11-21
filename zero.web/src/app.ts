import { inject } from 'aurelia-framework';
import { HttpClient, json } from 'aurelia-fetch-client';

@inject('serverConfig', HttpClient)
export class app {
    data;
    message = 'We are the borg.';
    http;
    serverConfig;
    url: string;
    port: number;

    constructor(serverConfig, http) {
        this.serverConfig = serverConfig;
        this.http = http;
        this.http.configure(config => {
            config
                .useStandardConfiguration()
                .withDefaults({                    
                    headers: {
                        'Accept': 'application/json',
                        'Authorization': 'Bearer ' + this.serverConfig.token,
                    },                    
                });
        });   
    }

    createNode() {

        let address = {
            "url" : this.url,
            "port": this.port
        }

        this.http.fetch('http://localhost:14256/api/node', {
                method: 'post',
                body: json(address)
            })
            .then(response => response.json())
            .then(result =>this.message = "success =" + result.success + ", " + result.message);
    }
}

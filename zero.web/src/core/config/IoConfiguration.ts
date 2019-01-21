import {inject} from 'aurelia-framework';

@inject('serverConfig')
export class IoConfiguration {

    constructor(serverConfig) {
        this.scfg = serverConfig;
    }

    scfg : any; 
    apiUrl: string = 'http://localhost:14265';
    apiKind: string = '';
}
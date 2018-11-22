import {inject, computedFrom, observable} from 'aurelia-framework';
import { IoNodeServices } from 'services/IoNodeService';
import {} from "ts-nameof";

@inject(IoNodeServices)
export class app {
    constructor(nodeService:IoNodeServices) {
        this.nodeServices = nodeService;
    }

    nodeServices: IoNodeServices = undefined;

    @computedFrom(nameof.full(this.nodeServices.response, -2))
    get message(): string {
        return this.nodeServices.response.message;
    }

    //binds
    url: string;
    port: number;
}

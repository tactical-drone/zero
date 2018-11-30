import { IoApi } from "core/api/IoApi";
import { IoApiReturn } from "../core/api/IoApiReturn";
import * as Promise from "bluebird";

export class IoNodeServices extends IoApi {
    constructor() {
        super();
    }

    async createNode(url: string): Promise<IoApiReturn> {
        return this.post('/api/node', { "url": url});
    }  

    async getLogs(): Promise<IoApiReturn>{
        return this.get('/api/node/logs');
    }

    async queryTransactionStream(id: number, tagQuery: string): Promise<IoApiReturn> {
        return this.get('/api/node/stream/'+id+'?tagQuery=' + tagQuery);
    }    

    async stopListner(id:number): Promise<IoApiReturn> {
        return this.get('/api/node/stopListener/' + id);
    }
}
import { IoApi } from "core/api/IoApi";

export class IoNodeServices extends IoApi {
    constructor() {
        super();
    }

    async createNode(url: string, port: number) {
        return this.post('/api/node', { "url": url, "port": port });
    }  

    async getLogs() {
        return this.get('/api/node/logs');
    }
    
}
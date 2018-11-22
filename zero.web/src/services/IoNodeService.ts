import {IoApi} from 'core/api/IoApi';
import { IoApiReturn } from 'core/api/IoApiReturn';

export class IoNodeServices extends IoApi {
    constructor() {
        super();
    }

    async createNode(url: string, port: number): Promise<IoApiReturn> {
        return this.post('/api/node', { "url": url, "port": port });
    }    
}
export class IoApiReturn {
    constructor(success: boolean, message: string, rows: any) {
        this.success = success;
        this.message = message;
        this.rows = rows;        
    }
    success: boolean;
    message: string;
    rows : any;
}
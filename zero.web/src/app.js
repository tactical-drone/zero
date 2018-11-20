var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
import { inject } from 'aurelia-framework';
import { HttpClient } from 'aurelia-fetch-client';
var app = /** @class */ (function () {
    function app(serverConfig) {
        var _this = this;
        this.message = 'We are the borg.';
        this.serverConfig = serverConfig;
        this.http = new HttpClient();
        this.http.configure(function (config) {
            config
                .useStandardConfiguration()
                .withBaseUrl('/api')
                .withDefaults({
                mode: 'cors',
                headers: {
                    'Accept': 'application/json',
                    'Authorization': 'Bearer ' + _this.serverConfig.token
                },
            });
        });
        //this.loadData();
    }
    app.prototype.loadData = function () {
        var _this = this;
        this.http.fetch('http://localhost:14256/api')
            .withDefaults({
            mode: 'no-cors',
            headers: {
                'Accept': 'application/json',
                'Authorization': 'Bearer ' + this.serverConfig.token
            },
        })
            .then(function (response) { return response.json(); })
            .then(function (result) {
            _this.message = result;
        });
    };
    app = __decorate([
        inject('serverConfig'),
        __metadata("design:paramtypes", [Object])
    ], app);
    return app;
}());
export { app };
//# sourceMappingURL=app.js.map
/// <reference path="../node_modules/ts-nameof/ts-nameof.d.ts" />
// we want font-awesome to load as soon as possible to show the fa-spinner
import {Aurelia} from 'aurelia-framework'
import environment from './environment';
import { PLATFORM } from 'aurelia-pal';
import "kendo-ui-core";
//import "@progress/kendo-ui"
//import '@progress/kendo-ui/js/kendo.all'
//import '@progress/kendo-ui/js/kendo.listview'
//import '@progress/kendo-ui/css/web/kendo.common.min.css'
//import '@progress/kendo-ui/css/web/kendo.default.min.css'
//import '@progress/kendo-ui/css/web/kendo.bootstrap.min.css'

import * as Bluebird from 'bluebird';
//import * as Promise from "bluebird";


//<link rel="stylesheet" href = "/../node_modules/kendo-ui-core/css/web/kendo.common.core.min.css" >
   //<link rel="stylesheet" href = "/../node_modules/kendo-ui-core/css/web/kendo.default.min.css" >

// <reference types="aurelia-loader-webpack/src/webpack-hot-interface"/>

// remove out if you don't want a Promise polyfill (remove also from webpack.config.js)
Bluebird.config(
    {
        //longStackTraces: true,
        warnings: { wForgottenReturn: false }
    });

export function configure(aurelia: Aurelia) {
  aurelia.use
      .standardConfiguration()      
      .plugin(PLATFORM.moduleName("aurelia-kendoui-bridge"), (kendo) => kendo.detect().notifyBindingBehavior())
      //.plugin(PLATFORM.moduleName("aurelia-templating"))
      //.plugin(PLATFORM.moduleName("kendo-ui-core"))
      //.plugin(PLATFORM.moduleName("aurelia-dependency-injection"))
      //.plugin(PLATFORM.moduleName("aurelia-templating-resources"))
      //.plugin(PLATFORM.moduleName("aurelia-templating-binding"))
      //.plugin(PLATFORM.moduleName("aurelia-templating-router"))

    
            .feature(PLATFORM.moduleName('resources/index'));

  // Uncomment the line below to enable animation.
  //aurelia.use.plugin(PLATFORM.moduleName('aurelia-animator-css'));
  // if the css animator is enabled, add swap-order="after" to all router-view elements

  // Anyone wanting to use HTMLImports to load views, will need to install the following plugin.
  // aurelia.use.plugin(PLATFORM.moduleName('aurelia-html-import-template-loader'));

  aurelia.use.developmentLogging(environment.debug ? 'debug' : 'warn');

  if (environment.testing) {
    aurelia.use.plugin(PLATFORM.moduleName('aurelia-testing'));
  }

    aurelia.container.registerInstance(
        'serverConfig',
        Object.assign({}, (aurelia.host as HTMLElement).dataset));    

  return aurelia.start().then(() => aurelia.setRoot(PLATFORM.moduleName('app'), document.body));
}

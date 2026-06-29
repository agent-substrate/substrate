"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const Defaults_1 = require("../Defaults");
const registration_1 = require("./registration");
// export the last socket layer
const makeWASocket = (config) => ((0, registration_1.makeRegistrationSocket)({
    ...Defaults_1.DEFAULT_CONNECTION_CONFIG,
    ...config
}));
exports.default = makeWASocket;

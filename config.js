const os = require('os');
const fs = require('fs');
const mkdirp = require('mkdirp');
const path = require('path');
const _ = require('underscore');

const msFilesBasePath = 'msdata';
exports.config = {};
exports.auth = null;

const callbackFuncs = [];

exports.init = (serviceName) => {
    let err = null;
    exports.serviceName = serviceName;

    console.log('Initialising configurations...');

    const winHomeDir = process.env[(process.platform === 'win32') ? 'USERPROFILE' : 'HOME'];

    if (os.platform() === 'win32') {
        exports.msFilesPath = winHomeDir + '\\Documents\\' + msFilesBasePath + '\\' + exports.serviceName + '\\';
    } else if (os.platform() === 'linux') {
        exports.msFilesPath = '/' + msFilesBasePath + '/' + exports.serviceName + '/';
    } else if (os.platform() === 'darwin') {
        exports.msFilesPath = '/' + msFilesBasePath + '/' + exports.serviceName + '/';
    }

    //Attempting to open or create file directory
    mkdirp.sync(exports.msFilesPath, (err) => {
        if (err) {
            console.error('Unable to open or create service directory: ' + exports.msFilesPath);
            throw (err);
        }
    });

    //Initialising config directory
    configPath = path.join(exports.msFilesPath, 'config');

    mkdirp.sync(configPath, (err) => {
        if (err) {
            console.error('Unable to open or create config directory: ' + configPath + '. Error: ' + err);
            throw(err);
        }
    });

    loadConfig();

    //Config reload call initialisation (10 seconds)
    setInterval(loadConfig, 10000);

    console.log('Configurations initialised successfully!');
};

const loadConfig = () => {
    let newConfig = {};
    let newAuth = null;
    let isReadError = false;

    const confFiles = fs.readdirSync(configPath);
    for (let i in confFiles) {
        let currFileName = confFiles[i];

        if (path.extname(currFileName) !== '.json') continue;

        let currConfigName = currFileName.replace(/\.[^/.]+$/, "");
        let currFilePath = path.join(configPath, currFileName);

        let currConfig = null;
        try {
            currConfig = JSON.parse(fs.readFileSync(currFilePath, 'utf8'));
            if (currConfigName === 'auth') {
                newAuth = currConfig;
            } else {
                newConfig[currConfigName] = currConfig;
            }
        } catch (err) {
            isReadError = true;
            console.error('Unable to read configuration file: ' + currFilePath + '. Error: ' + err);
        }
    }

    if (!isReadError) {
        if (newConfig && Object.keys(newConfig).length > 0) {
            if (Object.keys(exports.config).length === 0 || !_.isEqual(exports.config, newConfig)) {
                let oldConfig = exports.config;
                exports.config = newConfig;
                console.log('New configuration file loaded: ' + JSON.stringify(exports.config));
                for (let i = 0; i < callbackFuncs; i++) {
                    callbackFuncs[i](oldConfig, newConfig);
                }
            }
        }
        if (newAuth) {
            if (exports.auth === null || !_.isEqual(exports.auth, newAuth)) {
                exports.auth = newAuth;
                console.log('New authentication data loaded: ***');
            }
        }
    }
};

exports.regChangesCallback = (changesCallback) => {
    let cl = callbackFuncs.length;
    callbackFuncs[cl] = changesCallback;
};
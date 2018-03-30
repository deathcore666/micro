const conf = require('./config.js');
const log = require('./log.js');
const kafka = require('kafka-node');

const HighLevelProducer = kafka.HighLevelProducer;
const Producer = kafka.Producer;
const Client = kafka.Client;
const KeyedMessage = kafka.KeyedMessage;

let kclient = null;
let kproducer = null;

let isKafkaClientReady = false;
let isKafkaProducerReady = false;
exports.isReady = false;

let kafkaConnString = null;
let kafkaCliendId = null;
let initCallback = null;

let queue = [];
let kconf = null;

exports.send = (topic, msg, key = null) => {
    let kmsg = {topic: topic, messages: []};

    let keyedmessage = null;
    let message = null;

    if (key != null) {
        kmsg.messages[0] = new KeyedMessage(key, msg);
    } else {
        kmsg.messages[0] = msg;
    }

    queue.push(kmsg);
};

exports.initProducer = (callback) => {
    console.log('Initialising a kafka connection...');

    cluster = conf.config.main.kafkaCluster;
    console.log('Connecting to Kafka cluster: ', cluster);

    isConfFound = false;

    if (conf.config !== null && conf.config.kafka !== null) {

        kafkaArray = conf.config.kafka;
        for (let i = 0; i < kafkaArray.length; i++) {
            if (kafkaArray[i].kafkaCluster !== null && kafkaArray[i].kafkaCluster === cluster) {
                isConfFound = true;
                kconf = kafkaArray[i];
            }
        }
    }

    initCallback = callback;
    if (!isConfFound) {
        let msg = 'Kafka configs couldn\'t be found!';
        log.error(msg);
        initCallback(msg);
        return 1;
    }


    setInterval(sendQueue, 500);
    connectKafka();
};

const connectKafka = () => {
    let needReconnect = false;

    if (kconf != null) {
        if (kconf.kafkaConnectionString !== null && kconf.kafkaConnectionString !== kafkaConnString) {
            needReconnect = true;
            isKafkaClientReady = false;
            isKafkaProducerReady = false;
            exports.isReady = false;
        }

        if (kconf.kafkaClientId !== null && kconf.kafkaClientId !== kafkaCliendId) {
            needReconnect = true;
            isKafkaClientReady = false;
            isKafkaProducerReady = false;
            exports.isReady = false;
        }

        if (needReconnect === false) {
            console.log("Connection to kafka hasn\'t changed!")
        }
    }

    if (needReconnect) {

        kafkaConnString = kconf.kafkaConnectionString;
        kafkaCliendId = kconf.kafkaClientId;

        console.log('Connecting to Kafka Brokers...');

        if (kclient !== null && isKafkaClientReady) {
            kclient.close();
        }

        kclient = new kafka.KafkaClient({
            kafkaHost: kafkaConnString,
            autoConnect: true
        });

        kclient.on('error', onKafkaClientError);
        kclient.on('ready', onKafkaClientReady);

        kproducer = new  HighLevelProducer(kclient, { requireAcks : 1, partitionerType: 2 });

        kproducer.on('error', onKafkaProducerError);
        kproducer.on('ready', onKafkaProducerReady);
    }
};

const onKafkaClientError = (err) => {
    isKafkaClientReady = false;
    exports.isReady = false;
    let msg = 'Error connecting Kafka client: ' + err;
    log.error(msg);
    initCallback('Error connecting Kafka client: ' + err);
};

const onKafkaClientReady = (err) => {
    exports.isReady = true;
    isKafkaClientReady = true;
};

const onKafkaProducerError = (err) => {
    isKafkaProducerReady = false;
    exports.isReady = false;
    let msg = 'Error initialising a Kafka producer: ' + err;
    log.error(msg);
    initCallback('Error initialising a Kafka producer: ' + err);
};

const onKafkaProducerReady = (err) => {
    isKafkaProducerReady = true;
    exports.isReady = true;

    console.log('Kafka producer connected successfully: ' + kconf.kafkaConnectionString + ', CleintID" ' + kconf.kafkaClientId);

    conf.regChangesCallback(connectKafka);
    initCallback(false);
};

const sendQueue = () => {
    if (queue.length === 0) return;

    if(!isKafkaClientReady || !isKafkaProducerReady) {
        return;
    }

    let payloads = [];
    let qlen = queue.length;

    for (let i = 0;i < qlen;i++ ) {
        payloads[i] = queue[i];
    }

    try {
        kproducer.send(payloads, (err, data) => {
            if (err) {
                let msg = 'Unable to send messages to Kafka. Error: ' + err + '. Messages: ' + JSON.stringify(payloads);
                console.log(msg);
                log.error(msg);
            } else {
                let msg = 'Message successfully send to Kafka. Message: ' + JSON.stringify(payloads) + ', Kafka response: ' +
                    JSON.stringify(data);
                console.log(msg);
                log.debug(msg);
                queue.splice(0, qlen);
            }
        });
    } catch(err) {
        let msg = 'Unable to send messages to Kafka. Error: ' + err + '. Messages: ' + JSON.stringify(payloads);
        console.log(msg);
        log.error(msg);
    }
};
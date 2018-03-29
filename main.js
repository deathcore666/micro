const conf = require('./config');
const log = require('./log');
const kprod = require('./kafka-producer');

const serviceName = 'ora';

conf.init(serviceName);
log.init(serviceName);
kprod.initProducer(onProducerInitEnd);

const onProducerInitEnd = (err) => {
    if(err) {
        console.log('Unable to connect to Kafka. Error: ' + err);
        return 1;
    } else {
        console.log("ready to produce!")
    }

};
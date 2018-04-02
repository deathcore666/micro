const conf = require('./config');
const log = require('./log');
const kprod = require('./kafka-producer');

const serviceName = 'ora';

const onProducerInitEnd = (err) => {
    if(err) {
        console.log('Unable to connect to Kafka. Error: ' + err);
        return 1;
    } else {
        console.log("ready to produce!")
    }

};
conf.init(serviceName);
log.init(serviceName);
kprod.init(onProducerInitEnd);
kprod.send('replicated-test-topic', 'Message from docker');

require('./spec/helpers/env');
const { TraceUtils } = require('@themost/common');
const { JsonLogger } = require('@themost/json-logger');
TraceUtils.useLogger(new JsonLogger({
    format: 'raw',
}))
// set timeout to 30 seconds
jest.setTimeout(30000);
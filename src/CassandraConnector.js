const { Client } = require('cassandra-driver');
const { CassandraCqlFormatter } = require('./CassandaCqlFormatter');
const { TraceUtils } = require('@themost/common');

/**
 * @implements {import('@themost/common').DataAdapterBase}
 */
class CassandraConnector {
    /**
     * @type {import('cassandra-driver').Client}
     */
    rawConnection = null;

    constructor(options) {
        this.options = options;
    }

    open(callback) {
        if (this.rawConnection) {
            return callback();
        }
        this.rawConnection = new Client(this.options);
        this.rawConnection.connect().then(() => {
            return callback();
        }).catch((err) => {
            return callback(err);
        });
    }

    openAsync() {
        return new Promise((resolve, reject) => {
            void this.open((err) => {
                if (err) {
                    return reject(err);
                }
                return resolve();
            });
        });
    }

    close(callback) {
        if (this.rawConnection) {
            return this.rawConnection.shutdown((err) => {
                this.rawConnection = null;
                return callback(err);
            });
        }
        return callback();
    }

    closeAsync() {
        return new Promise((resolve, reject) => {
            this.close((err) => {
                if (err) {
                    return reject(err);
                }
                return resolve();
            });
        });
    }

    execute(query, params, callback) {
        void this.open((err) => {
            if (err) {
                return callback(err);
            }
            let sql;
            try {
                sql = (typeof query === 'string') ? query : new CassandraCqlFormatter().format(query);
            } catch (sqlError) {
                return this.close(() => {
                    return callback(sqlError);
                });
            }
            TraceUtils.debug(`SQL: ${sql}`);
            return this.rawConnection.execute(sql, params, (err, result) => {
                if (err) {
                    TraceUtils.error(`SQL: ${sql}`);
                    // forcibly close connection
                    this.close(() => {
                        return callback(err);
                    });
                }
                return callback(null, result && result.rows);
            });
        });
    }

    executeAsync(query, params) {
        return new Promise((resolve, reject) => {
            this.execute(query, params, (err, results) => {
                if (err) {
                    return reject(err);
                }
                return resolve(results);
            });
        });
    }

    formatType(type) {
        return new CassandraCqlFormatter().formatType(type);
    }

}

export {
    CassandraConnector
}
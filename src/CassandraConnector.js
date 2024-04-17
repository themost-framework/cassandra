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
            const queryOptions = {
                prepare: true
            };
            return this.rawConnection.execute(sql, params, queryOptions, (err, result) => {
                if (err) {
                    TraceUtils.error(`SQL: ${sql}`);
                    // forcibly close connection
                    return this.close(() => {
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

    /**
     * 
     * @param {string} name 
     * @returns {import('./CassandraConnector').CassandraKeyspace}
     */
    keyspace(name) {
        const self = this;
        const formatter = new CassandraCqlFormatter();
        return {
            exists(callback) {
                void self.execute('SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?', [name], (err, results) => {
                    if (err) {
                        return callback(err);
                    }
                    return callback(null, results.length > 0);
                });
            },
            existsAsync() {
                return new Promise((resolve, reject) => {
                    void this.exists((err, result) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(result);
                    });
                });
            },
            create(options, callback) {
                const keyspace = formatter.escapeName(name);
                const replication = formatter.stringify((options && options.replication) || {
                    'class': 'SimpleStrategy',
                    'replication_factor': 1
                });
                const sql = `CREATE KEYSPACE IF NOT EXISTS ${keyspace} WITH replication = ${replication}`
                void self.execute(sql, [
                ], (err) => {
                    if (err) {
                        return callback(err);
                    }
                    return callback();
                });
            },
            createAsync(options) {
                return new Promise((resolve, reject) => {
                    void this.create(options, (err) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve();
                    });
                });
            }
        }
    }

    /**
     * 
     * @param {string} name 
     * @returns {import('@themost/common').DataAdapterTable}
     */
    table(name) {
        const self = this;
        const {keyspace} = self.options;
        const formatter = new CassandraCqlFormatter();
        return {
            exists(callback) {
                void self.execute('SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?', [
                    keyspace,
                    name
                ], (err, results) => {
                    if (err) {
                        return callback(err);
                    }
                    return callback(null, results.length > 0);
                });
            },
            existsAsync() {
                return new Promise((resolve, reject) => {
                    void this.exists((err, result) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(result);
                    });
                });
            },
            columns(callback) {
                void self.execute(`
                SELECT column_name AS name, type, kind  FROM system_schema.columns WHERE table_name = ? AND keyspace_name=?;
                `, [
                    name,
                    keyspace
                ], (err, results) => {
                    if (err) {
                        return callback(err);
                    }
                    return callback(null, results.map((result) => {
                        return {
                            name: result.name,
                            type: result.type,
                            primary: result.kind === 'partition_key'
                        };
                    }));
                });
            },
            columnsAsync() {
                return new Promise((resolve, reject) => {
                    void this.columns((err, result) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(result);
                    });
                });
            },
            /**
             * 
             * @param {import('./CassandraConnector').CassandaTableField[]} fields 
             * @param {*} callback 
             */
            create(fields, callback) {
                let sql = '';
                try {
                    let columns = fields.map((field) => {
                        return `${formatter.escapeName(field.name)} ${formatter.formatType(field)}`;
                    }).join(',');
                    const primaryKeys = fields.filter((field) => field.primary).map((field) => field.name);
                    if (primaryKeys.length > 0) {
                        columns += ',';
                        columns += 'PRIMARY KEY (';
                        columns += primaryKeys.map((key) => formatter.escapeName(key)).join(',');
                        columns += ')';
                    }
                    sql = `CREATE TABLE IF NOT EXISTS "${keyspace}"."${name}" (${columns})`;
                } catch (error) {
                    return callback(error);
                }
                void self.execute(sql, [], (err) => {
                    if (err) {
                        return callback(err);
                    }
                    return callback();
                });
            },
            createAsync(fields) {
                return new Promise((resolve, reject) => {
                    void this.create(fields, (err) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve();
                    });
                });
            }
        }
    }

}

export {
    CassandraConnector
}
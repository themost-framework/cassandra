const { CassandraConnector } = require('../src/CassandraConnector');
const { QueryExpression, QueryEntity, ObjectNameValidator } = require('@themost/query');
const connectOptions = {
    contactPoints: [
        process.env.DB_HOST
    ],
    localDataCenter: process.env.DB_DATACENTER,
};

describe('CassandraAdapter', () => {

    const onValidateName = (event) => {
        // validate database object name by allowing qualified names e.g. dbo.Products
        event.valid = ObjectNameValidator.validator.test(event.name, true);
    };
    beforeAll(() => {
        ObjectNameValidator.validator.validating.subscribe(onValidateName);
    });
    afterAll(() => {
        //
        ObjectNameValidator.validator.validating.unsubscribe(onValidateName);
    });

    it('should create instance', () => {
        const db = new CassandraConnector(connectOptions);
        expect(db).toBeInstanceOf(CassandraConnector);
    });
    it('should connect', async () => {
        /**
         * @type {import('../src/CassandraConnector').CassandraConnector}
         */
        const db = new CassandraConnector(connectOptions);
        await expect(db.openAsync()).resolves.toBeFalsy();
        await db.closeAsync();
    });

    it('should execute sql', async () => {
        /**
         * @type {import('../src/CassandraConnector').CassandraConnector}
         */
        const db = new CassandraConnector(connectOptions);
        const results = await db.executeAsync(
            new QueryExpression().select('cluster_name', 'listen_address').from('system.local')
            );
        expect(results).toBeTruthy();
        const [{ cluster_name }] = results;
        expect(cluster_name).toBeTruthy();
        await db.closeAsync();
    });
});
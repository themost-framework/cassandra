const { CassandraConnector } = require('../src/CassandraConnector');
const { QueryExpression, QueryEntity, ObjectNameValidator } = require('@themost/query');

const connectOptions = {
    contactPoints: [
        process.env.DB_HOST
    ],
    localDataCenter: process.env.DB_DATACENTER,
    keyspace: process.env.DB_KEYSPACE,
};

describe('CassandraAdapter', () => {

    const onValidateName = (event) => {
        // validate database object name by allowing qualified names e.g. dbo.Products
        event.valid = ObjectNameValidator.validator.test(event.name, true);
    };
    /**
     * @type {CassandraConnector}
     */
    let db;
    beforeAll(async () => {
        db = new CassandraConnector(connectOptions);
        ObjectNameValidator.validator.validating.subscribe(onValidateName);
        db.keyspace('test').createAsync();
    });
    afterAll(async () => {
        //
        ObjectNameValidator.validator.validating.unsubscribe(onValidateName);
        await db.closeAsync();
    });

    it('should create instance', () => {
        const db = new CassandraConnector(connectOptions);
        expect(db).toBeInstanceOf(CassandraConnector);
    });
    
    it('should create keyspace', async () => {
        await db.keyspace('test').createAsync({ replication: { 'class': 'SimpleStrategy', 'replication_factor': 1 } });
        const exists = await db.keyspace('test').existsAsync();
        expect(exists).toBeTruthy();
    });

    it('should create table', async () => {
        await db.table('Thing').createAsync(
            [
                { name: 'id', type: 'Guid', primary: true },
                { name: 'name', type: 'Text' },
                { name: 'description', type: 'Text' },
                { name: 'dateCreated', type: 'DateTime' },
                { name: 'dateModified', type: 'DateTime' }
            ]
        );
        const exists = await db.table('thing').existsAsync();
        expect(exists).toBeTruthy();
    });
    
});
import { DataAdapterBase, DataAdapterDatabase, DataAdapterTable } from '@themost/common';

export declare interface CassandraCreateKeySpaceOptions {
    replication: {
        class: 'SimpleStrategy' | 'NetworkTopologyStrategy';
        replication_factor: number;
    }
}

export declare interface CassandaTableField {
    name: string;
    type: string;
    primary?: boolean;
    size?: number;
    scale?: number;
    many?: boolean;
}

export declare interface CassandaTableField extends DataAdapterTable {
    exists(callback: (err: Error, result: boolean) => void): void;
    existsAsync(): Promise<boolean>;
    create(fields: CassandaField[], callback: (err: Error) => void): void;
    createAsync(fields: CassandaField[]): Promise<void>;
    change(fields: CassandaField[], callback: (err: Error) => void): void;
    changeAsync(fields: CassandaField[]): Promise<void>;
    columns(callback: (err: Error, result: CassandaField[]) => void): void;
    columnsAsync(): Promise<CassandaField[]>;
}

export declare interface CassandraKeyspace {
    exists(callback: (err: Error, result: boolean) => void): void;
    existsAsync(): Promise<boolean>;
    create(options: CassandraCreateKeySpaceOptions, callback: (err: Error) => void): void;
    createAsync(options: CassandraCreateKeySpaceOptions): Promise<void>;
}

export declare class CassandraConnector implements DataAdapterBase {
    keyspace(name: string): CassandraKeyspace;
    table(name: string): DataAdapterTable;
}
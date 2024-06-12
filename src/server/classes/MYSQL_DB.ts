import {
    createPool,
    FieldPacket,
    OkPacket,
    Pool,
    PoolOptions,
    ResultSetHeader,
    RowDataPacket,
} from 'mysql2/promise';
import * as dotenv from 'dotenv';
import { FORMAT } from '../functions/FORMAT';
dotenv.config();

export interface ConditionClause {
    [key: string]: any;
}

export interface JoinClause {
    table: string; // Name of the table to join with
    type: 'INNER' | 'LEFT' | 'RIGHT' | 'FULL'; // Type of join
    on: string; // Join condition
    columns: string[]; // the columns to be selected from the right table
}

export class MYSQL_DB {
    static config: PoolOptions = {
        user: process.env.DB_USER,
        password: process.env.DB_PWD,
        host: process.env.DB_HOST,
        port: 25060,
        // database: 'economicnews',
        connectionLimit: 10,
        multipleStatements: true,
    } as PoolOptions;

    errors: string[];
    pool: Pool;

    constructor() {
        this.pool = createPool(MYSQL_DB.config);
        console.log('Pool created');
        this.errors = [];
    }
    private async executeQuery(query: string, params: any[]) {
        return await this.pool.execute(query + ';', params);
    }
    processResult<T>(result: any): T[] {
        if (
            Array.isArray(result) &&
            result.length > 0 &&
            Array.isArray(result[0])
        ) {
            return result[0] as T[];
        } else {
            return [];
        }
    }
    async SELECT<T>(
        tableName: string,
        options?: {
            whereClause?: ConditionClause;
            likeClause?: ConditionClause;
            joinClause?: JoinClause;
        }
    ): Promise<T[]> {
        if (!this.pool) {
            throw new Error(
                'Pool was not created. Ensure the pool is created when running the app.'
            );
        }

        try {
            const { whereClause, likeClause, joinClause } = options || {};

            const [whereClauseSQL, whereClauseParams] = FORMAT.whereClause(
                tableName,
                whereClause
            );
            const [likeClauseSQL, likeClauseParams] = FORMAT.likeClause(
                tableName,
                likeClause
            );

            let selectStatement = `SELECT * FROM ${tableName}\n`;

            if (joinClause && joinClause.columns) {
                // Add selected columns from the joined table
                const columns = joinClause.columns
                    .map((col: string) => `${joinClause.table}.${col}`)
                    .join(', ');
                console.log(`columns: ${columns}`);
                selectStatement = `SELECT ${tableName}.*, ${columns}\n`;
            }

            // Construct the JOIN clause separately
            let joinClauseSQL = '';

            if (joinClause) {
                joinClauseSQL = `${joinClause.type} JOIN ${joinClause.table} ON ${joinClause.on}`;
                console.log(`joinClause: ${joinClause}`);
                // Combine SELECT and JOIN clauses
                selectStatement += `FROM ${tableName}\n${joinClauseSQL}`;
            }

            // Combine WHERE and LIKE clauses
            let whereLikeClauses = '';

            if (whereClauseSQL) {
                whereLikeClauses += whereClauseSQL;
            }

            if (likeClauseSQL) {
                whereLikeClauses +=
                    (whereLikeClauses ? ' AND ' : '') + likeClauseSQL;
            }

            // Add the WHERE clause to the final query
            if (whereLikeClauses) {
                selectStatement += ` WHERE ${whereLikeClauses}`;
            }

            const params = [...whereClauseParams, ...likeClauseParams];

            console.log(`SQL: ${selectStatement} Params: ${params.join(', ')}`);

            // return [selectStatement, params];
            // Execute the query
            const result = await this.executeQuery(selectStatement, params);
            return this.processResult<T>(result);
        } catch (e) {
            console.warn(`Error in SELECT: ${e}`);
            throw new Error(`Error in SELECT: ${e}`);
        }
    }
    async UPDATE(
        table: string,
        values: Record<string, any>,
        whereClause: ConditionClause
    ): Promise<boolean> {
        if (!this.pool) {
            throw new Error(
                'Pool was not created. Ensure pool is created when running the app.'
            );
        }
        try {
            const [setClause, setParams] = FORMAT.setClause(values);
            const [whereClauseSQL, whereClauseParams] = FORMAT.whereClause(
                table,
                whereClause
            );

            console.log(`whereClauseSQL`, whereClauseSQL);
            console.log('whereClauseParams', whereClauseParams);

            const sql = `UPDATE ${table} SET ${setClause} WHERE ${whereClauseSQL}`;
            const params = [...setParams, ...whereClauseParams];

            console.log(`sql`, sql);
            console.log('params', params);

            const [result] = await this.pool.execute(sql, params);
            return (result as any).affectedRows === 1;
        } catch (e) {
            throw new Error(`Error in UPDATE: ${e}`);
        }
    }
    /**
     * Overwrites by default
     */
    async INSERT_BATCH_OVERWRITE<T extends Object>(
        data: T[],
        tableName: string
    ) {
        if (!this.pool) {
            throw new Error(
                'Pool was not created. Ensure the pool is created when running the app.'
            );
        }
        try {
            // Check if data is empty
            if (!data.length) {
                throw new Error('No data provided for batch insert.');
            }

            // Log the incoming data
            console.log('Data:', JSON.stringify(data, null, 2));

            // Construct the value placeholders and flatten the data
            const numKeys = Object.keys(data[0]).length;
            const oneArrayPlaceHolder = `(${Array(numKeys)
                .fill('?')
                .join(', ')})`;
            const valuePlaceholders = data
                .map(() => oneArrayPlaceHolder)
                .join(', ');

            // Convert dates and flatten data
            const values = data.flatMap((item) =>
                Object.values(item).map((value) => {
                    return value !== undefined ? value : null;
                })
            );

            const columns = Object.keys(data[0])
                .map((col) => `\`${col}\``)
                .join(', ');
            const updateColumns = Object.keys(data[0])
                .map((col) => `\`${col}\` = VALUES(\`${col}\`)`)
                .join(', ');

            // Construct the SQL query
            let sql = `INSERT INTO ${tableName} (${columns}) VALUES ${valuePlaceholders}`;
            sql += ` ON DUPLICATE KEY UPDATE ${updateColumns}`;

            // Detailed logging
            // console.log('Final SQL Query:', sql);
            // console.log('Query Parameters:', values);

            // Execute the query
            const [result]: [ResultSetHeader, any] = await this.pool.execute(
                sql,
                values
            );

            // Log the result object for debugging
            console.log(
                'INSERT_BATCH_OVERWRITE result:',
                JSON.stringify(result, null, 4)
            );

            const affected = result.affectedRows || 0;
            const changed = result.changedRows || 0;
            const inserted = affected - changed;

            console.log(
                `INSERT_BATCH_OVERWRITE: ${affected} rows affected, ${changed} rows changed, ${inserted} rows inserted.`
            );
            return { inserted, affected, changed };
        } catch (error) {
            console.error('SQL Execution Error:', error);
            throw new Error(
                `INSERT_BATCH_OVERWRITE Error: ${(error as Error).message}`
            );
        }
    }

    /**
     * remove all entries in @param table
     */
    async cleanTable(table: string): Promise<boolean> {
        try {
            if (!this.pool) {
                throw new Error(
                    'Pool was not created. Ensure the pool is created when running the app.'
                );
            }
            const deleteAllRecordsSql = `DELETE FROM ${table};`;
            await this.pool.execute(deleteAllRecordsSql);
            return true;
        } catch (e) {
            throw new Error(`cleanTable failed for table: ${table}: ${e}`);
        }
    }
    /**
     * Remove entried in @param table
     * where the column @param columnName
     * is older than @param targetDate of @type {Date}
     */
    async removeOldEntries(
        table: string,
        targetDate: Date,
        columnName: string
    ) {
        const funcName = `removeOldEntries`;
        // console.log(funcName);
        try {
            const formattedDate = this.formatDateToSQLTimestamp(targetDate);
            const removeOldSqlStatement = `
            DELETE FROM ${table}
            WHERE ${columnName} < '${formattedDate}';
        `;

            await this.pool.execute(removeOldSqlStatement);
        } catch (e) {
            throw new Error(`Error in ${funcName}: ${e}`);
        }
    }
    /**
     * This function is used to insert a single row into a table
     * and return the ID of the inserted row.
     * @param table
     * @param values
     * @returns
     */
    async INSERT_GETID(
        table: string,
        values: { [key: string]: any }
    ): Promise<
        [RowDataPacket[] | RowDataPacket[][] | ResultSetHeader, FieldPacket[]]
    > {
        console.log(`INSERT_GETID`);
        if (!this.pool) {
            throw new Error(
                'Pool was not created. Ensure pool is created when running the app.'
            );
        }

        const [setClause, setParams] = FORMAT.setClause(values);
        const sql = `INSERT INTO ${table} SET ${setClause}`;
        const params = [...setParams];

        // for debuggin we log the simple sql statement
        const plainSql = sql.replace(/\?/g, (match) =>
            typeof params[0] === 'string'
                ? `'${params.shift()}'`
                : params.shift()
        );

        try {
            return await this.pool.execute(plainSql);
        } catch (e) {
            console.error(e);
            throw new Error(`Error in INSERT_GETID`);
        }
    }
    formatDateToSQLTimestamp(date: Date): string {
        const year = date.getFullYear();
        const month = (date.getMonth() + 1).toString().padStart(2, '0');
        const day = date.getDate().toString().padStart(2, '0');
        const hours = date.getHours().toString().padStart(2, '0');
        const minutes = date.getMinutes().toString().padStart(2, '0');
        const seconds = date.getSeconds().toString().padStart(2, '0');
        return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
    }
}

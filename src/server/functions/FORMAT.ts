import { ConditionClause, JoinClause } from '../classes/MYSQL_DB';

export const FORMAT = {
    whereClause(
        tableName: string,
        whereClause?: ConditionClause
    ): [string, any[]] {
        let sql = '';
        const params: any[] = [];
        if (whereClause) {
            sql += Object.keys(whereClause)
                .map((key) => `${tableName}.${key} = ?`)
                .join(' AND ');
            Object.values(whereClause).forEach((value) => params.push(value));
        }
        return [sql, params];
    },
    /**
     * Here we pass the object to be formatted
     * AS IS. BRAINLESSSS
     * @param values
     * @returns
     */
    setClause<T extends Record<string, any>>(values: T): [string, any[]] {
        const setClause = Object.keys(values)
            .map((key) => `${key} = ?`)
            .join(', ');
        const setParams = Object.values(values);
        return [setClause, setParams];
    },
    likeClause(
        tableName: string,
        likeClause?: ConditionClause
    ): [string, any[]] {
        let sql = '';
        const params: any[] = [];

        if (likeClause) {
            sql +=
                Object.keys(likeClause)
                    .map((key, index) => {
                        const condition = `${tableName}.${key} LIKE ?`;
                        params.push(`%${likeClause[key]}%`);

                        return index === 0
                            ? `(${condition}`
                            : ` OR ${condition}`;
                    })
                    .join('') + ')';
        }

        return [sql, params];
    },
    // select(
    //     tableName: string,
    //     options?: {
    //         whereClause?: ConditionClause;
    //         likeClause?: ConditionClause;
    //         joinClause?: JoinClause;
    //     }
    // ): { selectStatement: string; params: any[] } {
    //     try {
    //         const { whereClause, likeClause, joinClause } = options || {};

    //         const [whereClauseSQL, whereClauseParams] = FORMAT.whereClause(
    //             tableName,
    //             whereClause
    //         );
    //         const [likeClauseSQL, likeClauseParams] = FORMAT.likeClause(
    //             tableName,
    //             likeClause
    //         );

    //         let selectStatement = `SELECT * FROM ${tableName}\n`;

    //         if (joinClause && joinClause.columns) {
    //             // Add selected columns from the joined table
    //             const columns = joinClause.columns
    //                 .map((col) => `${joinClause.table}.${col}`)
    //                 .join(', ');
    //             console.log(`columns: ${columns}`);
    //             selectStatement = `SELECT ${tableName}.*, ${columns}\n`;
    //         }

    //         // Construct the JOIN clause separately
    //         let joinClauseSQL = '';

    //         if (joinClause) {
    //             joinClauseSQL = `${joinClause.type} JOIN ${joinClause.table} ON ${joinClause.on}`;
    //             console.log(`joinClause: ${joinClause}`);
    //         }

    //         // Combine SELECT and JOIN clauses
    //         selectStatement += `FROM ${tableName}\n${joinClauseSQL}`;

    //         // Combine WHERE and LIKE clauses
    //         let whereLikeClauses = '';

    //         if (whereClauseSQL) {
    //             whereLikeClauses += whereClauseSQL;
    //         }

    //         if (likeClauseSQL) {
    //             whereLikeClauses +=
    //                 (whereLikeClauses ? ' AND ' : '') + likeClauseSQL;
    //         }

    //         // Add the WHERE clause to the final query
    //         if (whereLikeClauses) {
    //             selectStatement += ` WHERE ${whereLikeClauses}`;
    //         }

    //         const params = [...whereClauseParams, ...likeClauseParams];

    //         return {
    //             selectStatement,
    //             params,
    //         };
    //     } catch (e) {
    //         throw `Error in FORMAT.select: ${e}`;
    //     }
    // },
};

const express = require('express');
const sql = require('msnodesqlv8');
const mssql = require('mssql');

const app = express();
const port = 3000;

const localSqlConfig = {
    connectionString: "Driver={ODBC Driver 17 for SQL Server};Server=DESKTOP-87943JD\\SQLEXPRESS;Database=Aurify_bullions;Trusted_Connection=Yes;"
};

const awsSqlConfig = {
    user: 'admin',
    password: 'Aurify-bullions',
    server: 'bullions-database.cpggcgkawtf8.ap-south-1.rds.amazonaws.com',
    database: 'master', // Connect to master initially
    port: 1433,
    options: {
        encrypt: true,
        trustServerCertificate: true
    }
};

const targetDatabase = 'Aurify_bulli';

async function executeLocalSqlQuery(query) {
    console.log(`Executing SQL query on local DB: ${query}`);
    return new Promise((resolve, reject) => {
        sql.query(localSqlConfig.connectionString, query, (err, result) => {
            if (err) {
                console.error('Error executing SQL query on local DB:', err.message);
                reject(err);
            } else {
                console.log(`SQL query on local DB executed successfully. Returned ${result.length} rows.`);
                resolve(result);
            }
        });
    });
}

async function executeAwsSqlQuery(query) {
    console.log(`Executing SQL query on AWS DB: ${query}`);
    let pool;
    try {
        pool = await mssql.connect(awsSqlConfig);
        const result = await pool.request().query(query);
        console.log('SQL query on AWS DB executed successfully.');
        return result;
    } catch (err) {
        console.error('Error executing SQL query on AWS:', err.message);
        throw err;
    } finally {
        if (pool) {
            await pool.close();
        }
    }
}

async function ensureDatabase() {
    try {
        await executeAwsSqlQuery(`
            IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '${targetDatabase}')
            BEGIN
                CREATE DATABASE ${targetDatabase};
            END
        `);
        console.log(`Ensured database ${targetDatabase} exists.`);
        
        // Switch to the target database
        await executeAwsSqlQuery(`USE ${targetDatabase};`);
        console.log(`Switched to database ${targetDatabase}.`);
    } catch (err) {
        console.error('Error ensuring database exists:', err.message);
        throw err;
    }
}

async function getAccountTransactionSchema() {
    const schemaQuery = `
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'ACCOUNT_TRANSACTION'
        ORDER BY ORDINAL_POSITION
    `;
    return await executeLocalSqlQuery(schemaQuery);
}

async function createAccountTransactionTableInAws(schema) {
    const checkTableQuery = `
        USE ${targetDatabase};
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'ACCOUNT_TRANSACTION')
        BEGIN
            CREATE TABLE ACCOUNT_TRANSACTION (
                ${schema.map(col => {
                    let colDef = `${col.COLUMN_NAME} ${col.DATA_TYPE}`;
                    if (col.CHARACTER_MAXIMUM_LENGTH) {
                        colDef += `(${col.CHARACTER_MAXIMUM_LENGTH})`;
                    }
                    colDef += col.IS_NULLABLE === 'NO' ? ' NOT NULL' : ' NULL';
                    return colDef;
                }).join(', ')}
            )
        END
    `;
    await executeAwsSqlQuery(checkTableQuery);
}

async function migrateAccountTransactionData() {
    try {
        await ensureDatabase();
        
        const schema = await getAccountTransactionSchema();
        await createAccountTransactionTableInAws(schema);
        
        const localQuery = `SELECT TOP 5 * FROM ACCOUNT_TRANSACTION`;
        const localResult = await executeLocalSqlQuery(localQuery);
        
        const columnNames = schema.map(col => col.COLUMN_NAME).join(', ');
        const awsInsertQuery = `USE ${targetDatabase}; INSERT INTO ACCOUNT_TRANSACTION (${columnNames}) VALUES `;
        const values = localResult.map(row => {
            return `(${schema.map(col => {
                const value = row[col.COLUMN_NAME];
                if (value === null) return 'NULL';
                if (col.DATA_TYPE === 'datetime') {
                    return `'${value.toISOString()}'`;
                }
                return `'${value.toString().replace(/'/g, "''")}'`;
            }).join(', ')})`;
        }).join(',');
        
        const finalInsertQuery = awsInsertQuery + values;
        
        await executeAwsSqlQuery(finalInsertQuery);
        console.log('ACCOUNT_TRANSACTION data migration completed successfully.');
        
        const awsFetchQuery = `USE ${targetDatabase}; SELECT TOP 40 * FROM ACCOUNT_TRANSACTION`;
        const awsResult = await executeAwsSqlQuery(awsFetchQuery);
        return awsResult.recordset;
    } catch (err) {
        console.error('Error during ACCOUNT_TRANSACTION data migration:', err.message);
        throw err;
    }
}

app.get('/migrate-account-transaction', async (req, res) => {
    try {
        const migratedData = await migrateAccountTransactionData();
        res.status(200).json({
            message: 'ACCOUNT_TRANSACTION data migration completed.',
            data: migratedData
        });
    } catch (err) {
        console.error('Migration error:', err.message);
        res.status(500).send('An error occurred during ACCOUNT_TRANSACTION migration: ' + err.message);
    }
});

app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
});
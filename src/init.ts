import {
  QldbDriver,
  Result,
  TransactionExecutor,
} from "amazon-qldb-driver-nodejs";

const qldbDriver: QldbDriver = new QldbDriver(process.env.LEDGER_NAME || "");
const USERS_TABLE: string = process.env.USERS_TABLE || "";
const TRANSACTIONS_TABLE: string = process.env.TRANSACTIONS_TABLE || "";
const ACCOUNTS_TABLE: string = process.env.ACCOUNTS_TABLE || "";

const createTable = async (
  txn: TransactionExecutor,
  tableName: string
): Promise<number> => {
  const statement: string = `CREATE TABLE ${tableName}`;
  return await txn.execute(statement).then((result: Result) => {
    console.log(`Successfully created table ${tableName}.`);
    return result.getResultList().length;
  });
};

export const handler = async (event) => {
  console.log(JSON.stringify(event));
  try {
    const tables: string[] = await qldbDriver.getTableNames();
    await qldbDriver.executeLambda(async (txn: TransactionExecutor) => {
      if (!tables.includes(USERS_TABLE)) await createTable(txn, USERS_TABLE);
      if (!tables.includes(TRANSACTIONS_TABLE))
        await createTable(txn, TRANSACTIONS_TABLE);
      if (!tables.includes(ACCOUNTS_TABLE))
        await createTable(txn, ACCOUNTS_TABLE);
    });
    return {
      statusCode: 200,
      body: JSON.stringify({ message: "Tables ready!", data: tables }),
    };
  } catch (error) {
    console.error(error);
    return {
      statusCode: 500,
      body: JSON.stringify(error),
    };
  }
};

import {
  QldbDriver,
  Result,
  TransactionExecutor,
} from "amazon-qldb-driver-nodejs";

const qldbDriver: QldbDriver = new QldbDriver(process.env.LEDGER_NAME || "");
const USERS_TABLE: string = process.env.USERS_TABLE || "";
const TRANSACTIONS_TABLE: string = process.env.TRANSACTIONS_TABLE || "";
const ACCOUNTS_TABLE: string = process.env.ACCOUNTS_TABLE || "";

export async function readDocuments(
  txn: TransactionExecutor,
  tableName: string,
  history = false
): Promise<Result> {
  const statement: string = history
    ? `SELECT * FROM history(${tableName})`
    : `SELECT * FROM ${tableName}`;
  const result: Result = await txn.execute(statement);
  return result;
}

export const handler = async (event) => {
  console.log(JSON.stringify(event));
  const targetTable = event.path.includes(USERS_TABLE.toLocaleLowerCase())
    ? USERS_TABLE
    : event.path.includes(TRANSACTIONS_TABLE.toLocaleLowerCase())
    ? TRANSACTIONS_TABLE
    : event.path.includes(ACCOUNTS_TABLE.toLocaleLowerCase())
    ? ACCOUNTS_TABLE
    : "";

  try {
    const results = await qldbDriver.executeLambda(
      async (txn: TransactionExecutor) => {
        return (
          await readDocuments(txn, targetTable, event.path.includes("history"))
        ).getResultList();
      }
    );
    return {
      statusCode: 200,
      body: JSON.stringify({
        message: "Data ready!",
        data: results,
      }),
    };
  } catch (error) {
    console.error(error);
    return {
      statusCode: 500,
      body: JSON.stringify(error),
    };
  }
};

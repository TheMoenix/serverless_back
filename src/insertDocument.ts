import {
  QldbDriver,
  Result,
  TransactionExecutor,
} from "amazon-qldb-driver-nodejs";

const qldbDriver: QldbDriver = new QldbDriver(process.env.LEDGER_NAME || "");
const USERS_TABLE: string = process.env.USERS_TABLE || "";
const TRANSACTIONS_TABLE: string = process.env.TRANSACTIONS_TABLE || "";
const ACCOUNTS_TABLE: string = process.env.ACCOUNTS_TABLE || "";

export async function insertDocument(
  txn: TransactionExecutor,
  tableName: string,
  documents: object
): Promise<Result> {
  const statement: string = `INSERT INTO ${tableName} ?`;
  const result: Result = await txn.execute(statement, documents);
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
          await insertDocument(txn, targetTable, JSON.parse(event.body))
        ).getResultList();
      }
    );
    return {
      statusCode: 200,
      body: JSON.stringify({
        message: "Data inserted!",
        data: {
          documentIds: results.map((rec) =>
            rec.get("documentId")?.stringValue()
          ),
        },
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

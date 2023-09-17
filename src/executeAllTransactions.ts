import {
  QldbDriver,
  Result,
  TransactionExecutor,
} from "amazon-qldb-driver-nodejs";
import { Lambda } from "aws-sdk";

const lambda = new Lambda();
const qldbDriver: QldbDriver = new QldbDriver(process.env.LEDGER_NAME || "");
const TRANSACTIONS_TABLE: string = process.env.TRANSACTIONS_TABLE || "";
const ACCOUNTS_TABLE: string = process.env.ACCOUNTS_TABLE || "";

export async function readDocuments(
  txn: TransactionExecutor,
  tableName: string
): Promise<Result> {
  const statement: string = `SELECT * FROM ${tableName}`;
  const result: Result = await txn.execute(statement);
  return result;
}

export const handler = async (event) => {
  console.log(JSON.stringify(event));
  const accounts = JSON.parse(
    JSON.parse(
      (
        await lambda
          .invoke({
            FunctionName: "serverless-alshami-dev-getDocument",
            InvocationType: "RequestResponse",
            LogType: "None",
            Payload: JSON.stringify({ path: "accounts" }),
          })
          .promise()
      ).Payload?.toString() || ""
    ).body
  ).data;

  const transactions = JSON.parse(
    JSON.parse(
      (
        await lambda
          .invoke({
            FunctionName: "serverless-alshami-dev-getDocument",
            InvocationType: "RequestResponse",
            LogType: "None",
            Payload: JSON.stringify({ path: "transactions" }),
          })
          .promise()
      ).Payload?.toString() || ""
    ).body
  ).data.filter((rec) => !rec.status || rec.status !== "Done");

  try {
    const results = await qldbDriver.executeLambda(
      async (txn: TransactionExecutor) => {
        for (let i = 0; i < transactions.length; i++) {
          const transaction = transactions[i];
          const sourceAccount = accounts.find(
            (rec) => rec.id === transaction.sourceAccount
          );
          const destinationAccount = accounts.find(
            (rec) => rec.id === transaction.destinationAccount
          );
          await txn.execute(
            `UPDATE ${ACCOUNTS_TABLE} SET balance = ${
              sourceAccount.balance - transaction.amount
            } WHERE id = ${sourceAccount.id}`
          );
          sourceAccount["balance"] = sourceAccount.balance - transaction.amount;
          if (destinationAccount) {
            await txn.execute(
              `UPDATE ${ACCOUNTS_TABLE} SET balance = ${
                destinationAccount.balance + transaction.amount
              } WHERE id = ${destinationAccount.id}`
            );
            destinationAccount["balance"] =
              destinationAccount.balance + transaction.amount;
          }
        }
        return (
          await txn.execute(`UPDATE ${TRANSACTIONS_TABLE} SET status = 'Done'`)
        ).getResultList();
      }
    );
    return {
      statusCode: 200,
      body: JSON.stringify({
        message: "Transactions Executed!",
        data: { transactions: results },
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

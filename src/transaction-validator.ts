export type Transaction = Readonly<{
  amount: number;
  currency: string;
  account: string;
  date: string;
}>;

export type AccountCurrencyTotal = Readonly<{
  account: string;
  currency: string;
  total: number;
}>;

export type TransactionField =
  | "input"
  | "transaction"
  | "amount"
  | "currency"
  | "account"
  | "date";

export type TransactionErrorCode =
  | "EXPECTED_ARRAY"
  | "EXPECTED_OBJECT"
  | "INVALID_AMOUNT"
  | "INVALID_CURRENCY"
  | "INVALID_ACCOUNT"
  | "INVALID_DATE"
  | "TOTAL_OUT_OF_RANGE";

export type TransactionValidationError = Readonly<{
  index: number | null;
  field: TransactionField;
  code: TransactionErrorCode;
  message: string;
}>;

export type TransactionAggregationResult =
  | Readonly<{
      ok: true;
      transactionCount: number;
      totals: readonly AccountCurrencyTotal[];
    }>
  | Readonly<{
      ok: false;
      errors: readonly TransactionValidationError[];
    }>;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isStrictIsoDate(value: unknown): value is string {
  if (
    typeof value !== "string" ||
    !/^(?!0000)\d{4}-\d{2}-\d{2}$/.test(value)
  ) {
    return false;
  }

  const timestamp = Date.parse(`${value}T00:00:00.000Z`);
  return (
    Number.isFinite(timestamp) &&
    new Date(timestamp).toISOString().slice(0, 10) === value
  );
}

function validateTransaction(
  value: unknown,
  index: number,
  errors: TransactionValidationError[],
): Transaction | undefined {
  if (!isRecord(value)) {
    errors.push({
      index,
      field: "transaction",
      code: "EXPECTED_OBJECT",
      message: "Transaction must be a non-null object.",
    });
    return undefined;
  }

  let amount: number | undefined;
  if (typeof value.amount === "number" && Number.isFinite(value.amount)) {
    amount = value.amount;
  } else {
    errors.push({
      index,
      field: "amount",
      code: "INVALID_AMOUNT",
      message: "Amount must be a finite number.",
    });
  }

  let currency: string | undefined;
  if (
    typeof value.currency === "string" &&
    /^[A-Z]{3}$/.test(value.currency)
  ) {
    currency = value.currency;
  } else {
    errors.push({
      index,
      field: "currency",
      code: "INVALID_CURRENCY",
      message: "Currency must be a three-letter uppercase code.",
    });
  }

  let account: string | undefined;
  if (typeof value.account === "string" && value.account.trim().length > 0) {
    account = value.account.trim();
  } else {
    errors.push({
      index,
      field: "account",
      code: "INVALID_ACCOUNT",
      message: "Account must be a non-blank string.",
    });
  }

  let date: string | undefined;
  if (isStrictIsoDate(value.date)) {
    date = value.date;
  } else {
    errors.push({
      index,
      field: "date",
      code: "INVALID_DATE",
      message: "Date must be a real calendar date in YYYY-MM-DD format.",
    });
  }

  if (
    amount === undefined ||
    currency === undefined ||
    account === undefined ||
    date === undefined
  ) {
    return undefined;
  }

  return { amount, currency, account, date };
}

function compareText(left: string, right: string): number {
  if (left < right) {
    return -1;
  }
  if (left > right) {
    return 1;
  }
  return 0;
}

/**
 * Validates and aggregates transactions atomically: invalid input returns errors
 * and no partial totals.
 *
 * Complexity: O(n + g log g) time and O(g + e) space, where n is the number
 * of transactions, g the number of account/currency groups, and e the errors.
 */
export function groupTransactionTotals(
  input: unknown,
): TransactionAggregationResult {
  if (!Array.isArray(input)) {
    return {
      ok: false,
      errors: [
        {
          index: null,
          field: "input",
          code: "EXPECTED_ARRAY",
          message: "Input must be an array of transactions.",
        },
      ],
    };
  }

  const values: readonly unknown[] = input;
  const errors: TransactionValidationError[] = [];
  const transactions: Transaction[] = [];

  values.forEach((value, index) => {
    const transaction = validateTransaction(value, index, errors);
    if (transaction !== undefined) {
      transactions.push(transaction);
    }
  });

  if (errors.length > 0) {
    return { ok: false, errors };
  }

  const totalsByAccount = new Map<string, Map<string, number>>();

  for (const [index, transaction] of transactions.entries()) {
    let totalsByCurrency = totalsByAccount.get(transaction.account);
    if (totalsByCurrency === undefined) {
      totalsByCurrency = new Map<string, number>();
      totalsByAccount.set(transaction.account, totalsByCurrency);
    }

    const currentTotal = totalsByCurrency.get(transaction.currency) ?? 0;
    const nextTotal = currentTotal + transaction.amount;
    if (!Number.isFinite(nextTotal)) {
      return {
        ok: false,
        errors: [
          {
            index,
            field: "amount",
            code: "TOTAL_OUT_OF_RANGE",
            message: "The account/currency total is outside the finite number range.",
          },
        ],
      };
    }
    totalsByCurrency.set(
      transaction.currency,
      Object.is(nextTotal, -0) ? 0 : nextTotal,
    );
  }

  const totals: AccountCurrencyTotal[] = [];
  for (const [account, totalsByCurrency] of totalsByAccount) {
    for (const [currency, total] of totalsByCurrency) {
      totals.push({ account, currency, total });
    }
  }

  totals.sort(
    (left, right) =>
      compareText(left.account, right.account) ||
      compareText(left.currency, right.currency),
  );

  return {
    ok: true,
    transactionCount: transactions.length,
    totals,
  };
}

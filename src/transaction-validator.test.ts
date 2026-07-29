import {
  deepStrictEqual,
  equal,
  fail,
} from "node:assert/strict";
import { test } from "node:test";

import { groupTransactionTotals } from "./transaction-validator.js";

test("groups totals and sorts them by account, then currency", () => {
  const result = groupTransactionTotals([
    { amount: 7, currency: "USD", account: "beta", date: "2026-01-03" },
    { amount: 3, currency: "EUR", account: "alpha", date: "2026-01-02" },
    { amount: -2, currency: "USD", account: "alpha", date: "2026-01-01" },
    { amount: 5, currency: "EUR", account: "alpha", date: "2026-01-04" },
  ]);

  if (!result.ok) {
    fail("Expected valid transactions.");
  }
  equal(result.transactionCount, 4);
  deepStrictEqual(result.totals, [
    { account: "alpha", currency: "EUR", total: 8 },
    { account: "alpha", currency: "USD", total: -2 },
    { account: "beta", currency: "USD", total: 7 },
  ]);
});

test("returns deterministic output for different valid input orders", () => {
  const first = groupTransactionTotals([
    { amount: 4, currency: "USD", account: "z", date: "2026-01-01" },
    { amount: 2, currency: "EUR", account: "a", date: "2026-01-01" },
  ]);
  const second = groupTransactionTotals([
    { amount: 2, currency: "EUR", account: "a", date: "2026-01-01" },
    { amount: 4, currency: "USD", account: "z", date: "2026-01-01" },
  ]);

  deepStrictEqual(first, second);
});

test("rejects a non-array input", () => {
  const result = groupTransactionTotals({ transactions: [] });

  if (result.ok) {
    fail("Expected invalid input.");
  }
  deepStrictEqual(result.errors, [
    {
      index: null,
      field: "input",
      code: "EXPECTED_ARRAY",
      message: "Input must be an array of transactions.",
    },
  ]);
});

test("rejects non-finite and non-numeric amounts", () => {
  const result = groupTransactionTotals([
    { amount: Number.NaN, currency: "USD", account: "a", date: "2026-01-01" },
    { amount: "10", currency: "USD", account: "a", date: "2026-01-01" },
  ]);

  if (result.ok) {
    fail("Expected invalid amounts.");
  }
  deepStrictEqual(
    result.errors.map(({ index, field, code }) => ({ index, field, code })),
    [
      { index: 0, field: "amount", code: "INVALID_AMOUNT" },
      { index: 1, field: "amount", code: "INVALID_AMOUNT" },
    ],
  );
});

test("rejects malformed currency codes", () => {
  const result = groupTransactionTotals([
    { amount: 10, currency: "usd", account: "a", date: "2026-01-01" },
    { amount: 10, currency: "US", account: "a", date: "2026-01-01" },
  ]);

  if (result.ok) {
    fail("Expected invalid currencies.");
  }
  deepStrictEqual(
    result.errors.map(({ index, field }) => ({ index, field })),
    [
      { index: 0, field: "currency" },
      { index: 1, field: "currency" },
    ],
  );
});

test("rejects blank and non-string accounts", () => {
  const result = groupTransactionTotals([
    { amount: 10, currency: "USD", account: "  ", date: "2026-01-01" },
    { amount: 10, currency: "USD", account: 42, date: "2026-01-01" },
  ]);

  if (result.ok) {
    fail("Expected invalid accounts.");
  }
  deepStrictEqual(
    result.errors.map(({ index, field }) => ({ index, field })),
    [
      { index: 0, field: "account" },
      { index: 1, field: "account" },
    ],
  );
});

test("rejects impossible dates and non-ISO date formats", () => {
  const result = groupTransactionTotals([
    { amount: 10, currency: "USD", account: "a", date: "2026-02-29" },
    { amount: 10, currency: "USD", account: "a", date: "01/02/2026" },
  ]);

  if (result.ok) {
    fail("Expected invalid dates.");
  }
  deepStrictEqual(
    result.errors.map(({ index, field }) => ({ index, field })),
    [
      { index: 0, field: "date" },
      { index: 1, field: "date" },
    ],
  );
});

test("accepts a leap-day boundary", () => {
  const result = groupTransactionTotals([
    { amount: 0, currency: "USD", account: "a", date: "2024-02-29" },
  ]);

  if (!result.ok) {
    fail("Expected leap day and a zero amount to be valid.");
  }
  deepStrictEqual(result.totals, [
    { account: "a", currency: "USD", total: 0 },
  ]);
});

test("counts duplicate transactions independently", () => {
  const duplicate = {
    amount: 25,
    currency: "PHP",
    account: "cash",
    date: "2026-07-29",
  };
  const result = groupTransactionTotals([duplicate, duplicate]);

  if (!result.ok) {
    fail("Expected duplicate transactions to be valid.");
  }
  equal(result.transactionCount, 2);
  deepStrictEqual(result.totals, [
    { account: "cash", currency: "PHP", total: 50 },
  ]);
});

test("reports every invalid field in stable field order", () => {
  const result = groupTransactionTotals([null, {}]);

  if (result.ok) {
    fail("Expected invalid transactions.");
  }
  deepStrictEqual(
    result.errors.map(({ index, field }) => ({ index, field })),
    [
      { index: 0, field: "transaction" },
      { index: 1, field: "amount" },
      { index: 1, field: "currency" },
      { index: 1, field: "account" },
      { index: 1, field: "date" },
    ],
  );
});

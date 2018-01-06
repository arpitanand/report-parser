const mysql = require('mysql');
const csvWriter = require('csv-write-stream');
const fs = require('fs');

const options = {
  separator: ',',
  newline: '\n',
  headers: [
    'Client id',
    'Client name',
    '2014 - Bank overdraft / cash credit account',
    '2014 - Sundry debtors',
    '2014 - Inventory',
    '2014 - Cost of sales / revenue & Purchases',
    '2014 - Net sales revenue',
    '2015 - Bank overdraft / cash credit account',
    '2015 - Sundry debtors',
    '2015 - Inventory',
    '2015 - Cost of sales / revenue & Purchases',
    '2015 - Net sales revenue',
    '2016 - Bank overdraft / cash credit account',
    '2016 - Sundry debtors',
    '2016 - Inventory',
    '2016 - Cost of sales / revenue & Purchases',
    '2016 - Net sales revenue',
  ],
  sendHeaders: true,
};

const query = 'SELECT c.name, c.clientId, f.bankOverDraftCashCreditAc, f.financialYear, f.sDebtors, f.sCreditors, f.inventory, f.costOfSalesRevenueAndPurchases, f.netSalesRevenue from client c, financialMetrics f where c.clientId = f.clientId AND c.status = "VERIFIED"';

const writer = csvWriter(options);
writer.pipe(fs.createWriteStream('financial_metrics.csv'));

const mysqlClient = mysql.createConnection({
  host: 'localhost',
  user: 'root',
  password: '',
  database: 'oxyzo',
});

const generate = (data) => {
  Object.keys(data).forEach((clientId) => {
    const row = [
      clientId,
      data[clientId].name,
    ];

    ['1396310400000', '1427846400000', '1459468800000'].forEach((year) => {
      row.push(data[clientId][year].bankOverDraftCashCreditAc);
      row.push(data[clientId][year].sDebtors);
      row.push(data[clientId][year].inventory);
      row.push(data[clientId][year].costOfSalesRevenueAndPurchases);
      row.push(data[clientId][year].netSalesRevenue);
    });
    writer.write(row);
  });
};

mysqlClient.query(query, (error, results) => {
  if (error) { console.error('error', error); }
  const data = {};
  results.forEach((row) => {
    if (data[row.clientId] === undefined) {
      data[row.clientId] = { name: row.name };
    }
    data[row.clientId][row.financialYear] = {
      bankOverDraftCashCreditAc: row.bankOverDraftCashCreditAc,
      financialYear: row.financialYear,
      sDebtors: row.sDebtors,
      sCreditors: row.sCreditors,
      inventory: row.inventory,
      costOfSalesRevenueAndPurchases: row.costOfSalesRevenueAndPurchases,
      netSalesRevenue: row.netSalesRevenue,
    };
  });
  generate(data);
  mysqlClient.end();
});

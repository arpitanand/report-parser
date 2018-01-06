const mysql = require('mysql');
const csvWriter = require('csv-write-stream');
const fs = require('fs');
const async = require('async');
const dateFormat = require('dateformat');

const DATE_FORMAT = 'dd/mm/yyyy - hh:MM TT';
const options = {
  separator: ',',
  newline: '\n',
  headers: [
    'Client Id',
    'Client Name',
    'Client Pan',
    'Client Status',
    'Loan Application Id',
    'Loan Application Creation Date',
    'Loan Application Status',
    'Loan Request Id',
    'Loan Request Status',
    'Loan Request Creation Date',
    'Loan Request Offered Date',
    'Loan Request NBFC',
    'Loan Request Offered Amount',
    'Loan Request Interest rate',
    'Loan Request Repayment Cycle',
    'Loan Request Processing Charge',
  ],
  sendHeaders: true,
};


const query = 'SELECT c.clientId, c.name, c.pan, c.status as clientStatus, la.dateCreated as loanApplicationDateCreated, la.applicationId, la.loanApplicationStatus, lr.dateCreated, lr.status, lr.loanRequestId, n.name as nbfcName, lr.offerLoanAmount, lr.interestRate, lr.repaymentCycle, lr.processingCharge FROM client c, loanApplication la, loanRequest lr, nbfc n WHERE c.clientId = la.clientId AND la.applicationId = lr.loanApplicationId AND lr.nbfcId = n.nbfcId';

const writer = csvWriter(options);
writer.pipe(fs.createWriteStream('report_loan_requests.csv'));

const mysqlClient = mysql.createConnection({
  host: 'localhost',
  user: 'root',
  password: '',
  database: 'oxyzo',
});

const generate = (data) => {
  const rows = [];
  async.eachSeries(data, (lr, lrCallback) => {
    mysqlClient.query(`SELECT dateCreated FROM statusTransitionComment WHERE statusEntityType = "LOAN_REQUEST" AND toStatus = "OFFERED" AND entityId = "${lr.loanRequestId}"`, (error, results) => {
      if (error) { console.error('error', error); }
      rows.push([
        lr.clientId,
        lr.name,
        lr.pan,
        lr.clientStatus,
        lr.applicationId,
        dateFormat(new Date(lr.loanApplicationDateCreated), DATE_FORMAT),
        lr.loanApplicationStatus,
        lr.loanRequestId,
        lr.status,
        dateFormat(new Date(lr.dateCreated), DATE_FORMAT),
        results.length !== 0 ? dateFormat(new Date(results[0].dateCreated), DATE_FORMAT) : '',
        lr.nbfcName,
        lr.offerLoanAmount,
        lr.interestRate,
        lr.repaymentCycle,
        lr.processingCharge,
      ]);
      lrCallback(null, null);
    });
  }, () => {
    rows.forEach((row) => {
      writer.write(row);
    });
    mysqlClient.end();
    writer.end();
  });
};

mysqlClient.query(query, (error, results) => {
  if (error) { console.error('error', error); }
  generate(results);
});

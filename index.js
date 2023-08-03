const express = require('express');
const fs = require('fs');
const csvParser = require('csv-parser');

const app = express();
const port = 3000;

function calculateDailyAggregation(file) {
  const dailyAggregation = {};

  fs.createReadStream(file)
    .pipe(csvParser({ separator: ';' }))
    .on('data', (row) => {
      const date = row.dateTime.split('T')[0];
      const value = parseFloat(row.value);

      if (!isNaN(value)) {
        if (dailyAggregation[date]) {
          dailyAggregation[date].sum += value;
          dailyAggregation[date].count++;
        } else {
          dailyAggregation[date] = { sum: value, count: 1 };
        }
      }
    })
    .on('end', () => {
      for (const date in dailyAggregation) {
        dailyAggregation[date].average = dailyAggregation[date].sum / dailyAggregation[date].count;
        delete dailyAggregation[date].sum;
        delete dailyAggregation[date].count;
      }
      console.log("Daily Aggregation:");
      console.log(dailyAggregation);
    });
}

app.get('/calculate-aggregation', (req, res) => {
  const file = 'METRICS_REPORT-1673286660394 (3).csv';
  calculateDailyAggregation(file);

  res.send("Daily Aggregation calculated and logged in the console.");
});

app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});

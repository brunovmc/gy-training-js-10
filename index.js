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
      const dateString = row.dateTime.split(' ')[0];
      const dateParts = dateString.split('/');

      const isoDateString = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]}`;

      const value = Number(row.value.replace(',', '.'));

      if (!isNaN(value)) {
        if (dailyAggregation[isoDateString]) {
          dailyAggregation[isoDateString].sum += value;
          dailyAggregation[isoDateString].count++;
        } else {
          dailyAggregation[isoDateString] = { sum: value, count: 1 };
        }
      }
    })
    .on('end', () => {
      console.log("Daily Aggregation:");
      for (const date in dailyAggregation) {
        dailyAggregation[date].average = dailyAggregation[date].sum / dailyAggregation[date].count;
        console.log(`${date} - Average Value: ${dailyAggregation[date].average.toFixed(2)}`);
      }
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

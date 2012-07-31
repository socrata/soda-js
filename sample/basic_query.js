var soda = require('soda-js');

var consumer = new soda.Consumer('explore.data.gov');

consumer.query()
  .withDataset('644b-gaut')
  .limit(5)
  .where({ namelast: 'SMITH' })
  .order('namelast')
  .getRows()
    .on('success', function(rows) { console.log(rows); })
    .on('error', function(error) { console.error(error); });


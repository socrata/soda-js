var soda = require('../lib/soda-js');

var consumer = new soda.Consumer('open.whitehouse.gov');

consumer.query()
  .withDataset('p86s-ychb')
  .limit(5)
  .where({ namelast: 'SMITH' })
  .order('namelast')
  .getRows()
    .on('success', function(rows) { console.log(rows); })
    .on('error', function(error) { console.error(error); });


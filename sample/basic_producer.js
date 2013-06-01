var soda = require('../lib/soda-js');

var sodaOpts = {
        "username": "testuser@gmail.com",
        "password" : "OpenData",
        "apiToken" : "D8Atrg62F2j017ZTdkMpuZ9vY"
}
var producer = new soda.Producer('sandbox.demo.socrata.com', sodaOpts);


var addSample = function() {
  var data = {
    mynum : 42,
    mytext: "hello world",
    mymoney: 999.99
  }

  console.log("Adding Sample")
  producer.operation()
    .withDataset('rphc-ayt9')
    .add(data)
      .on('success', function(row) { console.log(row); updateSample(row[':id']); })
      .on('error', function(error) { console.error(error); })
}

var updateSample = function(id) {
  var data = { mytext: "goodbye world" }

  console.log("\nUpdating Sample")
  producer.operation()
    .withDataset('rphc-ayt9')
    .update(id, data)
      .on('success', function(row) { console.log(row); deleteSample(row[':id']); })
      .on('error', function(error) { console.error(error); })
}


var deleteSample = function(id) {
  console.log("\nDeleting Sample")
  producer.operation()
    .withDataset('rphc-ayt9')
    .delete(id)
      .on('success', function(row) { console.log(row); })
      .on('error', function(error) { console.error(error); })
}



addSample();
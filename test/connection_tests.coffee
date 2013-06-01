
soda = require('../lib/soda-js')

module.exports =
  
  'basic construction': (beforeExit, assert) ->
    connection = new soda._internal.Connection('opendata.socrata.com')
    assert.eql(connection.dataSite, 'opendata.socrata.com')

  'failed construction': (beforeExit, assert) ->
    caught = false
    try
      connection = new soda._internal.Connection('http://data.cityofchicago.org')
    catch ex
      caught = true
    assert.ok(caught)
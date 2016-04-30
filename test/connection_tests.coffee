
soda = require('../lib/soda-js')

module.exports =
  
  'basic construction (w/o proxy)': (beforeExit, assert) ->
    connection = new soda._internal.Connection('opendata.socrata.com')
    assert.eql(connection.dataSite, 'opendata.socrata.com')

  'basic construction (w/ construction)': (beforeExit, assert) ->
    connection = new soda._internal.Connection('opendata.socrata.com',{sodaProxy:'secure-socrata-proxy.herokuapp.com'})
    assert.eql(connection.dataSite, 'opendata.socrata.com')
    assert.eql(connection.sodaOpts.sodaProxy, 'secure-socrata-proxy.herokuapp.com')

  'failed construction': (beforeExit, assert) ->
    caught = false
    try
      connection = new soda._internal.Connection('http://data.cityofchicago.org')
    catch ex
      caught = true
    assert.ok(caught)
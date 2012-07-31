
soda = require('../lib/soda-js')

module.exports =
  
  'basic construction': (beforeExit, assert) ->
    consumer = new soda.Consumer('opendata.socrata.com')
    assert.eql(consumer.dataSite, 'opendata.socrata.com')

  'failed construction': (beforeExit, assert) ->
    caught = false
    try
      consumer = new soda.Consumer('http://data.cityofchicago.org')
    catch ex
      caught = true
    assert.ok(caught)

  'create query': (beforeExit, assert) ->
    consumer = new soda.Consumer('data.seattle.gov')
    query = consumer.query()
    assert.ok(query instanceof soda._internal.Query)
    assert.eql(query.consumer, consumer)


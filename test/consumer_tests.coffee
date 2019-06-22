
soda = require('../lib/soda-js')

module.exports =
  'create consumer connection': (beforeExit, assert) ->
    consumer = new soda.Producer('data.seattle.gov')
    assert.ok(consumer.connection instanceof soda._internal.Connection)
    assert.eql(consumer.connection.dataSite, 'data.seattle.gov')

  'create query': (beforeExit, assert) ->
    consumer = new soda.Consumer('data.seattle.gov')
    query = consumer.query()
    assert.ok(query instanceof soda._internal.Query)
    assert.eql(query.consumer, consumer)

  'create proxied query': (beforeExit, assert) ->
    consumer = new soda.Consumer('soda.demo.socrata.com',{sodaProxy:'a-test-proxy.com/socrata'})
    query = consumer.query()
    assert.ok(query instanceof soda._internal.Query)
    assert.eql(query.consumer, consumer)
    query
      .withDataset('a1b2-c3d4')
      .limit(10)
    assert.eql(query.getURL(),'https://a-test-proxy.com/socrata/soda.demo.socrata.com/resource/a1b2-c3d4.json?%24limit=10')

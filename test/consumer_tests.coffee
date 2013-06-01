
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


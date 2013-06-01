
soda = require('../lib/soda-js')

module.exports =
  'create producer connection': (beforeExit, assert) ->
    producer = new soda.Producer('data.seattle.gov')
    assert.ok(producer.connection instanceof soda._internal.Connection)
    assert.eql(producer.connection.dataSite, 'data.seattle.gov')

  'create producer operation': (beforeExit, assert) ->
    producer = new soda.Producer('data.seattle.gov')
    operation = producer.operation()
    assert.ok(operation instanceof soda._internal.Operation)
    assert.eql(operation.producer, producer)

soda = require('../lib/soda-js')

# fixture generator to allow injecting verifiers as networkers
producer = (verifier) ->
  connection:
    networker: ((opts, data) -> verifier(opts, data); -> null),
    emitterOpts:
      wildcard: true,
      delimiter: '.',
      maxListeners: 15


# convenience func for using the above fixture with an operation
operateWith = (networker) -> new soda._internal.Operation(producer(networker))

module.exports =
  'basic add': (beforeExit, assert) ->
    verifier = (opts, data) -> 
        assert.eql(opts.method, "post")
        assert.eql(opts.path, "/resource/abcd-1234")
        assert.eql(data, { hello: "world" })

    operateWith(verifier)
      .withDataset('abcd-1234')
      .add( {hello: "world" })

  'multiple add': (beforeExit, assert) ->
    verifier = (opts, data) -> 
        assert.eql(opts.method, "post")
        assert.eql(opts.path, "/resource/abcd-1234")
        assert.eql(data.length, 3)

    operateWith(verifier)
      .withDataset('abcd-1234')
      .add( [{col:"a"}, {col:"b"}, {col:"c"}] )

  'add prevents upsert of object': (beforeExit, assert) ->
    verifier = (opts, data) -> 
        assert.eql(opts.method, "post")
        assert.eql(opts.path, "/resource/abcd-1234")
        assert.eql(data, { col: "c" })

    operateWith(verifier)
      .withDataset('abcd-1234')
      .add( { ":id": 3, ":delete": true, col:"c"} )

  'add prevents upsert of array': (beforeExit, assert) ->
    verifier = (opts, data) -> 
        assert.eql(opts.method, "post")
        assert.eql(opts.path, "/resource/abcd-1234")
        assert.eql(data, [
          {col:"a"}, 
          {col:"b"}, 
          {col:"c"}, 
          {col:"d"}
        ])

    operateWith(verifier)
      .withDataset('abcd-1234')
      .add([
        { ":id": 1, col: "a"},
        { col: "b", ":delete": true },
        { ":id": 3, ":delete": true, col:"c"},
        { col: "d" }
      ])


  'upsert array': (beforeExit, assert) ->
    verifier = (opts, data) -> 
        assert.eql(opts.method, "post")
        assert.eql(opts.path, "/resource/abcd-1234")
        assert.eql(data, [
          { col: "a", ":id": 1},
          { col: "b", ":delete": true },
          { col: "c", ":id": 3, ":delete": true },
          { col: "d" }
        ])

    operateWith(verifier)
      .withDataset('abcd-1234')
      .upsert([
        { ":id": 1, col: "a"},
        { col: "b", ":delete": true },
        { ":id": 3, ":delete": true, col:"c"},
        { col: "d" }
      ])      

  'basic truncate': (beforeExit, assert) ->
    verifier = (opts, data) ->
      assert.eql(opts.method, "delete")
      assert.eql(opts.path, "/resource/lmno-9876")
      assert.isUndefined(data)

    operateWith(verifier)
      .withDataset('lmno-9876')
      .truncate()

  'basic delete': (beforeExit, assert) ->
    verifier = (opts, data) ->
      assert.eql(opts.method, "delete")
      assert.eql(opts.path, "/resource/lmno-9876/123")
      assert.isUndefined(data)

    operateWith(verifier)
      .withDataset('lmno-9876')
      .delete(123)

  'basic update': (beforeExit, assert) ->
    verifier = (opts, data) ->
      assert.eql(opts.method, "post") # seriously, why isn't this patch?
      assert.eql(opts.path, "/resource/lmno-9876/123")
      assert.eql(data, { num : 987 })

    operateWith(verifier)
      .withDataset("lmno-9876")
      .update(123, { num : 987 })

  'basic replace': (beforeExit, assert) ->
    verifier = (opts, data) ->
      assert.eql(opts.method, "put")
      assert.eql(opts.path, "/resource/lmno-1234/987")
      assert.eql(data, { num : 456 })

    operateWith(verifier)
      .withDataset("lmno-1234")
      .replace(987, { num : 456 })

  

soda = require('../lib/soda-js')

# fixture generator to allow injecting verifiers as networkers
consumer = (verifier) ->
  connection:
    networker: ((opts) -> verifier(opts); -> null),
    emitterOpts:
      wildcard: true,
      delimiter: '.',
      maxListeners: 15

# convenience func for using the above fixture with a query
queryWith = (networker) -> new soda._internal.Query(consumer(networker))

module.exports =

  'basic query': (beforeExit, assert) ->
    verifier = (opts) ->
      assert.eql(opts.path, '/resource/hospitals.json')

    queryWith(verifier)
      .withDataset('hospitals')
      .getRows()

  'col select': (beforeExit, assert) ->
    verifier = (opts) ->
      assert.eql(opts.query, { $select: 'street, city, state, zip' })

    queryWith(verifier)
      .withDataset('hospitals')
      .select('street', 'city', 'state', 'zip')
      .getRows()

  'string expr': (beforeExit, assert) ->
    expr = soda.expr.eq('name', 'bob')
    assert.eql(expr, "name = 'bob'")

  'number expr': (beforeExit, assert) ->
    expr = soda.expr.eq('age', 15)
    assert.eql(expr, "age = 15")

  'compound expr': (beforeExit, assert) ->
    expr = soda.expr.and(soda.expr.eq('columnone', 1), soda.expr.eq('columntwo', 2))
    assert.eql(expr, '(columnone = 1) and (columntwo = 2)')

  'string where': (beforeExit, assert) ->
    verifier = (opts) ->
      assert.eql(opts.query, { $where: '(salary > 35000) and (yearsWorked >= 5)' })

    queryWith(verifier)
      .withDataset('salaries')
      .where('salary > 35000', 'yearsWorked >= 5')
      .getRows()

  'obj where': (beforeExit, assert) ->
    verifier = (opts) ->
      assert.eql(opts.query, { $where: "(firstname = 'abed') and (lastname = 'nadir')" })

    queryWith(verifier)
      .withDataset('people')
      .where({ firstname: 'abed', lastname: 'nadir' })
      .getRows()

  'groupby': (beforeExit, assert) ->
    verifier = (opts) ->
      assert.eql(opts.query, { $group: 'department, type' })

    queryWith(verifier)
      .withDataset('payments')
      .group('department', 'type')
      .getRows()

  'having': (beforeExit, assert) ->
    # having is the same implementation as where, so just test a maxiquery
    verifier = (opts) ->
      assert.eql(opts.query,
        $group: 'firstname, lastname, total_salary',
        $having: "((firstname = 'jeff') or (lastname = 'winger')) and (total_salary > 100000)")

    queryWith(verifier)
      .withDataset('salaries')
      .group('firstname', 'lastname', 'total_salary') # of course in reality total_salary would be an aggr func..
      .having(soda.expr.or(soda.expr.eq('firstname', 'jeff'), soda.expr.eq('lastname', 'winger')), 'total_salary > 100000')
      .getRows()

  'orderby': (beforeExit, assert) ->
    verifier = (opts) ->
      assert.eql(opts.query, { $order: 'lastname asc, firstname desc' })

    queryWith(verifier)
      .withDataset('salaries')
      .order('lastname', 'firstname desc')
      .getRows()

  'offset and limit': (beforeExit, assert) ->
    verifier = (opts) ->
      assert.eql(opts.query, { $offset: 5, $limit: 10 })

    queryWith(verifier)
      .withDataset('hospitals')
      .offset(5)
      .limit(10)
      .getRows()


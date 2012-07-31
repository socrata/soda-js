# soda.coffee -- chained, evented, buzzworded library for accessing SODA via JS.

# sodaOpts options:
#   username: https basic auth username
#   password: https basic auth password
#   apiToken: socrata api token
#
#   emitterOpts: options to override EventEmitter2 declaration options

#  TODO:
#    * we're inconsistent about validating query correctness. do we continue with catch-what-we-can,
#      or do we just back off and leave all failures to the api to return?

EventEmitter = require('eventemitter2').EventEmitter2
httpClient = require('superagent')

# internal util funcs
isString = (obj) -> toString.call(obj) == '[object String]'
isArray = (obj) -> toString.call(obj) == '[object Array]'
isNumber = (obj) -> toString.call(obj) == '[object Number]'
extend = (target, sources...) -> (target[k] = v for k, v of source) for source in sources; null

# it's really, really, really stupid that i have to solve this problem here
toBase64 =
  if Buffer?
    (str) -> new Buffer(str).toString('base64')
  else
    # adapted/modified from https://github.com/rwz/base64.coffee
    base64Lookup = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/='.split('')
    rawToBase64 = btoa ? (str) ->
      result = []
      i = 0
      while i < str.length
        chr1 = str.charCodeAt(i++)
        chr2 = str.charCodeAt(i++)
        chr3 = str.charCodeAt(i++)
        throw new Error('Invalid character!') if Math.max(chr1, chr2, chr3) > 0xFF

        enc1 = chr1 >> 2
        enc2 = ((chr1 & 3) << 4) | (chr2 >> 4)
        enc3 = ((chr2 & 15) << 2) | (chr3 >> 6)
        enc4 = chr3 & 63

        if isNaN(chr2)
          enc3 = enc4 = 64
        else if isNaN(chr3)
          enc4 = 64

        result.push(base64Lookup[enc1])
        result.push(base64Lookup[enc2])
        result.push(base64Lookup[enc3])
        result.push(base64Lookup[enc4])
      result.join('')
    (str) -> rawToBase64(unescape(encodeURIComponent(str)))

handleLiteral = (literal) ->
  if isString(literal)
    "'#{literal}'"
  else if isNumber(literal)
    # TODO: possibly ensure number cleanliness for sending to the api? sci not?
    literal
  else
    literal

handleOrder = (order) ->
  if /( asc$| desc$)/i.test(order)
    order
  else
    order + ' asc'

addExpr = (target, args) ->
  for arg in args
    if isString(arg)
      target.push(arg)
    else
      target.push("#{k} = #{handleLiteral(v)}") for k, v of arg

# extern util funcs

# convenience functions for building where clauses, if so desired
expr =
  and: (clauses...) -> ("(#{clause})" for clause in clauses).join(' and ')
  or:  (clauses...) -> ("(#{clause})" for clause in clauses).join(' or ')

  gt:  (column, literal) -> "#{column} > #{handleLiteral(literal)}"
  gte: (column, literal) -> "#{column} >= #{handleLiteral(literal)}"
  lt:  (column, literal) -> "#{column} < #{handleLiteral(literal)}"
  lte: (column, literal) -> "#{column} <= #{handleLiteral(literal)}"
  eq:  (column, literal) -> "#{column} = #{handleLiteral(literal)}"

# main class
class Consumer
  constructor: (@dataSite, @sodaOpts = {}) ->
    throw new Error('dataSite does not appear to be valid! Please supply a domain name, eg data.seattle.gov') unless /^([a-z0-9-_]+\.)+([a-z0-9-_]+)$/i.test(@dataSite)

    # options passed directly into EventEmitter2 construction
    @emitterOpts = @sodaOpts.emitterOpts ?
      wildcard: true,
      delimiter: '.',
      maxListeners: 15

    # a function that takes options and returns a superagent (handler) -> void of the resulting request
    @networker = (opts) ->
      url = "https://#{@dataSite}#{opts.path}"

      client = httpClient(opts.method, url)

      client.set('X-App-Token', @sodaOpts.apiToken) if @sodaOpts.apiToken?
      client.set('Authorization', toBase64("#{@sodaOpts.username}:#{@sodaOpts.password}")) if @sodaOpts.username? and @sodaOpts.password?

      client.query(opts.query) if opts.query?

      (responseHandler) -> client.end(responseHandler)

  query: ->
    new Query(this)

  getDataset: (id) ->
    emitter = new EventEmitter(@emitterOpts)
    # TODO: implement me

# querybuilder class
class Query
  constructor: (@consumer) ->
    @_select = []
    @_where = []
    @_group = []
    @_having = []
    @_order = []
    @_offset = @_limit = null

  withDataset: (datasetId) -> @_datasetId = datasetId; this

  # for passing in a fully formed soql query. all other params will be ignored
  soql: (query) -> @_soql = query; this

  select: (selects...) -> @_select.push(select) for select in selects; this

  # args: ('clause', [...])
  #       ({ column: value1, columnb: value2 }, [...]])
  # multiple calls are assumed to be and-chained
  where: (args...) -> addExpr(@_where, args); this
  having: (args...) -> addExpr(@_having, args); this

  group: (groups...) -> @_group.push(group) for group in groups; this

  # args: ("column direction", ["column direction", [...]])
  order: (orders...) -> @_order.push(handleOrder(order)) for order in orders; this

  offset: (offset) -> @_offset = offset; this

  limit: (limit) -> @_limit = limit; this

  getRows: ->
    opts = method: 'get'

    throw new Error('no dataset given to work against!') unless @_datasetId?
    opts.path = "/resource/#{@_datasetId}.json"

    queryComponents = this._buildQueryComponents()
    opts.query = {}
    opts.query['$' + k] = v for k, v of queryComponents

    emitter = new EventEmitter(@consumer.emitterOpts)

    handler = (response) ->
      # TODO: possibly more granular handling?
      if response.ok
        if response.accepted
          # handle 202 by remaking request. inform of possible progress.
          emitter.emit('progress', response.body)
          setTimeout((-> @consumer.networker(opts)(handler)), 5000)
        else
          emitter.emit('success', response.body)
      else
        emitter.emit('error', response.body ? response.text)

      # just emit the raw superagent obj if they just want complete event
      emitter.emit('complete', response)

    @consumer.networker(opts)(handler)

    emitter

  _buildQueryComponents: ->
    query = {}

    if @_soql?
      query.query = @_soql
    else
      query.select = @_select.join(', ') if @_select.length > 0

      query.where = expr.and.apply(this, @_where) if @_where.length > 0

      query.group = @_group.join(', ') if @_group.length > 0

      if @_having.length > 0
        throw new Error('Having provided without group by!') unless @_group.length > 0
        query.having = expr.and.apply(this, @_having)

      query.order = @_order.join(', ') if @_order.length > 0

      query.offset = @_offset if isNumber(@_offset)
      query.limit = @_limit if isNumber(@_limit)

    query

class Dataset
  constructor: (@data, @client) ->
    # TODO: implement me

extend(exports ? this.soda,
  Consumer: Consumer,
  expr: expr,

  # exported for testing reasons
  _internal:
    Query: Query,
    util:
      toBase64: toBase64,
      handleLiteral: handleLiteral,
      handleOrder: handleOrder
)


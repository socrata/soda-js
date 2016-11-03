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

eelib = require('eventemitter2')
EventEmitter = eelib.EventEmitter2 || eelib
httpClient = require('superagent')

# internal util funcs
isString = (obj) -> typeof obj == 'string'
isArray = (obj) -> Array.isArray(obj)
isNumber = (obj) -> !isNaN(parseFloat(obj))
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
  
# serialize object to querystring
toQuerystring = (obj) ->
  str = []
  for own key, val of obj
    str.push encodeURIComponent(key) + '=' + encodeURIComponent(val)
  str.join '&'

class Connection
  constructor: (@dataSite, @sodaOpts = {}) ->
    throw new Error('dataSite does not appear to be valid! Please supply a domain name, eg data.seattle.gov') unless /^[a-z0-9-_.]+(:[0-9]+)?$/i.test(@dataSite)

    # options passed directly into EventEmitter2 construction
    @emitterOpts = @sodaOpts.emitterOpts ?
      wildcard: true,
      delimiter: '.',
      maxListeners: 15

    @networker = (opts, data) ->
      url = "https://#{@dataSite}#{opts.path}"

      client = httpClient(opts.method, url)

      client.set('Accept', "application/json") if data?
      client.set('Content-type', "application/json") if data?
      client.set('X-App-Token', @sodaOpts.apiToken) if @sodaOpts.apiToken?
      client.set('Authorization', "Basic " + toBase64("#{@sodaOpts.username}:#{@sodaOpts.password}")) if @sodaOpts.username? and @sodaOpts.password?
      client.set('Authorization', "OAuth " + accessToken) if @sodaOpts.accessToken?

      client.query(opts.query) if opts.query?
      client.send(data) if data?

      (responseHandler) => client.end(responseHandler || @getDefaultHandler())

  getDefaultHandler: ->
    # instance variable for easy chaining
    @emitter = emitter = new EventEmitter(@emitterOpts)

    # return the handler
    handler = (error, response) ->
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




# main class
class Consumer
  constructor: (@dataSite, @sodaOpts = {}) ->
    @connection = new Connection(@dataSite, @sodaOpts)

  query: ->
    new Query(this)

  getDataset: (id) ->
    emitter = new EventEmitter(@emitterOpts)
    # TODO: implement me

# Producer class
class Producer
  constructor: (@dataSite, @sodaOpts = {}) ->
    @connection = new Connection(@dataSite, @sodaOpts)

  operation: ->
    new Operation(this)

class Operation
  constructor: (@producer) ->

  withDataset: (datasetId) -> @_datasetId = datasetId; this

  # truncate the entire dataset
  truncate: ->
    opts = method: 'delete'
    opts.path = "/resource/#{@_datasetId}"
    this._exec(opts)

  # add a new row - explicitly avoids upserting (updating/deleting existing rows)
  add: (data) ->
    opts = method: 'post'
    opts.path = "/resource/#{@_datasetId}"

    _data = JSON.parse(JSON.stringify(data))
    delete _data[':id']
    delete _data[':delete']
    for obj in _data
      delete obj[':id']
      delete obj[':delete']

    this._exec(opts, _data)

  # modify existing rows
  delete: (id) ->
    opts = method: 'delete'
    opts.path = "/resource/#{@_datasetId}/#{id}"
    this._exec(opts)
  update: (id, data) ->
    opts = method: 'post'
    opts.path = "/resource/#{@_datasetId}/#{id}"
    this._exec(opts, data)
  replace: (id, data) ->
    opts = method: 'put'
    opts.path = "/resource/#{@_datasetId}/#{id}"
    this._exec(opts, data)
  
  # add objects, update if existing, delete if :delete=true
  upsert: (data) ->
    opts = method: 'post'
    opts.path = "/resource/#{@_datasetId}"
    this._exec(opts, data)

  _exec: (opts, data) ->
    throw new Error('no dataset given to work against!') unless @_datasetId?
    @producer.connection.networker(opts, data)()
    @producer.connection.emitter


# querybuilder class
class Query
  constructor: (@consumer) ->
    @_select = []
    @_where = []
    @_group = []
    @_having = []
    @_order = []
    @_offset = @_limit = @_q = null

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
  
  q: (q) -> @_q = q; this

  getOpts: ->
    opts = method: 'get'
    
    throw new Error('no dataset given to work against!') unless @_datasetId?
    opts.path = "/resource/#{@_datasetId}.json"

    queryComponents = this._buildQueryComponents()
    opts.query = {}
    opts.query['$' + k] = v for k, v of queryComponents
    
    opts
    
  getURL: ->
    opts = this.getOpts()
    query = toQuerystring(opts.query)
    
    "https://#{@consumer.dataSite}#{opts.path}" + (if query then "?#{query}" else "")

  getRows: ->
    opts = this.getOpts()

    @consumer.connection.networker(opts)()
    @consumer.connection.emitter

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
      
      query.q = @_q if @_q

    query

class Dataset
  constructor: (@data, @client) ->
    # TODO: implement me

extend(exports ? this.soda,
  Consumer: Consumer,
  Producer: Producer,
  expr: expr,

  # exported for testing reasons
  _internal:
    Connection: Connection,
    Query: Query,
    Operation: Operation,
    util:
      toBase64: toBase64,
      handleLiteral: handleLiteral,
      handleOrder: handleOrder
)


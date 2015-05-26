var pgEscape = require('pg-escape')
var debug = require('debug')('pg-metadata')

module.exports = pgMetadata
module.exports.createQuery = createQuery
module.exports.createMetadataObject = createMetadataObject
var informationSchemaFields = module.exports.informationSchemaFields = [
	'column_name', 
	'udt_name', 
	'data_type', 
	'character_maximum_length',
	'table_name',
	'table_schema',
	'table_catalog',
	'is_nullable',
	'numeric_precision'
]

function pgMetadata(connection, tableName, schemaName, databaseName, callback) {

	if (!connection)
		throw new Error('must provide a connection object')

	if (typeof (tableName) === 'function') {
		callback = tableName
		tableName = undefined
		schemaName = undefined
		databaseName = undefined
	}

	if (typeof (schemaName) === 'function') {
		callback = schemaName
		schemaName = undefined
		databaseName = undefined
	}

	if (typeof (databaseName) === 'function') {
		callback = databaseName
		databaseName = undefined
	}

	if (!callback)
		throw new Error('must provide a callback')

	var query = pgEscape(createQuery(tableName, schemaName, databaseName))

	debug(query)

	connection.query(query, function (err, result) {
		if (err) {
			callback(err)
		} else {			
			var schema = createMetadataObject(result.rows)
			debug(schema)
			callback(null, schema)	
		} 
	})
}

function createQuery(opts) {
	opts = opts || {}

	var sql = 'SELECT ' + informationSchemaFields.join(',') + ' FROM information_schema.columns'

	var whereClause = []
	var values = []

	if (opts.table) {
		whereClause.push('table_name=%L')
		values.push(opts.table)
	}

	if (opts.schema) {
		whereClause.push('table_schema=%L')
		values.push(opts.schema)
	} 

	if (opts.database) {
		whereClause.push('table_catalog=%L')
		values.push(opts.database)
	}

	if (whereClause.length > 0) {
		sql += whereClause.join(' AND ')
	}

	return pgEscape.apply(null, [sql].concat(values))
}

function createMetadataObject(resultSet) {
	var metadata = {}

	for (var i = 0; i < resultSet.length; i++) {
		var row = resultSet[i]
		
		var key = row.column_name
		var table = row.table_name
		var schema = row.table_schema

		var database = metadata[row.table_catalog]

		if (!database) {
			metadata[row.table_catalog] = database = {}
		}

		var schema = database[row.table_schema]

		if (!schema) {
			database[row.table_schema] = schema = {}			
		}

		var table = schema[row.table_name]
		
		if (!table) {
			schema[row.table_name] = table = {}
		}

		table[key] = {
			type: row.udt_name,
			length: row.character_maximum_length ?  row.character_maximum_length : row.numeric_precision,
			required: row.is_nullable ? true : false
		}
	}

	return metadata
}
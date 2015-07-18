var pgMetadata = require('./index')
var should = require('should')

describe('pg-metadata', function () {

	var sqlBase = 'SELECT ' + pgMetadata.informationSchemaFields.join(',') + ' FROM information_schema.columns'

	var resultSet = [
		// two columns for table "atable" in schema "aschema" in db "adb"
		{
			character_maximum_length: 244,
			column_name: 'a',
			udt_name: 'varchar',
			table_name: 'atable',
			table_schema: 'aschema',
			table_catalog: 'adb',
			is_nullable: true,
			numeric_precision: null,
			numeric_scale: null,
			numeric_precision_radix: null,
			datetime_precision: null,
			interval_type: null,
			interval_precision: null
		},
		{ 
			character_maximum_length: 244,
			column_name: 'b',
			udt_name: 'varchar',
			table_name: 'atable',
			table_schema: 'aschema',
			table_catalog: 'adb',
			numeric_precision: null,
			numeric_scale: null,
			numeric_precision_radix: null,
			datetime_precision: null,
			interval_type: null,
			interval_precision: null },

		// one column for table "btable" in schema "aschama" in db "adb"
		{ character_maximum_length: 244,
			column_name: 'a',
			udt_name: 'varchar',
			table_name: 'btable',
			table_schema: 'aschema',
			table_catalog: 'adb',
			numeric_precision: null,
			numeric_scale: null,
			numeric_precision_radix: null,
			datetime_precision: null,
			interval_type: null,
			interval_precision: null },

		// one column for table "btable" in schema "bschema" in db "bdb"
		{ character_maximum_length: 244,
			column_name: 'a',
			udt_name: 'varchar',
			table_name: 'btable',
			table_schema: 'bschema',
			table_catalog: 'bdb',
			numeric_precision: null,
			numeric_scale: null,
			numeric_precision_radix: null,
			datetime_precision: null,
			interval_type: null,
			interval_precision: null },

		// two numeric columns for table "ctable" in schema "cschema" in db "cdb"
		{ character_maximum_length: null,
			column_name: 'a',
			udt_name: 'float4',
			table_name: 'ctable',
			table_schema: 'cschema',
			table_catalog: 'cdb',
			numeric_precision: 24,
			numeric_scale: null,
			numeric_precision_radix: 2,
			datetime_precision: null,
			interval_type: null,
			interval_precision: null },

		{ character_maximum_length: null,
			column_name: 'b',
			udt_name: 'numeric',
			table_name: 'ctable',
			table_schema: 'cschema',
			table_catalog: 'cdb',
			numeric_precision: 12,
			numeric_scale: 2,
			numeric_precision_radix: 10,
			datetime_precision: null,
			interval_type: null,
			interval_precision: null },

		// two interval columns for table "dtable" in schema "dschema" in db "ddb"
		{ character_maximum_length: null,
			column_name: 'a',
			udt_name: 'interval',
			table_name: 'dtable',
			table_schema: 'dschema',
			table_catalog: 'ddb',
			numeric_precision: null,
			numeric_scale: null,
			numeric_precision_radix: null,
			datetime_precision: null,
			interval_type: 'YEAR',
			interval_precision: null },

		{ character_maximum_length: null,
			column_name: 'b',
			udt_name: 'interval',
			table_name: 'dtable',
			table_schema: 'dschema',
			table_catalog: 'ddb',
			numeric_precision: null,
			numeric_scale: null,
			numeric_precision_radix: null,
			datetime_precision: null,
			interval_type: null,
			interval_precision: 2 },

		// two datetime columns for table "etable" in schema "eschema" in db "edb"
		{ character_maximum_length: null,
			column_name: 'a',
			udt_name: 'time',
			table_name: 'etable',
			table_schema: 'eschema',
			table_catalog: 'edb',
			numeric_precision: null,
			numeric_scale: null,
			numeric_precision_radix: null,
			datetime_precision: 6,
			interval_type: null,
			interval_precision: null },

		{ character_maximum_length: null,
			column_name: 'b',
			udt_name: 'timestamptz',
			table_name: 'etable',
			table_schema: 'eschema',
			table_catalog: 'edb',
			numeric_precision: null,
			numeric_scale: null,
			numeric_precision_radix: null,
			datetime_precision: 4,
			interval_type: null,
			interval_precision: null },
	]

	describe('creates a query', function () {

		it('without restriction', function () {
			var result = pgMetadata.createQuery()
			result.should.eql(sqlBase)
		})

		it('for a specific database', function () {
			var opts = { database: 'aDatabase' }
			var result = pgMetadata.createQuery(opts)
			result.should.eql(sqlBase + ' WHERE table_catalog=\'' + opts.database + '\'')
		})

		it('for a specific table', function () {
			var opts = { table: 'aTable' }
			var result = pgMetadata.createQuery(opts)
			result.should.eql(sqlBase + ' WHERE table_name=\'' + opts.table + '\'')
		})

		it('for a specific schema', function () {
			var opts = { schema: 'aSchema' }
			var result = pgMetadata.createQuery(opts)
			result.should.eql(sqlBase + ' WHERE table_schema=\'' + opts.schema + '\'')
		})

		it('for a database, schema and table', function () {
			var opts = { schema: 'aSchema', table: 'aTable', database: 'aDatabase' }
			var result = pgMetadata.createQuery(opts)
			result.should.eql(sqlBase + ' WHERE table_name=\'' + opts.table + '\' AND table_schema=\'' + opts.schema + '\' AND table_catalog=\'' + opts.database + '\'')

		})
	})

	describe('creates a metadata object from a query result set that contains all', function () {
		it('the databases', function () {
			var actual = pgMetadata.createMetadataObject(resultSet)
			
			actual.should.have.property('adb')
			actual.should.have.property('bdb')
			actual.should.have.property('cdb')
			actual.should.have.property('ddb')
			actual.should.have.property('edb')
		})

		it('schemas in a database', function () {
			var actual = pgMetadata.createMetadataObject(resultSet)
			
			actual.adb.should.have.property('aschema')
			actual.bdb.should.have.property('bschema')
			actual.cdb.should.have.property('cschema')
			actual.ddb.should.have.property('dschema')
			actual.edb.should.have.property('eschema')
		})

		it('tables in each schema', function () {
			var actual = pgMetadata.createMetadataObject(resultSet)
			
			actual.adb.aschema.should.have.property('atable')
			actual.adb.aschema.should.have.property('btable')

			actual.bdb.bschema.should.have.property('btable')
		})

		it('columns in each table', function () {
			var actual = pgMetadata.createMetadataObject(resultSet)
			
			actual.adb.aschema.atable.should.have.property('a')
			actual.adb.aschema.atable.should.have.property('b')
			actual.bdb.bschema.btable.should.have.property('a')
		})

		it('types of data and length for each columns', function () {
			var actual = pgMetadata.createMetadataObject(resultSet)
			
			actual.adb.aschema.atable.a.should.have.property('type', 'varchar')
			actual.adb.aschema.atable.a.should.have.property('length', 244)
			actual.adb.aschema.atable.a.should.not.have.property('scale')
			actual.adb.aschema.atable.a.should.not.have.property('precision_radix')
			actual.adb.aschema.atable.a.should.have.property('required', true)

			actual.adb.aschema.atable.b.should.have.property('type', 'varchar')
			actual.adb.aschema.atable.b.should.have.property('length', 244)
			actual.adb.aschema.atable.b.should.have.property('required', false)

			actual.bdb.bschema.btable.a.should.have.property('type', 'varchar')
			actual.bdb.bschema.btable.a.should.have.property('length', 244)

			actual.cdb.should.be.eql({
				cschema: {
					ctable: {
						a: { type: 'float4', required: false, precision: 24, scale: null, precision_radix: 2 },
						b: { type: 'numeric', required: false, precision: 12, scale: 2, precision_radix: 10 }
					}
				}
			})

			actual.ddb.should.be.eql({
				dschema: {
					dtable: {
						a: { type: 'interval', required: false, 'interval_type': 'YEAR', precision: null },
						b: { type: 'interval', required: false, 'interval_type': null, precision: 2 }
					}
				}
			})

			actual.edb.should.be.eql({
				eschema: {
					etable: {
						a: { type: 'time', required: false, precision: 6 },
						b: { type: 'timestamptz', precision: 4, required: false }
					}
				}
			})
		})
	})

	it('queries the database metadata tables', function (done) {
		pgMetadata(new MockConnection(resultSet), function(err, metadata) {
			metadata.should.have.property('adb')
			.which.have.property('aschema')
			.which.have.property('atable')
			.which.have.property('a')
			.which.have.property('type', 'varchar')
			
			done()
		})
	})
})

function MockConnection(resultSet) {
	this.error = false
	this.resultSet = resultSet
}

MockConnection.prototype.query = function(q, cb) {
	var self = this
	setImmediate(function () {
		if (self.error)
			cb(new Error('lalala'))
		else 
			cb(null, { rows: self.resultSet })
	})
}

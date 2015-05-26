pg-metadata
---------

get the metadata of a postresql or redshift db

```javascript	
    var connection = /* anything that exposes a .query('sql', params, function(err, results) {}) interface to a postgresql server */
    var pgMetadata = require('pg-metadata')

    pgMetadata(connection, /* tableName, schemaName, databaseName */, function(err, metadata) {
        console.log(metadata.mydb.myschema.mytable.columnA.type) // prints varchar
        console.log(metadata.mydb.myschema.mytable.columnA.length) // 200
        console.log(metadata.mydb.myschema.mytable.columnA.required) // prints true, field is not nullable
    })
```

#### Doing it yourself
instead of letting pgMetadata run the query, you can run it yourself:
```javascript
    var pgMetadata = require('pg-metadata')
    var query = pgMetadata.createQuery(/* tableName, ,schemaName, databaseName */)

    // run the query and when you get the result set do:
    var metadata = pgMetadata.createMetadataObject(resultSet)    
```

See also [pg-schema](https://github.com/kessler/pg-schema)
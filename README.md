pg-metadata
---------

get the metadata of a postresql or redshift db

```javascript	
    var connection = /* anything that exposes a .query('sql', params, function(err, results) {}) interface to a postgresql server */
    var pgMetadata = require('pg-metadata')

    pgMetadata(connection, /* { table: 'tableName', schema: 'schemaName', database: 'databaseName' } */
        function(err, metadata) {

        // prints varchar
        console.log(metadata.mydb.myschema.mytable.columnA.type) 

        // prints 200
        console.log(metadata.mydb.myschema.mytable.columnA.length) 

        // prints true, field is not nullable
        console.log(metadata.mydb.myschema.mytable.columnA.required) 
    })
```

#### Doing it yourself
instead of letting pgMetadata run the query, you can run it yourself:
```javascript
    var pgMetadata = require('pg-metadata')
    var query = pgMetadata.createQuery(/* { table: 'tableName', schema: 'schemaName', database: 'databaseName' } */)

    // run the query and when you get the result set do:
    var metadata = pgMetadata.createMetadataObject(resultSet)    
```

See also [pg-schema](https://github.com/kessler/pg-schema)
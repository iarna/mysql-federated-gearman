"use strict";
var mysql = require('mysql2');
var charset = require('mysql2/lib/constants/charsets');
var type = require('mysql2/lib/constants/types');
var Gearman = require('node-gearman');
var Promise = require('promisable');
var copy = require('shallow-copy');

var server = mysql.createServer();
server.listen(9306);

for (var ii in [1,2,3]) {
    var worker = new Gearman('localhost',4730);
    worker.registerWorker("upper", function(payload, job){
        setTimeout(function(){
            if (!payload) { job.error('no payload provided'); return }
            var response =  payload.toString().toUpperCase();
            job.end(response);
        },1000);
    });
}

var column = function(name,type,length) {
    return {
      catalog: 'def', 
      schema: 'test', 
      table: 'table', 
      orgTable: 'table', 
      name: name, 
      orgName: name, 
      characterSet: charset.UTF8_BIN,  
      columnLength: length, 
      columnType: type, 
      flags: 0, 
      decimals: 0
    }
}

var tableStatus = {};
tableStatus.columns = [
    column('Name',type.VARCHAR,200),
    column('Engine',type.VARCHAR,200),
    column('Version',type.TINY,3),
    column('Row_format',type.VARCHAR,200),
    column('Rows',type.LONG,11),
    column('Avg_row_length',type.LONG,11),
    column('Data_length',type.LONG,11),
    column('Max_data_length',type.LONG,11),
    column('Index_length',type.LONG,11),
    column('Data_free',type.LONG,11),
    column('Auto_increment',type.LONG,11),
    column('Create_time',type.DATETIME,0),
    column('Update_time',type.DATETIME,0),
    column('Collation',type.VARCHAR,200),
    column('Checksum',type.VARCHAR,200),
    column('Create_options',type.VARCHAR,200),
    column('Comment',type.VARCHAR,200) ];
tableStatus.rows = [
    ['jobs','QUEUE',10,'',null,0,0,0,0,0,null,null,null,null,'utf8_bin',null,'',''] ];

var columns = [
    column('id',type.LONGLONG,20),
    column('command',type.VARCHAR,200),
    column('args',type.BLOB,65535),
    column('status',type.VARCHAR,20),
    column('result',type.BLOB,65535) ];
var querycount = 0;
var queries = [];


server.on('connection', function(conn) {
  console.log('connection');

  var client = new Gearman('localhost',4730);

  conn.serverHandshake({
    protocolVersion: 10,
    serverVersion: 'node.js rocks',
    connectionId: 1234,
    statusFlags: 2,
    characterSet: 8,
    capabilityFlags: 0xffffff
  });
  conn.on('query', function(sql) {
    console.log('query:' + sql);
    if (sql.match(/1=0/)) {
        conn.writeColumns(columns);
        conn.writeEof();
        return;
    }
    if (sql.match(/SHOW TABLE STATUS LIKE/)) {
        conn.writeColumns(tableStatus.columns);
        tableStatus.rows.forEach(function(row) { conn.writeTextRow(row) });
        conn.writeEof();
        return;
    }
    var result;
    if (result = sql.match(/INSERT INTO `table` [(]`id`, `command`, `args`[)]  VALUES  [(]\d+, '([^']+)', '([^']+)'[)]/)) {
        var query = {
            id:      querycount+1, 
            command: result[1],
            args:    result[2],
            status:  'created'
        };
        queries[querycount] = Promise(function(resolve){
            var job = client.submitJob(query.command, query.args);
            job.on('error', resolve.reject);
            job.on('timeout', function(){ resolve.reject(new Error('Timeout')) });
            var data = '';
            job.on('data', function (D){ data = D });
            job.on('end', function (){ resolve.fulfill(data) });
        })
        .then(function(V){
            query.status = 'ok';
            query.result = V.toString();
            return query;
        })
        .catch(function(E){
            query.status = 'error';
            query.result = E;
            return query;
        });
        querycount ++;
        conn.writeOk({affectedRows: 0, insertId: querycount});
        return;
    }
    if (result = sql.match(/SELECT .*`id` = (\d+)/)) {
        var query;
        var index = result[1]-1;
        var queryResult = queries[index];
        if (queryResult) {
            queryResult.then(function(query){
                console.log(query);
                conn.writeTextResult([query],columns);
                delete queries[query.id];
            });
        }
        else {
            conn.writeColumns(columns);
            conn.writeEof();
        }
        return;
    }
    conn.writeOk({affectedRows: 0, insertId: 1})
  });
  conn.on('end', function(){
      client.end();
  });
});

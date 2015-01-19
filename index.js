#!/bin/env node
var fs = require('fs');
var mysql = require('mysql');
var async = require('async');
var program = require('commander');
var CSV = require('comma-separated-values');

program
  .option('-c, --config <path>', 'set config path', './config.json')
  .parse(process.argv);

var config = require(program.config);
var client = mysql.createConnection(config);
var encoded = CSV.encode([config.fields]);
var stream = fs.createWriteStream(config.file);
stream.write(encoded + '\r\n');
client.connect();
var downloaded = 0;
var lastKey = 0;
console.time('Wrote');
var resultLength = config.limit;
async.until(
  function() {
    return resultLength < config.limit;
  },
  function(cb) {
    var sql = mysql.format(
      'SELECT ??, ?? FROM ?? WHERE '
        + config.condition
        + 'AND ?? > ? ORDER BY ?? ASC LIMIT ?;',
      [
        config.key,
        config.fields,
        config.table,
        config.key,
        lastKey,
        config.key,
        config.limit
      ]
    );

    client.query(sql, function(err, result) {
      if (!result || !result.length) {
        resultLength = 0;
        return cb(err)
      };
      resultLength = result.length;
      downloaded += resultLength;
      if (process.stdout.isTTY) {
        process.stdout.write("\u001b[2J\u001b[0;0H");
      }
      console.log('Downloaded: %s', downloaded);
      lastKey = result[result.length - 1][config.key];
      process.nextTick(function() {
        var arr = [];
        for (var i = 0, l = result.length; i < l; i ++) {
          var row = result[i];
          var csvRow = [];
          for (var j = 0, l2 = config.fields.length; j < l2; j ++) {
            var field = config.fields[j];
            csvRow.push(row[field]);
          }
          arr.push(csvRow);
        }
        var encoded = CSV.encode(arr);
        stream.write(encoded + '\r\n');
        console.timeEnd('Wrote');
      });
      cb(err)
    });
  },
  function(err) {
    if (err) {
      console.log(err);
    }
    stream.write('', function() {
      process.exit();
    });
  }
);

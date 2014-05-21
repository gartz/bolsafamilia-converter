#!/usr/bin/env node

var fs = require('fs');
var readline = require('readline');
var Q = require('q');
var program = require('commander');

// Arguments without the program argument or it folder
var argv = process.argv.slice(2);

program
  .version('0.0.1')
  .usage('<file> [options] ...')
  .option('-s', 'Define STATE column', parseInt)
  .option('-v', 'Define VALUE column, will count as benefited', parseInt)
  .option('-c', 'Define CITIZENS column', parseInt)
  .option('-d', 'CSV divisor rule column')
  .on('--help', function () {
    console.log('  Columns and lines begin from 1 and not 0.');
    console.log('');
    console.log('  Examples:');
    console.log('');
    console.log('    ./bolsafamilia.js ./dbs_demo/2014bolsa_familia_1000.csv -v 11 -s 1 ./dbs_demo/2012base.csv -c 7 -s 4 -d ,');
    console.log('');
  })
  .parse(process.argv);

var files = [];

// Setup file templates configurations
var params = {};
for (var i = 0; i < argv.length; i++) {
  var param = argv[i];
  switch (param) {
    case '-s':
      params.state = + argv[++i] - 1;
      break;
    case '-v':
      params.value = + argv[++i] - 1;
      break;
    case '-c':
      params.citizens = + argv[++i] - 1;
      break;
    case '-d':
      params.divisor = argv[++i];
      break;
    default:
      if (param.indexOf('.csv') != -1) {
        if (params.file) {
          files.push(params);
          params = {};
        }
        params.file = param;
      }
  }
}
if (params.file) {
  files.push(params);
  params = {};
}

// if there isn't any configuration just show help
if (files.length === 0) {
  program.help();
}

// prepare states dict
var states = {};

// A helper for creating data
function Region() {
  this.value = 0;
  this.citizens = 0;
  this.benefits = 0;
}

// Create a total (for the country) object
var total = new Region();

// mark the initial timestamp
var dateInit = Date.now();


files.forEach(function (tmp) {
  // Checkup for integrit before begin
  if (typeof tmp.state !== 'number') {
    program.outputHelp();
    throw new Error('No STATE column defined for file ' + tmp.file);
  }
  // setup deferrer
  tmp.defer = Q.defer();
  
  // start reading lines
  var rl = readline.createInterface({
    input: fs.createReadStream(tmp.file),
    output: process.stdout,
    terminal: false
  });
  
  // resolve when finish reading the file
  rl.on('close', function () {
    tmp.defer.resolve();
  });
  
  var callbacks = [];
  
  if (typeof tmp.citizens === 'number') {
    callbacks.push(function (columns, state) {
      var citizens = + columns[tmp.citizens];
      
      if (isNaN(citizens)) {
        return;
      }
      
      total.citizens += citizens;
      states[state].citizens += citizens;
    });
  }
  if (typeof tmp.value === 'number') {
    callbacks.push(function (columns, state) {
      
      var value = + columns[tmp.value];
      
      if (isNaN(value)) {
        return;
      }

      total.value += value;
      total.benefits++;
      
      states[state].value += value;
      states[state].benefits++;
    });
  }
  
  if (callbacks.length === 0) {
    program.outputHelp();
    throw new Error('File must have a column for CITIZENS and/or VALUE ' + tmp.file);
  }
  
  // read the file lines
  rl.on('line', function (line) {
    var columns = line.split(tmp.divisor || '\t');
    var state = columns[tmp.state];
    
    if (!states[state]) {
      states[state] = new Region();
    }

    callbacks.forEach(function (cb) {
      cb(columns, state);
    });
  });
});

// map defered promisses from files
var promises = files.map(function (file) {
  return file.defer.promise;
});

Q.all(promises).done(function () {
  for (var state in states) {
    var citizens = states[state].citizens;
    var benefits = states[state].benefits;
    
    states[state].percent = (benefits / citizens) * 100;
    
    if (isNaN(states[state].percent)) {
      delete states[state];
    }
  }
  
  total.percent = (total.benefits / total.citizens) * 100;
  
  var dateEnd = Date.now();
  
  var results = {
    country: total,
    states: states,
    statistics: {
      start: dateInit,
      end: dateEnd,
      diff: dateEnd - dateInit,
      diffSeconds: (dateEnd - dateInit) / 1e3
    }
  };
  
  console.log(results);
});
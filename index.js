
/**
 * Module dependencies.
 */

var fs = require('fs');
var MongoClient = require('mongodb').MongoClient;
var input;
var collection;

/**
 * Run.
 */

exec('tmp/taxdmp/nodes.dmp', parseNode, function(){
  exec('tmp/taxdmp/names.dmp', parseName, function(){
    write(function(){
      console.log('done');
      process.exit();
    });
  });
});

function parseNode(line) {
  var record = {
    _id: line[0],
    parentGenbankId: line[1],
    rank: 'no rank' == line[2] ? undefined : line[2],
  // parent tax_id       -- parent node id in GenBank taxonomy database
  // rank          -- rank of this node (superkingdom, kingdom, ...) 
  // embl code       -- locus-name prefix; not unique
  // division id       -- see division.dmp file
  // inherited div flag  (1 or 0)    -- 1 if node inherits division from parent
  // genetic code id       -- see gencode.dmp file
  // inherited GC  flag  (1 or 0)    -- 1 if node inherits genetic code from parent
  // mitochondrial genetic code id   -- see gencode.dmp file
  // inherited MGC flag  (1 or 0)    -- 1 if node inherits mitochondrial gencode from parent
  // GenBank hidden flag (1 or 0)            -- 1 if name is suppressed in GenBank entry lineage
  // hidden subtree root flag (1 or 0)       -- 1 if this subtree has no sequence data yet
  // comments        -- free-text comments and citations
  };

  return record;
}

function parseName(line) {
  if ('scientific name' != line[3]) return;

  var record = {
    _id: line[0],
    title: line[1]
  };

  return record;
}

/**
 * Script to convert pubmed taxonomy files to data.json.
 */

function exec(path, parse, done) {
  input = fs.createReadStream(path, { encoding: 'utf-8' });
  // to start over:
  // mongo cdnjson-organisms --eval "db.dropDatabase()"
  // to export:
  // mongoexport --db cdnjson-organisms --collection organisms --out data.json --jsonArray
  open(function(c){
    collection = c;
    return done();
    //collection.ensureIndex('genbankId', function(){
      lines(input, function(line){
        line = line.trim().split(/\s*\|\s*/g);
        var record = parse(line);
        if (record) {
          collection.update({ _id: line[0] }, record, { upsert: true }, function(err){
            if (err) throw err;
          }); 
        }
      }, done);
    //});
  });
}

function open(fn) {
  MongoClient.connect('mongodb://127.0.0.1:27017/cdnjson-organisms', function(err, db){
    if (err) throw err;

    fn(db.collection('organisms'));
  });
}

/**
 * Get lines from stream.
 */

function lines(stream, fn, done) {
  var last = '';

  input.on('data', function(data){
    var arr = (last + data).split('\n');
    if (arr) {
      last = arr.pop();

      process.nextTick(function(){
        arr.forEach(function(x){
          fn(x);
        });
      });
    } else {
      last = '';
    }
  });

  input.on('end', function(){
    process.nextTick(done);
  });
}

/**
 * Write to data.json file.
 */

function write(fn) {
  var output = fs.createWriteStream('tmp/data.json', { encoding: 'utf-8' });
  output.write('[');
  var cursor = collection.find().stream();
  var first = true;

  cursor.on('data', function(doc){
    doc.genbankId = doc._id;
    delete doc._id;
    var str = first ? '\n' : ',\n';
    str = str + '  ' + JSON.stringify(doc);
    first = false;
    if (!output.write(str)) {
      cursor.pause();
      output.once('drain', function(){
        process.nextTick(function(){
          cursor.resume();
        });
      });
    }
  });

  cursor.on('end', function(){
    output.write(']');
    fn();
  });

  // maybe can do export:
  // http://docs.mongodb.org/manual/core/import-export/
  // var output = fs.createWriteStream
  // 
}
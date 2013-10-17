
var java = require("java");
java.classpath.push("iBoxDBv13.jar")

var DB = java.import("iBoxDB.LocalServer.DB");

Object.prototype.jmap = function () {
    if (this.keySetSync) {
        var list = DB.arraySync(this.keySetSync());
        var r = {};
        for (var i = 0; i < list.length; i++) {
            r[list[i]] = this.getSync(list[i]);
        }
        return r;
    }
    if (this.iteratorSync) {
        return DB.arraySync(this);
    }
    if (this instanceof Array) {
        if (this.length == 0) { return DB.arraySync(); }
        var r = DB.arraySync(this[0]);
        for (var i = 1; i < this.length; i++) {
            r = DB.arraySync(r, this[i]);
        }
        return r;
    }
    var o = java.newInstanceSync("java.util.HashMap");
    for (var f in this) {
        if (typeof (this[f]) == 'function') { continue; }
        o.putSync(f, this[f]);
    }
    return o;
}
function printObjectArray( list ){
    list = list.jmap();
    for (var i = 0; i < list.length; i++) {
        console.log(list[i].jmap());
    }
}

var db = java.newInstanceSync("iBoxDB.LocalServer.DB", 2, "");
db.ensureTableSync("Table", { ID: 0 }.jmap(), null);
// 1 = autoIncrementID , 2048 bytes=fixed length [option]
db.ensureParametersSync("Table", 1, 2048);
db.openSync();

// supports unstructured data insert and query
db.insertSync("Table", { value: "hello db" }.jmap());
db.insertSync("Table", { name: "iBoxDB", version: "1.3 java" }.jmap());
db.insertSync("Table", { product: "iBoxDB", size: 200 }.jmap());

console.log("*Find");
var h = db.findSync("Table", 1);
console.log(h.jmap());

console.log("*Query 1");
h = db.selectSync( "from Table where value==?", 'hello db' );
printObjectArray(h);

console.log("*Query 2");
h = db.selectSync("from Table where product==?", 'iBoxDB');
printObjectArray(h);

console.log("*ALL");
h = db.selectSync( "from Table");
printObjectArray(h);
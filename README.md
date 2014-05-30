iBoxDB.Java
=======
Traditional table with unstructured data, pure java database, for both Java and Android, compatible with node.js.

iBoxDB has a well designed interface with great performance and capability for agile development, you can focus on the applications. It can embed into an application and deploy on mobiles, desktops, servers, then help designers to persist objects and documents thread-safely with transaction support. 

Features: CURD, Index, composite Index, query language, transactions support, concurrency control, memory management, hot replication, file and in-memory database, dynamic columns, NoSQL ORM, high performance, zero configuration, copy and run, no dependencies.



Examples:

Each box is an isolated data's space (Transaction-Based)

     try( Box box = db.cube() ){
         box.insert( ... );
         CommitResult r = box.commit();
     }


Normal Object

    Member m = new Member();
    m.ID = box.newId(Member.IncTableID, 1);
    m.setName("Andy");
    m.setTags(new Object[] { "Nice", "Strong" });
    box.insert("Table", m);
		
		
Dynamic Object (document database)

    game.put("GameType", "ACT");
    box.insert("Table", game);
  	
  	
Key-Value Style Query

    box.bind("Table", ID).select( Member.class );
    //Composite-Key Supported
    box.bind("Table2",8, "MyID").select(Product.class);
    
SQL like Query

    box.select( Member.class, "from Member where Name==?", "MyName" );
   
Custom DatabaseFunction use java

    public static class QueryArray implements IFunction {
			String match;

			public QueryArray(String match) {
				this.match = match;
			}

			public Object execute(int argCount, Object[] args) {
				Object[] tags = (Object[]) args[0];
				if (tags == null) {
					return false;
				}
				for (Object t : tags) {
					if (match.equals(t)) {
						return true;
					}
				}
				return false;
			}
		}
		
    box.select(Member.class, "from Member where [Tags]",  new QueryArray("Strong")
    
**UpdateIncrement**

|   |Apply To | Trigger | Type | Value From |
|---|---------|---------|------|------------|
| AutoIncrement |  primary key | insert |  number | Table max(ID)+1 |
| *UpdateIncrement* | non-primary key | insert/update | long | Database newId( 1024, 1 ) | 

**Selecting Tracer**

|   | Thread | Usage |
|---|--------|-------|
| Locker | blocked | read/write same record |
| *Tracer* | non-blocked | read/write different records |
    
    
Replication Master-Slave , Master-Master



![](https://raw.github.com/iboxdb/forjava/master/images/show.png)

[Benchmark with MongoDB](https://github.com/iboxdb/forjava/wiki/Benchmark-with-MongoDB)   







  

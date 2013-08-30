iBoxDB.java
=======
Traditional way deal with data, with NoSQL fashional styles, pure java database, not only CURD and QL, but also Replication services, for both Java and Android. 

features: CURD, Index, composite Index, query language, transactions support, concurrency control, replication, in-memory database supported, dynamic columns, NoSQL styles, high performance, zero configuration, copy and run.


[Benchmark with MongoDB](https://github.com/iboxdb/forjava/wiki/Benchmark-with-MongoDB)


Examples:

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
    

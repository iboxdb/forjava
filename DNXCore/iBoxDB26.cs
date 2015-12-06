//#define WINDOWS_PHONE
//#define NET2

using System;
using System.Text;
using System.Collections.Generic;

using iBoxDB.LocalServer;
using iBoxDB.LocalServer.IO;


/*
  "frameworks": {
    "dnxcore50": {
      "dependencies": {
          "System.IO.FileSystem": "4.0.0",
          "System.Threading.Thread": "4.0.0-*",
        
             "System.Console": "4.0.0-*",
             "System.Threading.Tasks.Parallel": "4.0.0-*"
      }
    }
  }

copy to  "static void Main(string[] args)"
  iBoxDB.LocalServer.DB.Root("/tmp/");
  var text =  iBoxDB.TestHelper.RunALL();
  Console.WriteLine(text);
*/
namespace iBoxDB
{
    public class TestHelper
    {
        public static String RunALL(bool moreSpeedTest = false)
        {

            DBPlatform.SetStorage();

            String text = "";
            text += iBoxDB.Shortcut.PMain().ToString();
            text += iBoxDB.Example.Start.PMain().ToString();
            text += iBoxDB.Example.MasterSlave.PMain().ToString();
            text += iBoxDB.Example.MasterMaster.PMain().ToString();
            text += iBoxDB.Example.BeyondSQL.PMain().ToString();

            if (moreSpeedTest)
            {
                System.GC.Collect();
                text += iBoxDB.Example.Speed.PMain(DBPlatform.Speed_ThreadCount).ToString();
                System.GC.Collect();
                text += iBoxDB.Example.Speed.ReplicationSpeed(
                    DBPlatform.Replication_ThreadCount, DBPlatform.Replication_Time).ToString();
            }
            else
            {
                //text += iBoxDB.Example.Speed.PMain(10).ToString();
                //text += iBoxDB.Example.Speed.ReplicationSpeed(10, 1).ToString();
            }
            DBPlatform.DeleteDB();
            return text + "\r\n";
        }
    }

    public class Shortcut
    {
        public class Record
        {
            public int ID;
            public string Name;
        }
        public static StringBuilder PMain()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("\r\n*Shortcut\r\n");
            try
            {
                DBPlatform.DeleteDB();

                var server = new DB(1);
                server.GetConfig().EnsureTable<Record>("Table", "ID");
                var db = server.Open();

                db.Insert("Table", new Record { ID = 1, Name = "Andy" });
                var o1 = db.SelectKey<Record>("Table", 1);
                sb.Append("Name1 " + o1.Name + "\r\n");

                o1.Name = "Kelly";
                db.Update("Table", o1);
                o1 = null;
                var o2 = db.SelectKey<Record>("Table", 1);
                sb.Append("Name2 " + o2.Name + "\r\n");

                server.Dispose();
            }
            catch (Exception ex)
            {
                sb.Append(ex.ToString() + "\r\n");
            }
            return sb;
        }
    }

}

namespace iBoxDB.Example
{

    using System.Threading;

    using iBoxDB.LocalServer.Replication;
    using iBoxDB.Example.Server;
    using iBoxDB.DBDebug;

    #region Start
    public class Start
    {
        public static StringBuilder PMain()
        {

            StringBuilder sb = new StringBuilder();
            sb.Append("\r\n*Start\r\n");
            try
            {
                DBPlatform.DeleteDB();
                using (var server = new MyServer())
                {
                    using (var db = server.GetInstance(1))
                    {
                        using (var box = db.Cube())
                        {
                            // insert member & product
                            box.Bind("Member").Insert(
                                new Member()
                                {
                                    ID = box.NewId(Member.IncTableID, 1),
                                    Name = "Andy",
                                    RegTime = new DateTime(2013, 1, 2),
                                    Tags = new string[] { "Nice", "Strong" }
                                }
                                );
                            box.Bind("Member").Insert(
                             new MemberVIP()
                             {
                                 ID = box.NewId(Member.IncTableID, 1),
                                 Name = "Kelly",
                                 RegTime = new DateTime(2013, 1, 3),
                                 Tags = new string[] { "Gamer" },
                                 VIP = 3
                             }
                             );

                            // Dynamic Column 
                            Product game = new Product()
                            {
                                Type = 8,
                                UID = new Guid("{22222222-0000-0000-0000-000000000000}"),
                                Name = "MoonFlight"
                            };
                            game["GameType"] = "ACT";
                            box.Bind("Product").Insert(game);

                            box.Commit().Assert();
                        }


                        using (var box = db.Cube())
                        {
                            // SQL like
                            //  > < >= <=  == != 
                            //  & | () 
                            //  []
                            var m = GetFirst(box.Select<MemberVIP>("from Member where VIP>?", 1));
                            sb.Append("Kelly RegTime " + m.RegTime + "\r\n");
                            m.Name = "Kelly J";
                            m.Amount = 100;
                            box.Bind("Member", m.ID).Update(m);
                            box.Commit().Assert();
                        }
                        using (var box = db.Cube())
                        {
                            var m = GetFirst(box.Select<Member>("from Member where Name==?", "Kelly J"));
                            sb.Append("Updated : " + m.Name + "  " + m.Amount + "\r\n");
                        }

                        using (var box = db.Cube())
                        {
                            // Key-Value Style , Composite-Key Supported
                            var cs = box
                                .Bind("Product", 8, new Guid("{22222222-0000-0000-0000-000000000000}"))
                                .Select<Product>();
                            sb.Append("Product Name " + cs.Name + "  " + cs["GameType"] + "\r\n");
                        }

                        using (var box = db.Cube())
                        {
                            //Custom QueryFunction  
                            // [] <= call IFunction Interface
                            // [A,B] <=  Fields will be passed  
                            var list = box.Select<Member>("from Member where [Tags]", new QueryArray("Strong"));
                            sb.Append("The Strong one is ");
                            foreach (var m in list)
                            {
                                sb.Append("'" + m.Name + "'\r\n");
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                sb.Append(ex.ToString() + "\r\n");
            }
            return sb;
        }

        private static T GetFirst<T>(IEnumerable<T> list)
        {
            foreach (var o in list)
            {
                return o;
            }
            return default(T);
        }
    }
    #endregion

    #region MasterSlave
    public class MasterSlave
    {
        public static StringBuilder PMain()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("\r\n*Replication: Master-Slave");
            try
            {
                DBPlatform.DeleteDB();
                using (var server = new ReplicableServer())
                {
                    var masterA = server.GetInstance(ReplicableServer.MasterA_DBAddress);
                    var slaveA = server.GetInstance(ReplicableServer.SlaveA_DBAddress);

                    using (var box = masterA.Cube())
                    {
                        for (var i = 0; i < 3; i++)
                        {
                            box.Bind("Member").Insert(
                              new Member()
                              {
                                  ID = box.NewId(Member.IncTableID, 1),
                                  Name = "LN " + i
                              }
                              );
                        }
                        box.Commit().Assert();
                    }

                    // Post database's changes to slave
                    var recycler = (InMemoryBoxRecycler)masterA.GetBoxRecycler();
                    lock (recycler.GetPackage())
                    {
                        foreach (var p in recycler.GetPackage())
                        {
                            if (p.Socket.SourceAddress == ReplicableServer.MasterA_DBAddress)
                            {
                                (new BoxData(p.OutBox)).SlaveReplicate(slaveA).Assert();
                            }
                        }
                        recycler.GetPackage().Clear();
                    }

                    // checking
                    sb.Append("\r\nMasterA Address is " + masterA.LocalAddress + " \r\n");
                    using (var box = masterA.Cube())
                    {
                        foreach (var o in box.Select<Member>("from Member", null))
                        {
                            sb.Append(o.Name + " , ");
                        }
                    }
                    sb.Append("\r\nSlaveA Address is " + slaveA.LocalAddress + " \r\n");
                    using (var box = slaveA.Cube())
                    {
                        foreach (var o in box.Select<Member>("from Member", null))
                        {
                            sb.Append(o.Name + " , ");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                sb.Append(ex.ToString() + "\r\n");
            }
            return sb;
        }

    }
    #endregion

    #region MasterMaster
    public class MasterMaster
    {
        public static StringBuilder PMain()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("\r\n\r\n*Replication: Master-Master");
            try
            {
                DBPlatform.DeleteDB();
                using (var server = new ReplicableServer())
                {
                    var masterA = server.GetInstance(ReplicableServer.MasterA_DBAddress);
                    var masterB = server.GetInstance(ReplicableServer.MasterB_DBAddress);

                    // send to MasterB_Address
                    using (var box = masterA.Cube(ReplicableServer.MasterB_DBAddress))
                    {
                        for (var i = 0; i < 3; i++)
                        {
                            box.Bind("Member").Insert(
                              new Member()
                              {
                                  ID = box.NewId(Member.IncTableID, 1) * 1000 + ReplicableServer.MasterA_DBAddress,
                                  Name = "A" + i
                              }
                              );
                            //or Key = [ID,Address]
                            //m.ID =  box.NewId(Member.IncTableID, 1) ;
                            //m.Address = box.LocalAddress;
                            //box.Bind("Member").Insert(m);
                        }

                        box.Commit().Assert();
                    }
                    //send to MasterA_Address
                    using (var box = masterB.Cube(ReplicableServer.MasterA_DBAddress))
                    {
                        for (var i = 0; i < 3; i++)
                        {
                            box.Bind("Member").Insert(
                              new Member()
                              {
                                  ID = box.NewId(Member.IncTableID, 1) * 1000 + ReplicableServer.MasterB_DBAddress,
                                  Name = "B" + i
                              }
                              );
                        }
                        box.Commit().Assert();
                    }

                    List<Package> buffer;
                    var recycler = (InMemoryBoxRecycler)masterA.GetBoxRecycler();
                    lock (recycler.GetPackage())
                    {
                        buffer = new List<Package>(recycler.GetPackage());
                        recycler.GetPackage().Clear();
                    }
                    recycler = (InMemoryBoxRecycler)masterB.GetBoxRecycler();
                    lock (recycler.GetPackage())
                    {
                        buffer.AddRange(recycler.GetPackage());
                        recycler.GetPackage().Clear();
                    }
                    foreach (var p in buffer)
                    {
                        if (p.Socket.DestAddress == ReplicableServer.MasterA_DBAddress)
                        {
                            (new BoxData(p.OutBox)).MasterReplicate(masterA);
                        }
                        if (p.Socket.DestAddress == ReplicableServer.MasterB_DBAddress)
                        {
                            (new BoxData(p.OutBox)).MasterReplicate(masterB);
                        }
                    }

                    // checking
                    sb.Append("\r\nMasterA Address is " + masterA.LocalAddress + " \r\n");
                    using (var box = masterA.Cube())
                    {
                        foreach (var o in box.Select<Member>("from Member", null))
                        {
                            sb.Append(o.Name + " , ");
                        }
                    }
                    sb.Append("\r\nMasterB Address is " + masterB.LocalAddress + " \r\n");
                    using (var box = masterB.Cube())
                    {
                        foreach (var o in box.Select<Member>("from Member", null))
                        {
                            sb.Append(o.Name + " , ");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                sb.Append(ex.ToString() + "\r\n");
            }
            return sb;
        }
    }
    #endregion

    #region UpdateIncrement Tracer
    public class BeyondSQL
    {
        public static StringBuilder PMain()
        {
            StringBuilder sb = new StringBuilder();
            try
            {
                DBPlatform.DeleteDB();
                using (var server = new MyServer())
                {

                    using (var db = server.GetInstance())
                    {
                        // UpdateIncrement,version number from box.NewId(byte.MaxValue, 1);
                        sb.Append("\r\n\r\n*Version Control \r\n");
                        MemberInc m = new MemberInc();
                        m.ID = 1;
                        m.Name = "Andy";

                        sb.Append("number increasing: ");
                        db.Get().Insert("MemberInc", m);
                        MemberInc mg = db.Get().SelectKey<MemberInc>(
                                        "MemberInc", 1L);
                        sb.Append(mg.Version);

                        db.Get().Update("MemberInc", mg);
                        mg = db.Get().SelectKey<MemberInc>("MemberInc",
                                        1L);
                        sb.Append(" " + mg.Version);
                        db.Get().Update("MemberInc", mg);
                        mg = db.Get().SelectKey<MemberInc>("MemberInc",
                                        1L);
                        sb.Append(" " + mg.Version);

                        // Selecting Tracer
                        sb.Append("\r\n*Selecting Tracer \r\n");
                        using (var boxTracer = db.Cube())
                        {
                            bool keepTrace = true;
                            Member tra = boxTracer.Bind("MemberInc", 1L)
                                            .Select<Member>(keepTrace);
                            String currentName = tra.Name;

                            {
                                // another box changes the name
                                MemberInc mm = new MemberInc();
                                mm.ID = 1;
                                mm.Name = "Kelly";
                                db.Get().Update("MemberInc", mm.ID, mm);
                            }

                            // will auto rollback
                            if (boxTracer.Commit().Equals(CommitResult.OK))
                            {
                                throw new Exception();
                            }
                            else
                            {
                                sb.Append("the name '" + currentName + "' is changed,");
                            }
                        }
                        Member nm = db.Get().SelectKey<Member>("MemberInc", 1L);
                        sb.Append("new name is '" + nm.Name + "'");
                    }
                }
            }
            catch (Exception ex)
            {
                sb.Append(ex.ToString());
            }
            return sb;
        }
    }
    #endregion

    #region Speed
    public class Speed
    {
        #region Main
        public static StringBuilder PMain(int threadCount)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("\r\n\r\n*SpeedTest");
            try
            {
                DBPlatform.DeleteDB();
                using (var server = new MyServer())
                {
                    using (var db = server.GetInstance(1))
                    {

                        var objectCount = 10;
                        sb.Append("\r\nBegin Insert " + (threadCount * objectCount).ToString("#,#"));

                        DDebug.StartWatch();
                        DBPlatform.For(0, threadCount,
                            (i) =>
                            {
                                using (var box = db.Cube())
                                {
                                    for (var o = 0; o < objectCount; o++)
                                    {
                                        var m = new Member
                                        {
                                            ID = box.NewId(0, 1),
                                            Name = i.ToString() + "_" + o.ToString(),
                                            Age = 1
                                        };
                                        box.Bind("TSpeed").Insert(m);
                                    }
                                    box.Commit().Assert();
                                }
                            }
                        );

                        var sec = DDebug.StopWatch().TotalSeconds;
                        var avg = (threadCount * objectCount) / sec;
                        sb.Append("\r\nElapsed " + sec + "s, AVG Insert " + avg.ToString("#,#") + " o/sec");

                        DDebug.StartWatch();
                        DBPlatform.For(0, threadCount,
                            (i) =>
                            {
                                using (var box = db.Cube())
                                {
                                    for (var o = 0; o < objectCount; o++)
                                    {
                                        long ID = i * objectCount + o + 1;
                                        var mem = box.Bind("TSpeed", ID).Select<Member>();
                                        if (mem.ID != ID) { throw new Exception(); }
                                    }
                                }
                            }
                        );

                        sec = DDebug.StopWatch().TotalSeconds;
                        avg = (threadCount * objectCount) / sec;
                        sb.Append("\r\nElapsed " + sec + "s, AVG Lookup " + avg.ToString("#,#") + " o/sec");

                        //Parallel Query Language
                        DDebug.StartWatch();
                        int count = 0;
                        DBPlatform.For(0, threadCount,
                            (i) =>
                            {
                                using (var box = db.Cube())
                                {
                                    var tspeed = box.Select<Member>("from TSpeed where ID>=? & ID<=?",
                                        (long)(i * objectCount + 1), (long)(i * objectCount + objectCount));
                                    foreach (var m in tspeed)
                                    {
                                        // age == 1
                                        Interlocked.Add(ref count, m.Age);
                                    }
                                }
                            }
                        );
                        if (count != (threadCount * objectCount)) { throw new Exception(count.ToString()); }

                        sec = DDebug.StopWatch().TotalSeconds;
                        avg = count / sec;
                        sb.Append("\r\nElapsed " + sec + "s, AVG Query " + avg.ToString("#,#") + " o/sec");
                    }
                }
            }
            catch (Exception ex)
            {
                sb.Append(ex.ToString());
            }
            return sb;

        }
        #endregion

        #region ReplicationSpeed

        public static StringBuilder ReplicationSpeed(int threadCount, int time)
        {
            StringBuilder sb = new StringBuilder();
            try
            {
                DBPlatform.DeleteDB();
                using (var server = new ReplicableServer())
                {
                    var masterA = server.GetInstance(ReplicableServer.MasterA_DBAddress);
                    var slaveA = server.GetInstance(ReplicableServer.SlaveA_DBAddress);
                    var masterB = server.GetInstance(ReplicableServer.MasterB_DBAddress);

                    var data = ((InMemoryBoxRecycler)masterA.GetBoxRecycler()).GetBoxDataAndClear();

                    BoxData.SlaveReplicate(slaveA, data).Assert();
                    BoxData.MasterReplicate(masterB, data).Assert();

                    var objectCount = 10;

                    double slaveSec = 0;
                    double masterSec = 0;
                    for (var t = 0; t < time; t++)
                    {
                        DBPlatform.For(0, threadCount,
                            (i) =>
                            {
                                using (var box = masterA.Cube(ReplicableServer.MasterB_DBAddress))
                                {
                                    for (var o = 0; o < objectCount; o++)
                                    {
                                        var m = new Member
                                        {
                                            ID = box.NewId(0, 1),
                                            Name = i.ToString() + "_" + o.ToString(),
                                            Age = 1
                                        };
                                        box.Bind("TSpeed").Insert(m);
                                    }
                                    box.Commit().Assert();
                                }
                            }
                        );


                        data = ((InMemoryBoxRecycler)masterA.GetBoxRecycler()).GetBoxDataAndClear();

                        DDebug.StartWatch();
                        BoxData.SlaveReplicate(slaveA, data).Assert();
                        slaveSec += DDebug.StopWatch().TotalSeconds;


                        DDebug.StartWatch();
                        BoxData.MasterReplicate(masterB, data).Assert();
                        masterSec += DDebug.StopWatch().TotalSeconds;

                    }
                    sb.Append("\r\n\r\n*Replicate " + (threadCount * time).ToString("#,#") + " transactions, totals " + (threadCount * objectCount * time).ToString("#,#") + " objects");
                    var avg = (threadCount * objectCount * time) / slaveSec;
                    sb.Append("\r\nSlaveReplicationSpeed " + slaveSec + "s, AVG " + avg.ToString("#,#") + " o/sec");

                    avg = (threadCount * objectCount * time) / masterSec;
                    sb.Append("\r\nMasterReplicationSpeed " + masterSec + "s, AVG " + avg.ToString("#,#") + " o/sec");

                    int count = 0;

                    DDebug.StartWatch();
                    for (var t = 0; t < time; t++)
                    {
                        DBPlatform.For(0, threadCount,
                            (i) =>
                            {
                                for (var dbc = 0; dbc < 2; dbc++)
                                {
                                    using (var box = dbc == 0 ? slaveA.Cube() : masterB.Cube())
                                    {
                                        for (var o = 0; o < objectCount; o++)
                                        {
                                            long ID = i * objectCount + o + 1;
                                            ID += (t * threadCount * objectCount);
                                            var mem = box.Bind("TSpeed", ID).Select<Member>();
                                            if (mem.ID != ID) { throw new Exception(); }
                                            Interlocked.Add(ref count, mem.Age);
                                        }
                                    }
                                }
                            }
                        );
                    }

                    if (count != (threadCount * objectCount * time * 2))
                    {
                        throw new Exception();
                    }
                    masterSec = DDebug.StopWatch().TotalSeconds;
                    avg = count / masterSec;
                    sb.Append("\r\nLookup just after replication " + masterSec + "s, AVG " + avg.ToString("#,#") + " o/sec");

                }
            }
            catch (Exception ex)
            {
                sb.Append(ex.ToString());
            }
            return sb;

        }
        #endregion

    }
    #endregion

}


#region iBoxDB.Example.Server
namespace iBoxDB.Example.Server
{
    using iBoxDB.LocalServer.IO;
    using iBoxDB.LocalServer.Replication;

    public abstract class IDClass
    {
        public long ID { get; set; }
    }

    public class Member : IDClass
    {
        public static readonly byte IncTableID = 0;

        public string Name { get; set; }

        public DateTime RegTime;

        public string[] Tags { get; set; }

        public decimal Amount { get; set; }

        public int Age;
    }

    public class MemberVIP : Member
    {
        public int VIP;
    }

    public class MemberInc : Member
    {
        // UpdateIncrement, type is long
        public long Version;
    }

    public class Product : Dictionary<string, object>
    {

        public int Type
        {
            get
            {
                return (int)this["Type"];
            }
            set
            {
                this["Type"] = value;
            }
        }

        public Guid UID
        {
            get
            {
                return (Guid)this["UID"];
            }
            set
            {
                this["UID"] = value;
            }
        }


        public string Name
        {
            get
            {
                return (string)this["Name"];
            }
            set
            {
                this["Name"] = value;
            }
        }
    }


    public class QueryArray : IFunction
    {
        string match;
        public QueryArray(string match)
        {
            this.match = match;
        }


        public object Execute(int argCount, object[] args)
        {
            var tags = (string[])args[0];
            if (tags == null) { return false; }
            foreach (var t in tags)
            {
                if (t == match)
                {
                    return true;
                }
            }
            return false;
        }
    }

    class MyConfig : DBPlatform.Config
    {
        public MyConfig()
            : base()
        {
            this.EnsureTable<Member>("Member", "ID");
            // StringColumnName(Length) , default length is 32
            this.EnsureIndex<Member>("Member", "Name(20)");
            //particular index for MemberVIP.VIP
            this.EnsureIndex<MemberVIP>("Member", "VIP");


            this.EnsureTable<Product>("Product", "Type", "UID");


            this.EnsureTable<Member>("TSpeed", "ID");


            this.EnsureTable<MemberInc>("MemberInc", "ID");
            //UpdateIncrement config
            this.EnsureUpdateIncrementIndex<MemberInc>("MemberInc", "Version");
        }
    }

    public class MyServer : LocalDatabaseServer
    {
        protected override DatabaseConfig BuildDatabaseConfig(long address)
        {
            return new MyConfig();
        }
    }


    public class ReplicableServer : LocalDatabaseServer
    {
        public const int MasterA_DBAddress = 10;
        public const int MasterB_DBAddress = 20;

        //  negative number
        public const int SlaveA_DBAddress = -10;


        protected override DatabaseConfig BuildDatabaseConfig(long name)
        {
            if (name == MasterB_DBAddress || name == MasterA_DBAddress)
            {
                return new MyConfig();
            }
            if (name == SlaveA_DBAddress)
            {
                return new DBPlatform.Config();
            }
            throw new NotImplementedException();
        }

        protected override IBoxRecycler BuildBoxRecycler(long name, DatabaseConfig config)
        {
            if (name == MasterA_DBAddress || name == MasterB_DBAddress)
            {
                return new InMemoryBoxRecycler(name, config);
            }
            return base.BuildBoxRecycler(name, config);
        }
    }

    class Package
    {
        public Socket Socket;
        public byte[] OutBox;
    }
    // store changes
    class InMemoryBoxRecycler : IBoxRecycler
    {
        List<Package> qBuffer;
        public InMemoryBoxRecycler(long name, DatabaseConfig config)
        {
            qBuffer = new List<Package>();
        }

        public void OnReceived(Socket socket, BoxData outBox, bool normal)
        {
            if (socket.DestAddress == long.MaxValue)
            {
                // default replicate address
                return;
            }
            lock (qBuffer)
            {
                qBuffer.Add(
                    new Package { Socket = socket, OutBox = outBox.ToBytes() }
                    );
            }
        }
        public List<Package> GetPackage()
        {
            return qBuffer;
        }
        public BoxData[] GetBoxDataAndClear()
        {
            lock (qBuffer)
            {
                List<BoxData> list = new List<BoxData>();
                foreach (var p in qBuffer)
                {
                    list.Add(BoxData.Create(p.OutBox));
                }
                qBuffer.Clear();
                return list.ToArray();
            }
        }

        public void Dispose()
        {
            qBuffer = null;
        }
    }



}
#endregion


#region DBPlatform
namespace iBoxDB
{

    public class DBPlatform
    {
        public static void SetStorage()
        {
            //DB.Root(path); DB Files Path
#if (WINDOWS_PHONE || UNITY_WP8) && (!UNITY_EDITOR)
            DB.Root(Windows.Storage.ApplicationData.Current.LocalFolder.Path);
#if (NETFX_CORE || UNITY_METRO) && (!UNITY_EDITOR)
            iBoxDB.WSDatabaseConfig.ResetStorage();
#else
            Xamarin_IL2CPP_MakeSureClassesAreLinked();
#endif
#endif

        }

        public delegate void FRun(int i);


#if WINDOWS_PHONE || NET2 || UNITY_EDITOR || UNITY_WP8 || UNITY_ANDROID
        public static void For(int start, int end, FRun run)
        {
            for (var i = start; i < end; i++)
            {
                run(i);
            }
        }
#else
        public static void For(int start, int end, FRun run)
        {
            System.Threading.Tasks.Parallel.For(start, end,
                          (i) =>
                          {
                              run(i);
                          });
        }
#endif

#if WINDOWS_PHONE || NET2 || UNITY_EDITOR || NETFX_CORE || UNITY_WP8 || UNITY_METRO || UNITY_ANDROID
        public static int Speed_ThreadCount = 200;
        public static int Replication_ThreadCount = 40;
        public static int Replication_Time = 4;
#else
        public static int Speed_ThreadCount = 20000;
        public static int Replication_ThreadCount = 200;
        public static int Replication_Time = 10;

#endif
        public class Config : BoxFileStreamConfig
        {
            public Config()
                : base()
            {
                // Memory Control, time memory tradeoff, minimum is 1
                //this.CacheLength = MB(512);

                //File Size, Minimum is 1
                //this.FileIncSize = (int)MB(4);

                //Thread
                //this.ReadStreamCount = 8;
            }
        }
        public static void DeleteDB()
        {
            DBDebug.DDebug.DeleteDBFiles(new long[] { 1, 10, 20, -10 });
        }
        private static void Xamarin_IL2CPP_MakeSureClassesAreLinked()
        {
#if !NETFX_CORE && !UNITY_METRO && !WINDOWS_PHONE && !UNITY_WP8 && !UNITY_EDITOR
            if (Object.ReferenceEquals(1, 2))
            {
                //ensure the methods 
                new System.IO.FileStream("", System.IO.FileMode.OpenOrCreate,
                   System.IO.FileAccess.ReadWrite, System.IO.FileShare.ReadWrite, 1);
            }
#endif
        }

    }

}
#endregion


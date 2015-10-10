using iBoxDB.DBDebug;
using iBoxDB.LocalServer.IO;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace iBoxDB.LocalServer.Replication
{
    //Phase 1, copy main file
    //Phase 2, copy changed data
    public class FileBackupBoxRecycler : IBoxRecycler2
    {
        volatile bool receiving = false;
        volatile List<BoxData> list;
        volatile Mirror mirror;

        public void Backup(IDatabase database, long backupAddress, string backuproot = null)
        {
            Phase1(database, backupAddress, backuproot);
            Phase2();
        }
        public void Phase1(IDatabase database, long backupAddress, string backuproot = null)
        {
            lock (this)
            {
                if (list != null || mirror != null) { throw new Exception(); }
                list = new List<BoxData>();
            }

            var bakConfig = new BoxFileStreamConfig();
            if (backuproot != null)
            {
                bakConfig.LocalRootPath = backuproot;
            }
            bakConfig.SwapFileBuffer = 0;
            //bakConfig.ReadStreamCount = 0;
            //bakConfig.FileIncSize = 0;
            mirror = new Mirror(bakConfig, backupAddress);

            using (var reader = database.CreateFileReader())
            {
                long pos = 0;
                long len = reader.Length;
                var buf = new byte[1024 * 1024 * 1];
                while (pos < len)
                {
                    int count = reader.Read(pos, buf, 0, buf.Length);
                    mirror.Write(pos, buf, 0, count);
                    pos += count;
                }
            }
        }
        public void Phase2()
        {
            while (receiving)
            {
                Thread.Sleep(10);
            }
            List<BoxData> backupList;
            lock (this)
            {
                backupList = list;
                list = null;
            }
            mirror.Write(backupList.ToArray());
            mirror.Dispose();
            mirror = null;
        }

        public void OnReceiving(Socket socket)
        {
            receiving = true;
        }
        public void OnReceived(Socket socket, BoxData outBox, bool normal)
        {
            lock (this)
            {
                if (list != null)
                {
                    list.Add(outBox);
                }
            }
        }
        public void OnFlushed(Socket socket)
        {
            receiving = false;
        }

        public void Dispose()
        {

        }
    }

    public class BackupTest
    {
        public class DBObject : IEquatable<DBObject>
        {
            public long ID;
            public String Value;
            public DateTime DT;

            public bool Equals(DBObject other)
            {
                return ID == other.ID && Value == other.Value && DT == other.DT;
            }

        }
        public static bool Test(bool background)
        {
            var bakAddr = 0 - Math.Abs(DateTime.Now.Ticks);

            DDebug.DeleteDBFiles(1);
            DB server = new DB(1);
            server.SetBoxRecycler(new FileBackupBoxRecycler());
            server.GetConfig().EnsureTable<DBObject>("DBObject", "ID");
            DB.AutoBox auto = server.Open();

            Parallel.For(0, 300, (i) =>
            {
                var obj = new DBObject();
                obj.ID = auto.NewId(0);
                obj.Value = "Value " + obj.ID;
                obj.DT = DateTime.Now;
                auto.Insert("DBObject", obj);
            });


            // Export 
            if (background)
            {
                Thread backupThread = new Thread(() =>
                {
                    ((FileBackupBoxRecycler)auto.GetDatabase().GetBoxRecycler()).Phase1(auto.GetDatabase(), bakAddr);
                });
                backupThread.Start();

                Parallel.For(0, 300, (i) =>
                {
                    var obj = new DBObject();
                    obj.ID = auto.NewId(0);
                    obj.Value = "Value " + obj.ID;
                    obj.DT = DateTime.Now;
                    auto.Insert("DBObject", obj);
                });

                backupThread.Join();
                ((FileBackupBoxRecycler)auto.GetDatabase().GetBoxRecycler()).Phase2();
            }
            else
            {
                ((FileBackupBoxRecycler)auto.GetDatabase().GetBoxRecycler()).Backup(auto.GetDatabase(), bakAddr);
            }


            //Import
            DB bakserver = new DB(bakAddr);
            bakserver.GetConfig().DBConfig.SwapFileBuffer = 0;
            DB.AutoBox bakauto = bakserver.Open();

            DBObject[] s1 = auto.Select<DBObject>("from DBObject").ToArray();
            DBObject[] s2 = bakauto.Select<DBObject>("from DBObject").ToArray();            

            server.Dispose();
            bakserver.Dispose();
            DDebug.DeleteDBFiles(bakAddr);
            return s1.SequenceEqual(s2);
        }
    }
}

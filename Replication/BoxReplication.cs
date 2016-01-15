using System;
using System.Collections.Generic;
using iBoxDB.LocalServer.Helper;
using iBoxDB.LocalServer.IO;

namespace iBoxDB.LocalServer.Replication
{

    public sealed class BoxReplication
    {

        public static IBox MasterReplicate(IDatabase self, params BoxData[] data)
        {
            return MasterReplicate(self, long.MaxValue, data);
        }
        public static IBox MasterReplicate(IDatabase self, long dest, params BoxData[] data)
        {
            if (data == null || data.Length < 1) { return null; }
            IBox box = null;

            foreach (var d in data)
            {
                var actions = d.GetActions();
                if (actions != null && actions.Count > 0)
                {
                    box = box == null ? self.Cube(dest) : box;
                    foreach (var e in actions)
                    {
                        MasterAction(e, (ILocalBox)box);
                    }
                }
            }
            return box;

        }

        private static bool MasterAction(BEntity op, ILocalBox box)
        {
            switch (op.ActionType)
            {
                case ActionType.Insert:
                    return box.Insert(op.TableName, op.Value, op.Length).Has();
                case ActionType.Delete:
                    return box.Delete(op.TableName, op.Key).Has();
                case ActionType.Update:
                    if (box.Update(op.TableName, op.Key, op.Value).Has())
                    {
                        return true;
                    }
                    else
                    {
                        return box.Insert(op.TableName, op.Value, op.Length).Has();
                    }
            }
            throw new Exception("");
        }


    }
}

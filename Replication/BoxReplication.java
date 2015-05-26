package iBoxDB.LocalServer.Replication;

import iBoxDB.LocalServer.ActionType;
import iBoxDB.LocalServer.BEntity;
import iBoxDB.LocalServer.Box;
import iBoxDB.LocalServer.Database;
import iBoxDB.LocalServer.LocalBox;

import java.util.ArrayList;

//BoxReplication.masterReplicate(masterA, new BoxData(p.OutBox)).commit().Assert();
public final class BoxReplication {

	public static Box masterReplicate(Database self, BoxData... data) {
		return masterReplicate(self, Long.MAX_VALUE, data);
	}

	public static Box masterReplicate(Database self, long dest, BoxData... data) {
		if (data == null || data.length < 1) {
			return null;
		}
		Box box = null;

		for (final BoxData d : data) {
			ArrayList<BEntity> actions = new ArrayList<BEntity>() {
				{
					for (BEntity b : d.getActions()) {
						this.add(b);
					}
				}
			};

			if (actions != null && actions.size() > 0) {
				box = box == null ? self.cube(dest) : box;

				for (BEntity e : actions) {
					MasterAction(e, (LocalBox) box);
				}
			}
		}
		return box;

	}

	private static boolean MasterAction(BEntity op, LocalBox box) {
		switch (op.ActionType.Ord) {
		case ActionType.Ord_Insert:
			return box.Insert(op.TableName, op.Value);
		case ActionType.Ord_Delete:
			return box.Delete(op.TableName, op.Key);
		case ActionType.Ord_Update:
			if (box.Update(op.TableName, op.Key, op.Value)) {
				return true;
			} else {
				return box.Insert(op.TableName, op.Value);
			}
		}
		throw new RuntimeException("");
	}

}

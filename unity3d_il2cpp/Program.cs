
using System;
using System.Collections;
using System.Collections.Generic;

using Mono.Cecil;

namespace CA
{
    class Program
    {
        //use .NET Framework 3.5 , Mono.Cecil 0.9.6 . for Unity3D IL2CPP beta
        static void Main(string[] args)
        {
            String path = @"C:\Users\Root\Downloads\iBoxDB_io_v22_181_m\Lib\NetDB\net2\iBoxDB.net2.dll";

            var asm = AssemblyDefinition.ReadAssembly(path);

            List<TypeDefinition> td = new List<TypeDefinition>();

            foreach (var t1 in asm.MainModule.Types)
            {
                td.Add(t1);
                foreach (var t2 in t1.NestedTypes)
                {
                    td.Add(t2);
                    foreach (var t3 in t2.NestedTypes)
                    {
                        td.Add(t3);
                        foreach (var t4 in t3.NestedTypes)
                        {
                            td.Add(t4);
                        }
                    }
                }
            }

            foreach (var t in td)
            {
                foreach (var i in t.Interfaces)
                {
                    if (i.Name.Contains("IEnumerable`1"))
                    {
                        t.Interfaces.Add(t.Module.Import(typeof(IEnumerable)));
                        break;
                    }
                    if (i.Name.Contains("IEnumerator`1"))
                    {
                        t.Interfaces.Add(t.Module.Import(typeof(IEnumerator)));
                        break;
                    }
                    if (i.Name.Contains("IBEnumerable`1"))
                    {
                        t.Interfaces.Add(t.Module.Import(typeof(IEnumerable)));
                        GenericInstanceType ng = new GenericInstanceType(t.Module.Import(typeof(IEnumerable<>)));
                        ng.GenericArguments.Add(((GenericInstanceType)i).GenericArguments[0]);
                        t.Interfaces.Add(ng);
                        break;
                    }
                }
            }

            var lt = asm.MainModule.GetType("iBoxDB.LocalServer.Local");
            lt.Interfaces.Add(lt.Module.Import(typeof(ICollection<KeyValuePair<string, object>>)));
            lt.Interfaces.Add(lt.Module.Import(typeof(IEnumerable<KeyValuePair<string, object>>)));
            lt.Interfaces.Add(lt.Module.Import(typeof(IEnumerable)));


            asm.Write(@"c:\Temp\iBoxDB.net2.dll");
        }
    }
}

### Java6+Jar to Java 9 Module Jar


#### Update Jar to module
Download [module-info.class](https://github.com/iboxdb/forjava/blob/master/java9/module-info.class)
```bush
$ jar --update --file iBoxDBv27.jar --module-version 2.7  module-info.class
```


#### Build a JRE, set launcher = newmodule/app.NewClass.Main() 
```bush
$jlink --module-path /home/user/jdk-9/jmods:/home/user/Downloads/iBoxDBv21600_27/JavaDB/iBoxDBv27.jar:. --add-modules java.base,iBoxDB,newmodule --launcher run=newmodule/app.NewClass   --output ujre
```

#### List the modules
```bush
$./ujre/bin/java --list-modules
```


#### Execute the App
```bush
$./ujre/bin/run
```
 
 
#### the App module
```java
module-info.java
module newmodule {
    requires iBoxDB;
    exports app;
}
```

#### Show Java Class
```sh
$javap module-info.class
```


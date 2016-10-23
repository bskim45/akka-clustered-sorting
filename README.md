Akka Clustered Sorting
==============
This was the part of POSTECH CSED332 Software Design Project (2015 Fall)
* Bumsoo Kim
* Juwon Gang
* Jiwon Choi

Goal
-----
Goal is to sort key/value records stored on disk.
* The input data is stored on multiple machines.
* The output data should be stored on multiple machines.

Distributed/Parallel sorting key/value records stored on multiple disks on multiple machines

Requirements
---
###Input
* Single master and a fixed number of multiple slaves with their IP addresses.
* Input blocks of 100MB each on multiple disks on each slaves.

###Output
* Sorted output blocks (of any size) on multiple disks on each slaves.

※ Generating input/output file uses `gensort` from http://www.ordinal.com/gensort.html.
Download proper gensort binary according to your platform, and use [`gen_partitions.py` python helper script](https://github.com/bskim45/akka-clustered-sorting/blob/master/gensort/gen_partitions.py) for easy input generation.

Challenge
-----
* The input data does not fit in memory! - Use disk-based merge sort
* The input/output data is stored on multiple machines. - Distributed sorting network
* Sort/Partition can proceed in parallel for each input block.
	* Disk may be busy - Allocate a fixed number of sort/partition threads because of disk I/O.

The complete overview
---
![](https://github.com/bskim45/akka-clustered-sorting/blob/master/arts/overview.png)

Development Environments
----
* **Scala** `2.11.7` or above
* **akka** `2.3.14` or above
* **sbt** `0.13.9` or above

Coding Standards
----
 * Tab width : 2 (use **spaces** instead of tabs)
 * Encoding : `UTF-8`
 * Line separator(newline character) : LF(`\n`)

How to use
----
### 0. Environment Setup
We use maximum `24G` heap per node according to project description. Also, we use port `5150` for Master-Slave connection and `5151` for Slave-Slave connection. Make sure that **no one is using port 5150 and 5151**.

### 1. Boot up nodes
Launch 1 master and than several slaves as you wish, with following usage.
Be aware of the launching order. You must **launch master first**.
Executables are located under `dists` folder under proejct root. If there's no build suit to you, please see [Build](#build) section to make you own. Of course, it's just fine to run with `sbt run` command.

#### Master
```
> master <# of slaves>
```
with sbt command:
```
> sbt "run <# of slaves>"
```
Pass # of slaves you want as an argument. See `--help` for more options.

### Slave
```
> slave <master IP:port> -I <input1>,<input2>,... -O <output>
```
with sbt command:
```
> sbt "run <master IP:port> -I <input1>,<input2>,... -O <output>"
```
*Note that there's no space between commas.*  See `--help` for more options.

### 2. Establish connection
Slaves will automatically try to connect to the master of given address.
After # of slaves are successfully connected to the master, master will start to give orders to slaves.

### 3. Validating Results
Merged output files are located at `<output>` directory that you've stated above on each slaves. Outputs are sliced into 32MB(32,000,000 btyes/ 320,000 records) chunks, ordered by part number, starting from **0**. For instance, `partition_1` is second part of merged file.

Build
----
With the help of [sbt-assembly](https://github.com/sbt/sbt-assembly) and [sbt-native-packager](https://github.com/sbt/sbt-native-packager), you can build executive binary yourself.

Run the following commands on each project folder(Slave, Master), where `build.sbt` is located.

#### Fat JAR
To build entire project in one fat-jar, use:
~~~
> sbt assembly
~~~

#### Universal
By using [sbt-native-packager](https://github.com/sbt/sbt-native-packager), you can build not only multi-platform executables, but also platform-dependent executables such as Windows `.msi`.

To build `.zip` format for all systems, type:
~~~
> sbt universal:packageBin
~~~
If it worked properly, two folders `bin`, `lib` will be created. You can find your executive inside of `bin`. sbt task `universal:packageBin` is one of packaging formats supported by [sbt-native-packager](https://github.com/sbt/sbt-native-packager). [See here](http://www.scala-sbt.org/sbt-native-packager/gettingstarted.html) for more platform dependent packaging formats.

LICENSE
---
Apache License 2.0
Copyright (c) 2015 Bumsoo Kim (http://bsk.im), Juwon Gang, Jiwon Choi
See [LICENSE](https://github.com/bskim45/akka-clustered-sorting/blob/master/LICENSE) for details.
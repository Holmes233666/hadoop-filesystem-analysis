# Hadoop文件系统源码剖析

## 〇、前言

该项目目的为通过分析Hadoop 底层文件系统的支持机制，考虑向其中加入对 windows 文件系统的支持，实现 XdFileSystem（西电文件系统） 。

## 一、环境搭建和源代码调试

### 运行环境

#### 操作系统

+ 源码阅读环境是ubuntu 18.0或MacOS 12.4，二者都是Unix like的操作系统，都可以完美运行
+ 最终编写的支持Windows文件系统的`XidianFileSystem`的运行环境是Windows 7

#### hadoop版本选取

+ 源码阅读和分析：是``hadoop2.7.3`。
+ 最终的`XidianFileSystem`分别在`hadoop2.0.0-alpha`和`hadoop2.7.3`上均进行了测试，都可以运行成功

#### JDK版本

Oracle 1.7.0_21

对于上述版本的hadoop，1.7在各平台（MacOS, Linux和Windows）均能运行

#### IDE

Jetbrain IDEA 2021.3以上

目前主流IDE是idea，从实际使用来看idea使用起来也更加便捷高效，功能也更多。

### 获取源码

#### fork源码

浏览器访问https://github.com/apache/hadoop，点击`fork`，fork到个人账号方便之后编写注释，额外编写程序的等需求。

<img src="https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/05/30/3430606e195be7809cdccea7b0905dc3-image-20220530223357722-095ad6.png" alt="image-20220530223357722" style="zoom: 15%;" />

#### clone到本机

```shell
git clone git@github.com:<username>/<repo>.git
```

#### 直接用idea打开

<img src="https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/05/30/c3a90440becc86186cd7a3c8d2229f7b-image-20220530230247530-168b82.png" alt="image-20220530230247530" style="zoom:20%;" />

点击左下角选择hadoop版本，之后idea会基于maven自动进行配置和索引。这时进行全局搜索，类型跳转会更加便捷。

## 二、调试方法

调试对hadoop运行流程和核心思想理解有重要意义

有两种调试方法

1. 直接调试hadoop shell命令
2. 编写程序调试

### 调试hadoop shell命令

hadoop shell命令例子如下

```shell
hadoop fs ls file:///path/to/directory
```

<img src="https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/05/31/8648caa3c5e29e2288047edb6faab2ee-image-20220531114808056-0347e0.png" alt="image-20220531114808056" style="zoom:40%;" />

该命令等同于本地运行

通过hadoop配置，可以对上述命令进行远程调试。我们调试的主要思路是

1. 运行命令，

#### 下载代码发行包

下载链接：https://archive.apache.org/dist/hadoop/common/hadoop-2.7.3/hadoop-2.7.3.tar.gz

#### 配置文件修改

配置好环境变量

> linux中环境变量的设置方法这里不细说

```sql
# JAVA_HOME真针对本机情况设置
export JAVA_HOME="/usr/lib/jvm/java-1.7.0-openjdk-amd64"
export HADOOP_HOME="/home/parallels/Desktop/hadoop-2.7.3"
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib"
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
```

设置`core-site.xml`

```shell
cd $HADOOP_HOME
vim etc/hadoop/core-site.xml
```

倒数第二行添加

```xml
<property>
  <name>fs.defaultFS</name>
  <!-- 下面的file://表示本地文件系统 -->
  <value>file:///</value>
</property>
```

截图

<img src="https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/05/31/1ec21655fc4ee44b4ac9aab20e24891e-1652268544418-0d2274f5-9293-46f8-9f89-01a716b7966c-1e22e5.png" alt="img" style="zoom:50%;" />

测试运行

```shell
hadoop fs -ls /
```

![shell流程](https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/06/01/c6ea967ee4a166068418a5ede037e48c-shell%E6%B5%81%E7%A8%8B-c2e579.png)

该命名等同于命令行直接输入

```shell
ls /
```

![img](https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/05/31/0f715369596db596fb138e1ac99cb0a7-1652268668806-7dc9c172-2fba-48b2-9018-702bcda2456b-a1b918.png)

第一行有一个`WARN`，忽略即可

#### 开始调试

下面的调试建议在本机进行，这样idea快一些，虚拟机里慢

也可以虚拟机里进行

命令行输入（其实是设置环境变量，不在命令行输入而是直接设置为永久的环境变量或者将下面的命令添加到`etc/hadoop/hadoop-env.sh`中的任一位置都可以）

```shell
export HADOOP_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=8888"
```

然后再命令行输入

```shell
hadoop fs -ls /
```

![img](https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/05/31/7f896bb3d28ed4a3565f499283b8f1fe-1652268834098-18dcd4c2-4eda-4727-9c0f-5dd26d3a78fe-b69ff8.png)

看到该结果进行下一步

#### IDEA打开源码

这个IDEA可以在虚拟机或本机打开。（或者任何可以远程访问hadoop所运行主机的主机）

用IDEA打开（不用进行设置，不用操作maven）

点击`Edit configuration`

![img](https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/05/31/e5d0f94a7843dda7fa2123bbda76f822-1652268962235-fe417feb-9bb1-4ae6-a2dc-75acccfe03a9-e96a99.png)

![img](https://cdn.nlark.com/yuque/0/2022/png/1374390/1652268993341-438f564a-0e6e-4268-aaae-965ee8860d8d.png)

设置host和port

>  虚拟机里运行`ifconfig -a`能看到host<img src="https://cdn.nlark.com/yuque/0/2022/png/1374390/1652269129812-c2e52436-246a-4566-931e-4b9cd9dc2874.png" alt="img" style="zoom:50%;" />

如下图所示

![img](https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/05/31/6e74b7a45af8fbdff5f17fd7c66ef8f7-1652269018419-42e177fd-e426-4610-bbb5-85de7ca81054-273811.png)

最后电机右下角保存

然后可以开始Debug了

#### 开始

提前加一下断点，`org/apache/hadoop/fs/FsShell.java:340`

![img](https://cdn.nlark.com/yuque/0/2022/png/1374390/1652269200917-95dfbb9f-4140-41e9-a71c-8adf795d8557.png)

点击debug按钮，开始调试

![img](https://cdn.nlark.com/yuque/0/2022/png/1374390/1652269173308-d0f63a89-2905-40af-9981-d88b6b5af6c0.png)

### 编写代码调试

该部分参考《Hadoop权威指南 第四版》

#### 新建项目，导入依赖

在idea中新建project，在`pom.xml`中添加依赖

```xml
<dependency>
  <groupId>org.apache.hadoop</groupId>
  <artifactId>hadoop-common</artifactId>
  <version>2.7.3</version>
</dependency>
```

#### 编写代码

下面编写一个示例代码，改代码会使用hadoop filesystem读取本地文件内容，并输出到控制台

```java
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileSystemCat {

  public static void main(String[] args) throws Exception {
    String uri = args[0];
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    InputStream in = null;
    try {
      in = fs.open(new Path(uri));
      IOUtils.copyBytes(in, System.out, 4096, false);
    } finally {
      IOUtils.closeStream(in);
    }
  }
}
```

> TODO: 截图输出

#### 调试程序

对上述程序加断点即可进行调试，通过step into即可观察hadoop文件系统运行流程。

## 三、文件系统概述

Hadoop文件系统包括Hadoop抽象文件系统以及基于该抽象文件系统的大量具体文件系统，满足了构建在Hadoop上的各类应用的各种数据访问需求。

### 文件系统概述

#### 文件系统

最初的文件系统用于解决信息的长期存储，并达到如下要求

+ 能够存储大量信息
+ 使用信息的应用终止时，信息必须保存下载
+ 多个应用可以并发地存储信息

这些问题的通常方法，是把信息以一种单元，即“文件（file）”的形式存储在磁盘或其他外部介质上。一个文件时一个命名的、存储在设备上的信息的线性字节流。文件在需要的时候可以读取这些信息或者写入新的信息。存储在文件中的信息必须是永久的。对文件的管理，包括了文件的结构以及命名、存取、使用、保护和实现，称为文件系统。

#### 文件系统的用户界面

用户角度的文件系统主要包含一下两个方面

1. 文件是如何命名的，文件有哪些操作
2. 目录和目录树，目录有哪些操作

对于用户来说，文件作为一种抽象机制，最重要的特征就是命名方法。

+ 大部分的文件系统不关心文件里保存的数据，把文件内容作为无结构的字节序列保存。
+ 也有一些文件系统支持结构化文件，以记录为单位组织信息，免去文件系统使用者将“原始的”字节流转换成记录流的麻烦。
+ **元数据**：比如文件的创建日期、文件长度、创建信息、引用计数等

对用用户来说，文件有许多种操作包括创建文件、删除文件、打开文件、读文件、写文件等。

**目录和目录树**

此外，目录也是文件系统中另一个重要的概念。一个 “目录”或 “文件夹” 就是一个虚拟容器, 它里面保存着一组文件和其他一些目录。一个典型的文件系统可能会包含成千上万的目录, 多个文件通过存储在一个目录中, 可以达到有组织地存储文件的目的。同时, 在一个目录中 可以保持另外的目录 (称为子目录), 这些目录和文件构成了一个层次结构 (即目录树)。
使用目录树组织文件系统时, 需要通过某种方法指明文件名。路径名描述了怎样在一个 文件系统中确认一个文件的位置, 它是一个分量名序列, 各分量名之间用分隔符。

文件系统中用于管理目录的系统调用差别比较大，下面是UNIX文件系统的例子

创建目录、删除目录、打开目录、关闭目录、读目录和重命名目录等

#### 文件系统的实现

上面小节中我们讨论了用户角度的文件系统，用户关心的是文件如何命名，可以进行哪些操作，以及目录树的操作。

而实现者感兴趣的是文件和目录如何存储，存储空间怎么管理和系统如何有效可靠地工作。

## 四、Hadoop文件系统架构

### 架构图

<img src="https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/06/01/826761c600c43991d064e07bdaf8e768-image-20220601003250876-b609df.png" alt="image-20220601003250876" style="zoom:30%;" />

上图是我们总结的Hadoop文件系统的架构图，接下来我们自下而上分别解读各部分的含义，同时，下面出现的各种名词也将是后续内容的重要名词。

### 底层（文件）系统

**底层文件系统**，数据实际存储的文件系统，一般指我们熟知的 ntfs、 ext4等本地文件系统，以及一些**远程存储系统**，比如FTP、阿里云对象存储、AWS S3。

### Hadoop抽象文件系统和具体文件系统

在讲解**具体文件系统**之前，还要讲解Hadoop抽象文件系统的概述。

#### Hadoop抽象文件系统

为了提供对不同数据访问的一致接口，Hadoop借鉴了Linux虚拟文件系统（Virtual File System）的概念，引入了Hadoop抽象文件系统

<img src="https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/06/01/f8a7bbc5b6972ea30336167081060e66-f8a7bbc5b6972ea30336167081060e66-os3-e09110-d2225a.png" alt="Virtual File Systems – CHAPTER 11 FILE-SYSTEM IMPLEMENTATION" style="zoom:50%;" />

并在Hadoop抽象文件系统基础上，提供了大量的**具体文件系统**的实现（这里联系第三部分中提到的文件系统的实现），满足构建于Hadoop上应用的各种数据访问。

例如，通过hadoop抽象文件系统，MapReduce可以运行在HDFS（hadoop distrubuetd filesystem）上，也可以运行在基于Amazon S3的 S3FileSystem上。

#### 具体文件系统（hadoop兼容文件系统）

Java原生文件API提供了`java.io.FileSystem`，它是`java.io`私有的抽象类，不能被包以外的类引用。也就是说不能像类似于UnixFileSystem或Win32FileSystem那样，通过继承`java.io.FileSystem`，实现一个与Java文件系统兼容的又能访问**底层（文件）系统**的子类。

为了解决这个问题，Hadoop需要自己实现一个抽象的`FileSystem`类。即`org.hadoop.fs.FileSystem`类，接下来出现的其他以FileSystem结尾的类都是它的**子类**，也就是具体文件系统。他们共同遵循一个规范（这个规范基本类似于`POSIX filesystem`）。这些具体文件系统内部可能有**不同的架构**，但最后都会暴露为若干各个文件操作接口比如

![image-20220601005757061](https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/06/01/b4a2dde42eee2963f4272f93a2bbaf2b-image-20220601005757061-9616ce.png)

> 注意，从这里可以看出 HDFS 只是`org.hadoop.fs.FileSystem`的一个实现即`DistributedFileSystem`，也就是一种具体文件系统。
>
> 本报告下面提到的FileSystem将特指`org.hadoop.fs.FileSystem`或者说Hadoop抽象文件系统

### 操作层

hadoop抽象文件系统对外提供了对不同数据访问的一致接口，它们与Linux和Java的文件API类似，主要可以分为两个部分

1. 处理文件和目录相关事务
   1. mkdir
   2. rename
   3. delete
   4. 等
2. 读写文件数据
   1. read
   2. write
   3. create
   4. seek 改变文件读写位置，对应Linux的lseek操作
   5. close

这两个部分共同组成操作层。通过这些操作，上层应用可以访问不同的数据（或者说不同的文件系统），它们实际使用的是具体文件系统的实例。这种结构仍然非常类似于VFS。

此外，文件的读写都是通过数据流实现的。文件信息的管理和权限控制则与具体文件系统对应的底层（文件）系统的实际架构和实现密切相关。

### 业务层（或应用层）

这一层会通过操作层提供的数据访问接口，对数据进行读写、监控等操作。除了图中展示的，命令行中常用的`ls`，`rm`，`mkdir`等，也属于这一层。

### 调用层

调用下层的方式有两种

1. shell：指令形如`hadoop [--config confdir] [COMMAND | CLASSNAME]`
   1. --config指定配置文件目录
   2. COMMAND则对应不同的命令类型，比如`fs`、`jar <jar>`等
2. API：即第一部分提到的远程调试中的那种程序

如果希望运行用户自定义程序（比如FileSystemCat），上述两种方法都可以，只需要将程序打为jar包放到hadoop发行包中的默认位置，就可以使用shell调用：

`hadoop FileSystemCat  file:///test/data.txt`

![image-20220608212231677](https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/06/08/fd9c171a92749acdff890ebae1f3ac5c-image-20220608212231677-a93b14.png)

## 五、Hadoop抽象文件系统

### 概述

`java.io.FileSystem`时`java.io`包的私有抽象类，不能被包以外的类引用，也就是说不能通过继承`java.io.FileSystem`实现一个和`Java`文件系统兼容的，又能访问如`HDFS`的子类。

为了提供对不同数据访问的一致接口，`Hadoop`借鉴了`Linux`虚拟文件系统(VFS)的概念，引入了`Hadoop`抽象文件系统，并在`Hadoop`抽象文件系统的基础上，提供了大量的具体文件系统的实现，例如`LocalFileSystem`和`DistributedFileSystem`，分别实现了本地文件系统和分布式文件系统，满足构建于`Hadoop`上各种应用的数据访问需求。`Hadoop`抽象文件系统实现在`org.apache.hadoop.fs`包中。

与`Linux`和`Java`文件API类似，`Hadoop`抽象文件系统的方法可以分为两部分：一部分用于处理文件和目录的相关事务；另一部分用于读写文件数据。下表总结了`Hadoop`抽象文件系统的文件操作与`Java`、`Linux`的对应关系：

| HadoopFileSystem操作                                         | Java操作                                | Linux操作    | 描述                           |
| :----------------------------------------------------------- | --------------------------------------- | ------------ | ------------------------------ |
| URL.openStream<br>FileSystem.open<br>FileSystem.create<br>FIleSystem.append | URL.openStream                          | open         | 打开一个文件                   |
| FSDataInputStream.read                                       | InputStream.read                        | read         | 读取文件中包含的数据           |
| FSDataOutputStream.write                                     | OutputStream.write                      | write        | 向文件中写入数据               |
| FSDataOutputStream.close<br>FSDataInputStream.close          | OutputStream.close<br>InputStream.close | close        | 关闭一个文件                   |
| FSDataInputStream.seek                                       | RandomAccessFile.seek                   | lseek        | 改变文件读写位置               |
| FileSystem.getFileStatus<br>FileSystem.get*                  | File.get*                               | stat         | 获取文件/目录属性              |
| FileSystem.set*                                              | FileSystem.set*                         | chmod等      | 修改文件属性                   |
| FileSystem.createNewFile                                     | File.createNewFile                      | create       | 创建一个文件                   |
| FileSystem.delete                                            | File.delete                             | remove       | 从文件系统总删除一个文件       |
| FileSystem.rename                                            | File.renameTo                           | rename       | 更换文件/目录名                |
| FileSystem.mkdirs                                            | File.mkdir                              | mkdir        | 在给定目录下创建一个子目录     |
| FileSystem.delete                                            | File.delete                             | rmdir        | 从一个目录中删除一个空的子目录 |
| FileSystem.listStatus                                        | File.list                               | readdir      | 读取一个目录下的项目           |
| FileSystem.setWorkingDirectory                               |                                         | getcwd/getwd | 返回当前工作目录               |
| FileSystem.setWorkingDirectory                               |                                         | chdir        | 更改当前工作目录               |

### `Configured`基类和`Closeable`接口

FileSystem抽象类继承了Configured基类，并实现了Closeable接口。Configured基类的源码如下：

``` java
// Configured类实现实现了Configurable接口
public class Configured implements Configurable {
    // 类中唯一的Configuration类对象
    private Configuration conf;

    // 构造器
    public Configured() {
        this((Configuration)null);
    }

    // 拷贝构造器
    public Configured(Configuration conf) {
        this.setConf(conf);
    }

    // setter，实现了Configurable接口中的方法
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    //getter， 实现了Configurable接口中的方法
    public Configuration getConf() {
        return this.conf;
    }
}
```

可以发现，Configured类是实现了Configurable接口，该接口的实现如下：

```java
public interface Configurable {
    // setter
    void setConf(Configuration var1);

    // getter
    Configuration getConf();
}
```

综上两点分析得知，Configured基类的作用是提供了访问配置文件的方法，配置即为Configuration类对象。

Closeable接口的实现如下：

```java
public interface Closeable {
    // close()方法
    public void close() throws IOException;
}
```

该接口仅仅是提供了FileSystem关闭文件时的操作函数接口。

#### URI详解

`URI(Uniform Resource Identifier，统一资源标识符)`，以特定的语法标识一个资源的字符串。所标识的资源可以是文件，也可以是电子邮件地址、一本书或者其他东西。

绝对`URI`由URI模式和模式特有部分 组成，由冒号隔开，格式为` [<scheme>:]<scheme-spefific-part>[#<fragment>]`。

常用的模式有：

| 模式   | 标识                             |
| ------ | -------------------------------- |
| file   | 本地磁盘上的文件                 |
| http   | 使用超文本传输协议的万维网服务器 |
| ftp    | FTP服务器                        |
| mailto | 电子邮件地址                     |
| telnet | 与基于Telnet协议的服务的连接     |

URI的模式特有部分无特定的语法，但时很多模式都定义了`层次式`的模式特有部分：`[//<authority>]<path>[?<query>]`

大部分的URI使用Intenet主机作为授权机构，这种情况下授权机构部分还可以包括可选的用户名和端口：`[<userInfo@>]<host>[:<port>]`

Java中`java.net.URI`类是对URI的抽象，提供了若该构造函数创建URI对象。可以通过URI的`get*()`方法获得URI相应的各部分。

有两种类型的URI：

1. `URL(Uniform Resource Locator，统一资源定位符)`：指向Internet上位于某个位置的资源
2. `URN(Uniform Resource Name，统一资源名)`：标识资源本身，不关注资源的位置，也不限于表示Internet资源



### Path类

`FileSystem`使用`Path`类对路径进行解析，将参数转化为标准的`URL`格式，对`Path`的参数做判断，标准化，字符串等操作。它在`java.net.URL`的基础上抽象了文件系统的路径。`Path`类的实现如下：

```java
public class Path implements Comparable {
    // Path字符串使用slash作为目录分隔符，一个目录若以slash开头，则它是一个绝对路径
    public static final String SEPARATOR = "/";
    public static final char SEPARATOR_CHAR = '/';
    // 当前路径为"."
    public static final String CUR_DIR = ".";
    // 是否为windows
    public static final boolean WINDOWS = System.getProperty("os.name").startsWith("Windows");
    private static final Pattern hasUriScheme = Pattern.compile("^[a-zA-Z][a-zA-Z0-9+-.]+:");
    private static final Pattern hasDriveLetterSpecifier = Pattern.compile("^/?[a-zA-Z]:");
    // 定义了Paht类私有变量url，路径以url的格式表示，其后对Path的操作主要通过对url的操作实现
    private URI uri;
    
    ...
}
```

其类图具体如下：

<img src="https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/06/01/34f961a3a9205c45ede675ee978f62e6-image-20220517230230871-653ce1.png" alt="image-20220517230230871" style="zoom: 30%;" />



### FileStatus类

任何文件系统的一个重要特征是定位其目录结构及检索其所处的文件和目录的能力。`Hadoop`中实现了一个叫做`FileStatus`的类，它封装了文件系统中文件和目录的元数据，包括文件长度、块大小、副本、修改时间、所有者以及许可信息等。`FileStatus`的类图如图所示：

<img src="https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/06/01/b95fadfff1b54d44ca980ff2b276d619-1-55d898.png" style="zoom:60%;" />

可以看到，`FileStatus`实现了`Writeable`接口，可以被序列化后在网络上传输。同时，一次性将文件的所有属性读出并返回到客户端，可以减少在分布式系统进行网络传输的次数。`Writeable`接口的实现如下：

```java
public interface Writable {
    void write(DataOutput var1) throws IOException;

    void readFields(DataInput var1) throws IOException;
}
```

`FileSystem`可以通过`getFileStatus()`获得一个文件或目录的状态对象。其抽象方法实现为：

```java
public abstract FileStatus getFileStatus(Path var1) throws IOException;
```

为了能够列出目录的内容，`FileSystem`也提供了`listStatus()`方法：

```java
// var1即为待获取目录内容的路径
public abstract FileStatus[] listStatus(Path var1) throws FileNotFoundException, IOException;

// 输入路径f和路径过滤器
public FileStatus[] listStatus(Path f, PathFilter filter) throws FileNotFoundException, IOException {
    // 首先将结果以ArrayList数据结构进行存储
    ArrayList<FileStatus> results = new ArrayList();
    // 调用私有同名方法，将放回的结构存入results中
    this.listStatus(results, f, filter);
    // 将结果转化为数组形式并返回
    return (FileStatus[])results.toArray(new FileStatus[results.size()]);
}

// 与上一个方法相似，只不过这次接受一个Path类型的数组，获取多个目录内容的状态
public FileStatus[] listStatus(Path[] files, PathFilter filter) throws FileNotFoundException, IOException {
    // 首先将结果以ArrayList数据结构进行存储
    ArrayList<FileStatus> results = new ArrayList();

    for(int i = 0; i < files.length; ++i) {
        // 对每一个文件都调用私有同名方法，并结果不断地加入result中
        this.listStatus(results, files[i], filter);
    }
	// 将结果转化为数组形式返回
    return (FileStatus[])results.toArray(new FileStatus[results.size()]);
}

// 调用上一个方法，只是将filter设为DEFAULT_FILTER
public FileStatus[] listStatus(Path[] files) throws FileNotFoundException, IOException {
    return this.listStatus(files, DEFAULT_FILTER);
}

// 同名私有方法
private void listStatus(ArrayList<FileStatus> results, Path f, PathFilter filter) throws FileNotFoundException, IOException {
    // 调用抽象函数，获取目录f的内容
    FileStatus[] listing = this.listStatus(f);
    if (listing == null) {
        throw new IOException("Error accessing " + f);
    } else {
        for(int i = 0; i < listing.length; ++i) {
            // 若filter接受该路径，则该信息被添加至results中
            if (filter.accept(listing[i].getPath())) {
                results.add(listing[i]);
            }
        }
    }
}
```

其中，`PathFilter`是一个接口，使得我们能够通过编程方式控制匹配，其内容如下：

``` java
public interface PathFilter {
    // 输入一个路径var1返回一个bool值决定是否接受该路径，起到过滤器的作用
    boolean accept(Path var1);
}
```

`DEFAULT_FILTER`是默认的`PathFilter`，其被定义为

```java
// 默认的PathFilter，总是返回true
private static final PathFilter DEFAULT_FILTER = new PathFilter() {
    public boolean accept(Path file) {
        return true;
    }
};
```

当传入`listStauts()`方法中的参数是一个文件时，它会简单地返回长度为1的`FileStatus`对象的一个数组。当传入参数是一个目录时，它会返回0或者多个`FileStatus`对象，代表着此目录所包含文件和目录。

### `FileSystem`抽象类的内部类和属性

`org.apache.hadoop.fs `包中的`FileSystem `抽象类和`AbstractFileSystem `抽象类作为抽象文件系统的基类，提供了基本的抽象操作，在这两个基类的基础上形成了两个类派生多个子类的层次结构。

下面是`FileSystem`抽象类的内部类。![image-20220517161748398](https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/06/01/04342a44d597e32e58d281107796f03d-image-20220517161748398-b67c26.png)

`FileSystem`的类图见下图。由此可见`FileSystem`是一个很大的抽象类。在`fs`包中，最重要的可以说是`FileSystem`抽象类。定义了文件系统中涉及的一些基本操作，如：`open`, `create`, `rename`, `delete`等，另外还有一些分布式文件系统具有的操作：`copyFromLocal`， `copyToLocalFile`等。`LocalFileSystem`和`DistributeFileSystem`类继承于此类，分别实现了本地文件系统和分布式文件系统。

<img src="https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/06/01/d6c26af4c4883457456bcbeea9297d51-image-20220517151123394-bb8ac2.png" alt="image-20220517151123394" style="zoom: 25%;" />

#### `get`方法获取文件系统

`FileSystem `是一个普通的文件系统API，所以首要任务是检索我们要用的文件系统实例。取得`FileSystem `实例有三种静态方法`get()`。这些`get()`方法是`Hadoop 0.21 `版本之前就存在的，但在0.21 版本中又添加了与之功能相同的三个方法`newInstance()`。

```java
  /**
   * Get a filesystem instance based on the uri, the passed
   * configuration and the user
   * @param uri of the filesystem
   * @param conf the configuration to use
   * @param user to perform the get as
   * @return the filesystem instance
   * @throws IOException
   * @throws InterruptedException
   */
  public static FileSystem get(final URI uri, final Configuration conf,
        final String user) throws IOException, InterruptedException {
    String ticketCachePath =
      conf.get(CommonConfigurationKeys.KERBEROS_TICKET_CACHE_PATH);
    UserGroupInformation ugi =
        UserGroupInformation.getBestUGI(ticketCachePath, user);
    return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
      @Override
      public FileSystem run() throws IOException {
        return get(uri, conf);
      }
    });
  }
```



```java
  /**
   * Returns the configured filesystem implementation.
   * @param conf the configuration to use
   */
  public static FileSystem get(Configuration conf) throws IOException {
    return get(getDefaultUri(conf), conf);
  }
```



```java
  /** @deprecated call #get(URI,Configuration) instead. */
  @Deprecated
  public static FileSystem getNamed(String name, Configuration conf)
    throws IOException {
    return get(URI.create(fixName(name)), conf);
  }
```



```java
  /** Returns the FileSystem for this URI's scheme and authority.  The scheme
   * of the URI determines a configuration property name,
   * <tt>fs.<i>scheme</i>.class</tt> whose value names the FileSystem class.
   * The entire URI is passed to the FileSystem instance's initialize method.
   */
  public static FileSystem get(URI uri, Configuration conf) throws IOException {
    String scheme = uri.getScheme();
    String authority = uri.getAuthority();

    if (scheme == null && authority == null) {     // use default FS
      return get(conf);
    }

    if (scheme != null && authority == null) {     // no authority
      URI defaultUri = getDefaultUri(conf);
      if (scheme.equals(defaultUri.getScheme())    // if scheme matches default
          && defaultUri.getAuthority() != null) {  // & default has authority
        return get(defaultUri, conf);              // return default
      }
    }
    
    String disableCacheName = String.format("fs.%s.impl.disable.cache", scheme);
    if (conf.getBoolean(disableCacheName, false)) {
      return createFileSystem(uri, conf);
    }

    return CACHE.get(uri, conf);
  }
```

#### `close`方法关闭文件系统

有两个方法来关闭文件系统。`closeAll()`用于关闭所有的文件系统，而`close()`只是关闭当前调用该方法的文件系统。

```java
  /**
   * Close all cached filesystems. Be sure those filesystems are not
   * used anymore.
   * 
   * @throws IOException
   */
  public static void closeAll() throws IOException {
    CACHE.closeAll();
  }
```

- 在确保文件系统没有在被其他地方用之后，关闭缓存中的文件系统

```java
  /**
   * No more filesystem operations are needed.  Will
   * release any held locks.
   */
  @Override
  public void close() throws IOException {
    // delete all files that were marked as delete-on-exit.
    processDeleteOnExit();
    CACHE.remove(this.key, this);
  }
```

- 删除那些标志为`delete-on-exit`的文件

#### `open`方法读取数据

数据的读取就像`Java `中的`io `一样，首先要获得一个输入流。输入流是通过`open() `方法获得的。抽象方法`open() `将由各个不同的文件系统重写。

抽象方法：

```java
  /**
   * The specification of this method matches that of
   * {@link FileContext#open(Path, int)} except that Path f must be for this
   * file system.
   */
  public abstract FSDataInputStream open(final Path f, int bufferSize)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException;
```

- 在指示的`Path`处打开一个`FSDataInputStream`，并指定`buffer`大小

实例方法：

```java
  /**
   * Opens an FSDataInputStream at the indicated Path.
   * @param f the file to open
   */
  public FSDataInputStream open(Path f) throws IOException {
    return open(f, getConf().getInt("io.file.buffer.size", 4096));
  }
```

- 调用抽象方法在指示的`Path`处打开一个`FSDataInputStream`，默认`buffer`大小为`4096`字节

#### `getUri`方法

```java
  /** Returns a URI whose scheme and authority identify this FileSystem.*/
  public abstract URI getUri();
```

- 抽象方法：返回一个模式和权限唯一标识此文件系统的`URI`

#### `create`方法写入数据

​	向文件系统中写入数据，或新建一个文件，需要获得一个用来写的输出流，这可以通过以下的多个`create()`方法来实现。这些重载的方法允许我们指定是否强制覆盖已有的文件、文件副本数量、写入文件时的缓冲大小、文件块大小以及文件许可等等。

使用写进程在指定的`Path`获得一个用来写的输出流，以向文件系统中写入数据，或新建一个文件

抽象方法：

```java
  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * @param f the file name to open
   * @param permission
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize
   * @param progress
   * @throws IOException
   * @see #setPermission(Path, FsPermission)
   */
  public abstract FSDataOutputStream create(Path f,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress) throws IOException;
```

- 抽象方法：必须指定以下七个参数
  - 文件路径
  - 文件许可
  - 是否覆盖已有的文件，若文件已存在且该参数为`false`则抛出错误
  - 用到的buffer的大小
  - 对文件进行块复制
  - 文件块大小
  - 用于传递回调接口的重载方法Progressable，可以告知数据写入数据节点的进度

下面是调用了抽象方法的重载实例方法，最都调用了上面的抽象方法，但参数列表都对参数进行了不同程度的缺省

```java
  /**
   * Create an FSDataOutputStream at the indicated Path.
   * Files are overwritten by default.
   * @param f the file to create
   */
  public FSDataOutputStream create(Path f) throws IOException {
    return create(f, true);
  }

  /**
   * Create an FSDataOutputStream at the indicated Path.
   * @param f the file to create
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an exception will be thrown.
   */
  public FSDataOutputStream create(Path f, boolean overwrite)
      throws IOException {
    return create(f, overwrite, 
                  getConf().getInt("io.file.buffer.size", 4096),
                  getDefaultReplication(f),
                  getDefaultBlockSize(f));
  }

  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * Files are overwritten by default.
   * @param f the file to create
   * @param progress to report progress
   */
  public FSDataOutputStream create(Path f, Progressable progress) 
      throws IOException {
    return create(f, true, 
                  getConf().getInt("io.file.buffer.size", 4096),
                  getDefaultReplication(f),
                  getDefaultBlockSize(f), progress);
  }

  /**
   * Create an FSDataOutputStream at the indicated Path.
   * Files are overwritten by default.
   * @param f the file to create
   * @param replication the replication factor
   */
  public FSDataOutputStream create(Path f, short replication)
      throws IOException {
    return create(f, true, 
                  getConf().getInt("io.file.buffer.size", 4096),
                  replication,
                  getDefaultBlockSize(f));
  }

  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * Files are overwritten by default.
   * @param f the file to create
   * @param replication the replication factor
   * @param progress to report progress
   */
  public FSDataOutputStream create(Path f, short replication, 
      Progressable progress) throws IOException {
    return create(f, true, 
                  getConf().getInt(
                      CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
                      CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT),
                  replication,
                  getDefaultBlockSize(f), progress);
  }

  /**
   * Create an FSDataOutputStream at the indicated Path.
   * @param f the file name to create
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   */
  public FSDataOutputStream create(Path f, 
                                   boolean overwrite,
                                   int bufferSize
                                   ) throws IOException {
    return create(f, overwrite, bufferSize, 
                  getDefaultReplication(f),
                  getDefaultBlockSize(f));
  }

  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * @param f the path of the file to open
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   */
  public FSDataOutputStream create(Path f, 
                                   boolean overwrite,
                                   int bufferSize,
                                   Progressable progress
                                   ) throws IOException {
    return create(f, overwrite, bufferSize, 
                  getDefaultReplication(f),
                  getDefaultBlockSize(f), progress);
  }

  /**
   * Create an FSDataOutputStream at the indicated Path.
   * @param f the file name to open
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file. 
   */
  public FSDataOutputStream create(Path f, 
                                   boolean overwrite,
                                   int bufferSize,
                                   short replication,
                                   long blockSize
                                   ) throws IOException {
    return create(f, overwrite, bufferSize, replication, blockSize, null);
  }

  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * @param f the file name to open
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file. 
   */
  public FSDataOutputStream create(Path f,
                                            boolean overwrite,
                                            int bufferSize,
                                            short replication,
                                            long blockSize,
                                            Progressable progress
                                            ) throws IOException {
    return this.create(f, FsPermission.getFileDefault().applyUMask(
        FsPermission.getUMask(getConf())), overwrite, bufferSize,
        replication, blockSize, progress);
  }

  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * @param f the file name to open
   * @param permission
   * @param flags {@link CreateFlag}s to use for this stream.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize
   * @param progress
   * @throws IOException
   * @see #setPermission(Path, FsPermission)
   */
  public FSDataOutputStream create(Path f,
      FsPermission permission,
      EnumSet<CreateFlag> flags,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress) throws IOException {
    return create(f, permission, flags, bufferSize, replication,
        blockSize, progress, null);
  }

  /**
   * Create an FSDataOutputStream at the indicated Path with a custom
   * checksum option
   * @param f the file name to open
   * @param permission
   * @param flags {@link CreateFlag}s to use for this stream.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize
   * @param progress
   * @param checksumOpt checksum parameter. If null, the values
   *        found in conf will be used.
   * @throws IOException
   * @see #setPermission(Path, FsPermission)
   */
  public FSDataOutputStream create(Path f,
      FsPermission permission,
      EnumSet<CreateFlag> flags,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress,
      ChecksumOpt checksumOpt) throws IOException {
    // Checksum options are ignored by default. The file systems that
    // implement checksum need to override this method. The full
    // support is currently only available in DFS.
    return create(f, permission, flags.contains(CreateFlag.OVERWRITE), 
        bufferSize, replication, blockSize, progress);
  }
```

还有一个用于传递回调接口的重载方法`Progressable`，如此一来，我们所写的应用就会被告知数据写入数据节点的进度，`Progressable `接口的代码片段如下：

```java
public interface Progressable {
    /**
    * Report progress to the Hadoop framework.
    */
    public void progress();
}
```

#### `append`方法追加数据

新建文件的另一种方法是使用`append()`在一个已有文件中追加(也有一些其他重载版本)。由下图可见，重要的是抽象方法`append()`。同抽象方法`create `一样，各个派生类需要重写，即不同的文件系统的实现会重新改函数的实现

抽象方法：

```java
  /**
   * Append to an existing file (optional operation).
   * @param f the existing file to be appended.
   * @param bufferSize the size of the buffer to be used.
   * @param progress for reporting progress if it is not null.
   * @throws IOException
   */
  public abstract FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException;
```

- 在指定的已存在的`Path`对象`f`后面增加`bufferSize`大小的数据，`process`可以报告进度

实例方法：调用抽象方法

```java
  /**
   * Append to an existing file (optional operation).
   * Same as append(f, getConf().getInt("io.file.buffer.size", 4096), null)
   * @param f the existing file to be appended.
   * @throws IOException
   */
  public FSDataOutputStream append(Path f) throws IOException {
    return append(f, getConf().getInt("io.file.buffer.size", 4096), null);
  }
```

- 只需传入`Path`对象，默认写`buffer`大小为`4096`字节，并且无传递回调接口的方法

```java
  /**
   * Append to an existing file (optional operation).
   * Same as append(f, bufferSize, null).
   * @param f the existing file to be appended.
   * @param bufferSize the size of the buffer to be used.
   * @throws IOException
   */
  public FSDataOutputStream append(Path f, int bufferSize) throws IOException {
    return append(f, bufferSize, null);
  }
```

- 传入文件路径对象和buffer大小，默认无传递回调接口的方法

#### `rename`方法对文件重命名

将`Path`对象`src`重命名为`dst`。

```java
  /**
   * Renames Path src to Path dst.  Can take place on local fs
   * or remote DFS.
   * @param src path to be renamed
   * @param dst new path after rename
   * @throws IOException on failure
   * @return true if rename is successful
   */
  public abstract boolean rename(Path src, Path dst) throws IOException;
    
```

- 只有一个抽象方法

#### `delete`方法删除文件

删除指定的`Path`对象

抽象方法：

```java
  /** Delete a file.
   *
   * @param f the path to delete.
   * @param recursive if path is a directory and set to 
   * true, the directory is deleted else throws an exception. In
   * case of a file the recursive can be set to either true or false. 
   * @return  true if delete is successful else false. 
   * @throws IOException
   */
// var1是待要删除的路径，若var1是一个目录，并且var2设为true，则该目录会被删除。若var1不是个目录，则var2可设为true或falase
public abstract boolean delete(Path var1, boolean var2) throws IOException;
```

- 接受两个参数`Path var1`和`boolean var2`，删除指定路径对象`Path`，如果`Path`是一个目录且`recursive`为真，则删除该目录，否则引发异常

实例方法（已被弃用）：

```java
  /**
   * Delete a file 
   * @deprecated Use {@link #delete(Path, boolean)} instead.
   */
  @Deprecated
// 删除目录，其实就是调用上面函数并将var2设为true
public boolean delete(Path f) throws IOException {
    return this.delete(f, true);
}
```

- 调用抽象方法，设置`recursive`默认为真，以删除一个文件或目录

#### 目录信息

FileSystem.getContentSummary()提供了类似Linux 的du、df提供的功能。du表示"disk usage"，它会报告特定的文件和每个子目录所使用的的磁盘大小；命令df则是"disk free"的缩写，用于显示文件系统上已用和可用的磁盘空间大小。其实现如下：

```java
// 接受一个路径f
public ContentSummary getContentSummary(Path f) throws IOException {
    // 先获取当前路径下的文件状态
    FileStatus status = this.getFileStatus(f);
    // 如果是一个文件而不是一个目录
    if (status.isFile()) {
        long length = status.getLen();
        // 大小为length，文件数为1，目录数为0，占用空间为length
        return (new ContentSummary.Builder()).length(length).fileCount(1L).directoryCount(0L).spaceConsumed(length).build();
    } else {
        // 分别代表文件大小，文件数，目录数
        long[] summary = new long[]{0L, 0L, 1L};
        // f是一个目录。获取当前目录下所有内容的状态
        FileStatus[] arr$ = this.listStatus(f);
        int len$ = arr$.length;

        // 遍历当前路径下所有文件或目录内容
        for(int i$ = 0; i$ < len$; ++i$) {
            FileStatus s = arr$[i$];
            long length = s.getLen();
            // 若仍然是个目录，则递归执行该函数，否则，是一个文件
            ContentSummary c = s.isDirectory() ? this.getContentSummary(s.getPath()) : (new ContentSummary.Builder()).length(length).fileCount(1L).directoryCount(0L).spaceConsumed(length).build();
            summary[0] += c.getLength();
            summary[1] += c.getFileCount();
            summary[2] += c.getDirectoryCount();
        }
// 遍历完所有文件后返回统计到的信息
        return (new ContentSummary.Builder()).length(summary[0]).fileCount(summary[1]).directoryCount(summary[2]).spaceConsumed(summary[0]).build();
    }
}
```

getContengSummary()方法的输入是一个文件或目录的路径，输出是该文件或目录的一些存储空间信息，这些信息定义在ContentSummary类，包括文件大小、文件数、目录数、文件配额，已使用空间和已使用文件配额等。具体该类中的成员变量有：

```java
public class ContentSummary implements Writable {
    // 文件大小
    private long length;
    // 文件数
    private long fileCount;
    // 目录数
    private long directoryCount;
    // 文件配额
    private long quota;
    // 已使用空间
    private long spaceConsumed;
    // 已使用文件配额
    private long spaceQuota;
    private long[] typeConsumed;
    private long[] typeQuota;
    // 定义了一些输出格式
    private static final String STRING_FORMAT = "%12s %12s %18s ";
    private static final String QUOTA_STRING_FORMAT = "%12s %15s ";
    private static final String SPACE_QUOTA_STRING_FORMAT = "%15s %15s ";
    private static final String HEADER = String.format("%12s %12s %18s ".replace('d', 's'), "directories", "files", "bytes");
    private static final String QUOTA_HEADER;
    
    ...
}
```

getContentSummary()方法并不是FileSystem的抽象方法，它可以在getFileStatus()的基础上，计算出ContentSummary()中包含的信息。

#### 工作目录

FileSystem可以设置或获取当前的工作目录，其实现如下：

```java
public abstract void setWorkingDirectory(Path var1);

public abstract Path getWorkingDirectory();
```

#### 创建文件

FileSystem也可以创建文件，其实现为

```java
// 给定路径创建文件
public abstract boolean mkdirs(Path var1, FsPermission var2) throws IOException;

// 给定路径创建文件，并制定权限
public abstract boolean mkdirs(Path var1, FsPermission var2) throws IOException;

// 给定路径创建文件，并将文件权限设置为默认权限
public boolean mkdirs(Path f) throws IOException {
    return this.mkdirs(f, FsPermission.getDirDefault());
}

// 静态方法，在文件系统中创建文件并赋予权限
public static boolean mkdirs(FileSystem fs, Path dir, FsPermission permission) throws IOException {
    // 调用抽象方法
    boolean result = fs.mkdirs(dir);
    // 设置权限
    fs.setPermission(dir, permission);
    return result;
}
```

其中，默认权限的实现为：

```java
public static FsPermission getDirDefault() {
    return new FsPermission((short)511);
}
```

可见，默认权限只是单纯的将权限设置为511。



上述这些抽象方法基本覆盖了用与处理文件和目录相关事物的主要操作，实现一个具体的文件体统，至少需要实现上面这些抽象方法。另一方面，Hadoop抽象文件系统基于以上这些方法，提供了一些工具方法，以方便用户调用。

### `AbstractFileSystem`抽象类

`AbstractFileSystem`是在`0.21`版本后出现的抽象类，和`FileSystem`一同作为抽象文件系统的基类，取代`FileSystem`类原来的部分功能。它与`FileSystem `有很多同名的方法。这些同名的方法完成相同的功能。比如get，create 等等。其它的方法大都是抽象方法。get 方法调用了`createFileSystem `方法实现。而`createFileSystem` 方法实现与`FileSystem` 的
`createFileSystem `可以说是完全一样。

`AbstractFilelSystem`类图如下图所示。

<img src="https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/06/01/28cf6e86faf2ed399fa083b1f2a962cf-image-20220517152056233-0875a3.png" alt="image-20220517152056233" style="zoom:33%;" />

## 六、Hadoop输入/输出流

​        Hadoop抽象文件系统使用流机制进行文件读写：
$$
\begin{cases}FSDataInputStream：读文件的流\\
FSDataOutputStream：写文件的流\end{cases}
$$
​        本次报告将针对这两个流，先从一个实例出发，总结输入输出两个流的使用位置；再针对不同的流的类别具体分析流的实现；并从hadoop文件系统的角度，剖析流的使用方式和在文件系统中扮演的角色。

### 流机制在hadoop文件系统的体现

#### 1.1.1 流使用位置的确定

​        分析这两个流的实现，我们首先需要定位两个流可能使用的场景。输入流必然与文件的读取有关，输出流与之相反必然是在创建文件或者其他可能的写入场景时使用。我们以输入流为例，使用hadoop读出文件`\home\sunyunqi\test.txt`的内容，打印在命令行中，使用远程debug模式尝试寻找输入流`FSDataInputStream`在何时使用。下面的读取实例详细说明了流机制在文件系统中的体现。

#### hadoop文件系统读取文件的示例

##### （1）shell的进入

输入命令：

```shell
hadoop fs -cat /home/sunyunqi/桌面/test.txt
```

下面过程展示流在hadoop文件系统读取文件时起到的作用。

<img src="https://cdn.jsdelivr.net/gh/Holmes233666/blogImage@main/img/image-20220521092623822.png" alt="image-20220521092623822" style="zoom: 33%;" />

​        可以看到在shell中执行上述命令后会进入hadoop的`FsShell`函数，`FsShell`是本地文件系统处理如`cat`，`chown`等常见命令的核心处理过程。`run()`将初始化一个shell实例并且处理传入的参数。在我们的示例中参数有两个，分别为`-cat`命令和文件目录`/home/sunyunqi/桌面/test.txt`，存储在argv数组中。

##### （2）run的核心过程

​        经过相关工具类的包装后，进入`FsShell`的核心处理过程：`run()`方法，以输入的指令为例，该方法包括三个处理过程：

- 初始化shell，识别输入的命令`cat`，并初始化`cat`命令的一个实例
- 处理命令的参数，在本例中即为传入的路径
- 返回处理结果`error_code`

​        如下图：

<img src="https://cdn.jsdelivr.net/gh/Holmes233666/blogImage@main/img/image-20220521111937798.png" alt="image-20220521111937798" style="zoom: 50%;" />

​        在三个过程中，显然只有步骤2：处理传入cat命令的路径的过程会涉及有关文件的读入，我们进入`instance.run()`查看相关细节：

<img src="https://cdn.jsdelivr.net/gh/Holmes233666/blogImage@main/img/image-20220521112505279.png" alt="image-20220521112505279" style="zoom:30%;" />

​        在处理参数时，command首先对命令`-cat`进行是否过时的判断，若命令过时会给出相应的提示。然后对参数中的选项部分进行处理，比如`-cat -n`，需要处理可能涉及的选项，此处不会涉及流操作，不进行深入讨论。涉及流操作的部分为line 165，处理核心的路径参数部分，该部分真正涉及到了文件的读入。

![image-20220521114526392](https://cdn.jsdelivr.net/gh/Holmes233666/blogImage@main/img/image-20220521114526392.png)

​        如上的三个过程，经过相关的异常控制后终于进入了`getInputStream()`过程。我们首先进入`getInputStram()`方法。该方法在`Display`类中，实际上包装的是`FileSystem`类的`open()`方法。如下图，我们至此可以看到`FSDataInputStream()`在此处得到了使用。

![image-20220521115723763](https://cdn.jsdelivr.net/gh/Holmes233666/blogImage@main/img/image-20220521115723763.png)

##### （3）`FileSystem.open()`对流的使用过程

​        在上述过程中我们可以总结得到输入流`FSDataInputStream`的使用位置，类似的，我们若使用命令`cp`进行测试会得到输出流`FSDataOutputStream`的使用位置。即：

- `FSDataInputStream`：通过`FileSystem.open()`打开，返回值是一个`InputStream`类型
- `FSDataOutputStream`：通过`FileSystem.create()`或者`FileSystem.append()`打开

---

<img src="https://cdn.jsdelivr.net/gh/Holmes233666/blogImage@main/img/image-20220521223724184.png" alt="image-20220521223724184" style="zoom:50%;" />

​        经过上述的过程，FsDataInputStream会通过校验和处理后，最后返回一个`FSDataInputStream`的子类`FSDataBounedInputStream`。然后返回上次调用处：`printToStdOut()`方法处。

<img src="https://cdn.jsdelivr.net/gh/Holmes233666/blogImage@main/img/image-20220521225319619.png" alt="image-20220521225319619" style="zoom:50%;" />

​        在`printToStdOut`方法中将通过一个hadoop实现的`IOUtils`：用于IO相关功能的类，实现最终将`in`中内容读入并拷贝进`out`中，输出在命令行中。这样，命令行中出现了最终的结果：`Hello World!`

![image-20220523165200825](https://cdn.jsdelivr.net/gh/Holmes233666/blogImage@main/img/image-20220523165200825.png)

​        另外，我们对`IOUtils`进行一些额外的说明。该类是hadoop实现的一个和输入输出有关的工具类，提供的常见方法是：

- 从一个流复制到另一个流：`copyBytes()`重载函数族
- 从流中读取若干字节到数组中：`readFully()`为代表的函数族

<img src="https://cdn.jsdelivr.net/gh/Holmes233666/blogImage@main/img/image-20220523194959089.png" alt="image-20220523194959089" style="zoom:50%;" />

​        上述`printToStdout()`方法中使用的`copyBytes()`即实现了将`in`流中的内容拷贝到`out`流中，实现输出到命令行。

##### （4）FileSystem流使用总结

​        由上分析可知，Hadoop中的输入流继承了java中原生的io类中的`InputStream`中`read()`，`read(byte[] b)`等方法。另一方面，其实Hadoop的`FSDataInputStream`和`FSDataOutputStream`也实现了一些额外的功能，我们将在下一节针对不同的流进行分析。

### 1.2 流的分类

#### 1.2.1 FSDataInputStream

![image-20220520155106657](https://cdn.jsdelivr.net/gh/Holmes233666/blogImage@main/img/image-20220520155106657.png)

​        如上图可知`FSDataInputStream`继承了Java原生的基本类型`DataInputStream`。`DataInputStream`用于从底层的输入流中读取基本的java数据类型，构造时需要传入直接`InputStream in`作为参数。如下：

```java
DataInputStream dis = new DataInputStream(InputStream in);
```

​        而`FSDataInputStream`由于继承了`DataInputStream`类，所以其在初始化时也会调用父类的构造方法，传入的参数也是一个`InputStream`：

```java
  public FSDataInputStream(InputStream in) {
    super(in);
    if( !(in instanceof Seekable) || !(in instanceof PositionedReadable) ) {
      throw new IllegalArgumentException(
          "In is not an instance of Seekable or PositionedReadable");
    }
  }
```

​        与Java原生的流不同的是，Hadoop的`FSDataInputStream`在`DataInputStream`的基础上额外提供了一些功能：

- 从流中的某一个位置开始读
- 在流中进行随机存取
- 提供读API，写入`ByteBuffer`而不是`byte[]`
- 设置预先读
- ……

​        我们将重点分析该类实现的`Seekable`接口和`PositionedReadable`接口的工能。

#####  `Seekable`接口

**（1）接口功能概述**

​        该接口提供在文件流中进行随机存取的方法：随机**定位文件阅读位置**的能力。

其中：

- `void seek(long pos)`：将当前偏移量设置到pos指代的位置，下次读数据时将从该位置开始
- `long getPos()`：得到当前的偏移量
- `seekToNewSource`：可以应用在HDFS中，文件有多个副本时，可以使用这个方法重新选择一个副本。返回值代表是否成功找到了这个新的位置。

<img src="https://cdn.jsdelivr.net/gh/Holmes233666/blogImage@main/img/image-20220523163324808.png" alt="image-20220523163324808" style="zoom:50%;" />

**（2）`seek()`函数的使用**

​        下面我们通过编写一个简单的程序来测试`seek()`函数的使用：我们在读完文件内容后将当前便宜设置到`1`处，再次打开流进行读取，观察输出的结果，测试的代码如下：

```java
public class FSDataInputStreamTest {
    public static void main(String[] args) {
        String uri = "file:///home/sunyunqi/桌面/test.txt";
        Configuration conf = new Configuration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(uri), conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        FSDataInputStream in = null;
        try {
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
            in.seek(6); 
            IOUtils.copyBytes(in, System.out, 4096, false);
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            IOUtils.closeStream(in);
        }
    }
}
```

​        上述程序中，我们将预读取的文件URI作为参数打开，使用上一节[1.1.2 (4) FileSystem流使用总结](#1.1.2  (4) FileSystem流使用总结)中说明的`fs.open()`方法，得到一个`InputStream`类型的对象。该对象本质是一个`FSDataInputStream`实例，所以我们可以调用他实现的`Seekable`接口中的的`seek()`方法：

```java
in.seek(6);			// 从文件的第6个字符开始读
```

​        我们在调用该方法前先输出一次：使用上述已经说明的`IOUtils.copyBytes()`方法，实现将输入流中的内容拷贝到标准输出流中。此时应该我们可以在控制台看到一行输出”Hello World“。然后执行`in.seek(6)`，再使用`seek.getPos()`方法打印输出下此时的偏移量，可以看到此时流位置被移动到了第6个字符，下次读取应该会从第6个字符后开始输出。于是再次执行`IOUtils.copyBytes()`时出现如下的效果——只输出了后面的`World!`：

![image-20220523225735005](https://cdn.jsdelivr.net/gh/Holmes233666/blogImage@main/img/image-20220523225735005.png)

即：`seek()`方法实现了流的随机读。

##### `PositionedReadable`接口

​        该接口提供了从流中的某一个位置开始读数据的方法，`FSDataInputStream`实现如下三个方法：

- `public int read(long position, byte[] buffer, int offset, int length)throws IOException`;

  该方法实现了从流中特定位置读取最多指定的长度的字符到缓冲数组中

- `public void readFully(long position, byte[] buffer, int offset, int length) throws IOException;`

  该方法则实现从流中特定位置读取指定的长度的字符到缓冲数组中

- `public void readFully(long position, byte[] buffer) throws IOException;`

​        从上面的三个方法可以看出`PositionedReadable`接口实现的是读取数据到缓冲数组中，似乎再对`Buffer`进行输出后`PositionedReadable`接口与`Seekable`接口实现的功能基本重合。其实不然，`PositionedReadable`接口中的方法不会改变流的位置。我们使用下面的程序进行测试：

```java
public class FSDataInputStreamTest {
    public static void main(String[] args) {
        /*** String uri = "file:///home/sunyunqi/桌面/test.txt";
     		 …… 此处程序与上例程序一致	 省略
        	 FSDataInputStream in = null;
        *****/
        try {
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
            byte[] buffer = new byte[5];
            System.out.println("现在流的位置为：" + in.getPos());
            in.read(0, buffer, 1, 3);
            System.out.println("现在流的位置为：" + in.getPos());
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            IOUtils.closeStream(in);
        }
    }
}
```

​        在程序中，我们在`read()`前后`read()`后各进行了一次`getPos()`，如下图实验结果，两次获得的流的位置相同，即`PostionedReadable`接口不会改变流的位置：

![image-20220524103931134](https://cdn.jsdelivr.net/gh/Holmes233666/blogImage@main/img/image-20220524103931134.png)

​        另外，值得说明的一点是`PositionedReadable`接口中的三个方法都是线程安全的，而`Seekable`接口中方法不然。

#### FSInputStream

​        除了`FSDataInputStream`，文件系统中还有另一个输入流类：`FSInputStream`。该类直接继承于父类`InputStream`类，而我们上面说明的`FSDataInputStream`则是继承于`InputStream`类的子类`DataInputStream`，二者在继承树上的关系可以用下面的类图表示：

<img src="https://cdn.jsdelivr.net/gh/Holmes233666/blogImage@main/img/image-20220523221531098.png" alt="image-20220523221531098" style="zoom: 50%;" />

​        另外也可以看出`FSInputStream`也实现了`Seekable`和`Positionable`两个接口，但是较`FSDataInputStream`类实现的其他的众多接口外，似乎`FSInputStream`功能要更单薄些。那么似乎直接使用`FSDataInputStream`就可以完成`FSInputStream`的所有功能了，`FSInputStream`有何存在的意义呢，而且二者似乎并没有直接使用上的关联。

​        我们展开`FSInputStream`实现的子类，可以看到其子类有10多个，这里选取了其中的任意四个，每一个子类从前缀大致可以看出是某一个具体的文件系统的输入流：

![image-20220523222036723](https://cdn.jsdelivr.net/gh/Holmes233666/blogImage@main/img/image-20220523222036723.png)

​        而在Hadoop的整体文件系统结构中，所有的文件系统子类都继承于抽象的父类`FileSystem`，该抽象类定义了一个文件系统所需要的基本操作，包括常见的：

- `open`
- `create`
- `close`
- `append`
- ……

​        在1.1中我们可以知道`FSDataInputStream`会在`FileSystem.open()`中得到使用，但是在上面的说明中我们并未说明是如何得到使用的，为了解决这个问题，同时解决`FSInputStream`和`FSDataInputStream`直接的关系问题，我们看一段`FileSystem`的子类`FTPFileSystem`类中`open()`函数的一段代码，该方法实现了`FileSystem`类的`open`方法：

```java
public FSDataInputStream open(Path file, int bufferSize) throws IOException {
    // 此处若干代码省略 …… 
    InputStream is = client.retrieveFileStream(file.getName());
    FSDataInputStream fis = new FSDataInputStream(new FTPInputStream(is,
        client, statistics));
    // 此处若干代码省略 …… 
    return fis;
  }
```

​       正如之前所说，`FSDataInputStream`会在文件系统使用`open()`函数时调用，在此处的`FTPFileSyetem`的`open()`方法中，首先`new`了一个`FTPInputStream`对象，然后将对象传入`FSDataInputStream`的构造函数中（由1.2.1可知，`FSDataInputStream`的构造函数需要的参数为一个唯一的`InputStream`对象，`FTPInputStream`作为`InputStream`的子类，符合条件）。我们再查看如下图所示的其他的具体的`FileSystem`子类文件系统中的`open()`方法，发现无一例外的是：，他们都会实现自己对应的，继承于`FSInputStream`类的`xxxInputStream`（xxx为文件系统名），在这些文件系统实现的`open`方法中，都需要将`xxxInputStream`包装于`FSDataInputStream`中。

![image-20220524110541498](https://cdn.jsdelivr.net/gh/Holmes233666/blogImage@main/img/image-20220524110541498.png)

​        通过以上的分析，我们虽然没有直接得到`FSDataInputStream`和`FSInputStream`之间的关系，但是我们得到了`FSDataInputStream`和`FSInputStream`的子类直接的关系，即：**`FSDataInputStream`的构造需要依赖于具体的文件系统对应的流来构造相应的对象，而这些流由相应的具体的文件系统来实现。**这是一个重要的结论。

#### FSDataOutputStream

​        与`FSDataInputStream`相反，`FSDataOutputStream`实现的是数据的写操作。在`FileSystem.create()`和`FileSystem.append()`中会会使用，如下图所示：

<img src="https://cdn.jsdelivr.net/gh/Holmes233666/blogImage@main/img/image-20220523203358414.png" alt="image-20220523203358414" style="zoom: 57%;" />

​        为了进一步观察`DataOutputStream`的功能实现，我们画出该类的类图：

<img src="https://cdn.jsdelivr.net/gh/Holmes233666/blogImage@main/img/image-20220523203703425.png" alt="image-20220523203703425" style="zoom:50%;" />

​        可以看到，该类直接继承了java.io中的原生IO类：`DataOutputStream`，该流实现将Java基本数据类型写入到底层输出流。主要实现的方法则是基本的`write()`方法组以及`flush()`等刷新流方法。`FSDataOutputStream`毫无疑问地继承了这些方法，同时实现`Syncable`和`CanSetDropBehind`接口。这两个接口都不提供类似`FSDataInputStream`中的随机读写的功能，即hadoop只支持随机读，但不支持随机写。

##### 内部类`PositionCache`

​        我们将类图展开，观察关键的方法：

<img src="https://cdn.jsdelivr.net/gh/Holmes233666/blogImage@main/img/image-20220523204858176.png" alt="image-20220523204858176" style="zoom:50%;" />

​        可以注意到`FSDataOutputStream`中含有一个内部类：`PositionCache`。在`FSDataOutputStream`中有三个构造函数，但是三个函数由上至下最终调用的是第三个构造函数，该内部类`PositionCache`则直接充当`FSDataOutputStream`类中的唯一初始化对象：

<img src="https://cdn.jsdelivr.net/gh/Holmes233666/blogImage@main/img/image-20220523210453015.png" alt="image-20220523210453015" style="zoom: 33%;" />

​        对于该内部类而言，一个`long`类型的`position`属性是其唯一的属性，记录了流当前写入的位置，并且该类继承了`OutputStream`的子类`FilterOutputStream`，实现了相关的`write()`方法，需要注意的是，这里的`write()`方法在写入时会对`position`属性进行自增操作：

```java
// 基本的写操作
    public void write(int b) throws IOException {
      out.write(b);
      position++;									// position自增
      if (statistics != null) {
        statistics.incrementBytesWritten(1);
      }
    }

// 向数组中写入的操作
 public void write(byte b[], int off, int len) throws IOException {
      out.write(b, off, len);
      position += len;                            // position 修改
      if (statistics != null) {
        statistics.incrementBytesWritten(len);
      }
    }
```

​        此外`PositionCache`类提供了一个`getPos()`。由此推之，该内部类是`FSDataOutputStream`对于无法随机写的一种弥补：通过访问`position`属性，可以得到当前写入的位置，实现了一种类似于写追踪的功能。

#####  `Syncable`接口

​        该接口中核心的方法即为：`sync()`方法，后续被`hflush()`方法替代，方法的目的是实现强制对基本设备的缓冲区执行同步操作。

##### `CanSetDropBehind`接口

​        该接口中也只有一个核心方法：`setDropBehind`，实现配置流是否应该删除缓存。

```java
public void setDropBehind(Boolean dropCache) 
      throws IOException, UnsupportedOperationException;
```

### IO流总结

​        通过我们的实验，我们首先通过探索得出了`FSDataInputStream`和`FSDataOutputStream`的使用方式：

- `FSDataInputStream`：通过`FileSystem.open()`访问，返回值是一个`InputStream`类型
- `FSDataOutputStream`：在`FileSystem.create()`或者`FileSystem.append()`处调用

​        然后我们从具体三个流的角度，分别分析其实现的功能。重点分析了`FSDataInputStream`实现的接口及其重要的函数功能。

​        在分析`FSInputStream`时，通过建立子类与`FSDataInputStream`之间的关系，得到了`FSInputStream`和`FSDataInputStream`之间通过具体子类建立联系的结论，进一步从文件系统的使用的角度总结了`FSDataInputStream`和`FSDataOutPutStream`在文件系统中扮演的角色：**实现可随机读，跟踪写的IO流，但具体实例依赖于具体文件系统实现的输入流和输出流。**这也是hadoop实现的IO流的一大特点。

## 七、FileSystem的权限

Hadoop文件系统中定义了一个和Linux系统类似的权限模型。每个文件和目录都有一个所有者 (owner) 和一个组（group), 在 FileStatus 类中, 可以 通过 getOwner() 和 getGroup() 获得相应的属性。

主要涉及FsPermission和FsAction类

![image-20220601204912836](https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/06/01/17683091c85643973eb39fb459155867-image-20220601204912836-c3c4e7.png)

用户身份机制对 Hadoop 文件系统来说只是外部特性。目前, Hadoop 并不提供创建用户 身份、用户组或处理用户凭证等功能。也正是这个原因, FileStatus 类使用了字符串 owner 和 group, 保存了文件所有者和文件所在用户组的信息, 代码如下:

```java
public class FileStatus implements Writable, Comparable {
  ...
  private FsPermission premission;
  private String owner; 
  private String group;
}
```

而在 Linux 系统中, 文件或目录元信息通过整型的用户 ID 和组 ID, 保存文件所有者和 文件所在用户组。Linux 系统的实现方式比较节省存储空间, 但需要增加用户 ID 到用户名、 组 ID 到组的映射关系, 并提供命令维护该关系。

FsPermission 保存了 FileStatus 对应文件的权限, 也是采用 Linux 的表示和显示习惯, 包 括使用八进制数来表示权限。当新建一个文件或目录时, 它的所有者是客户进程的用户, 它 所属组是父目录的组。文件或目录的文件所有者、文件组和其他用户的权限信息, 可以保存 在 FsPermission 对象中。应该说, FsPermission 是一个简单的包装类, PermissionStatus 则在 FsPermission 的基础上包含了文件的文件所有者和文件所在用户组的信息, 其实现也很简单。

和 Linux 系统一样, 文件或目录对其所有者、同组的其他用户以及所有其他用户可以分 别有着不同的读、写和执行权限, 也就是前面讨论的 rwx 权限。在 Hadoop 中, rwx 权限通 过枚举 FsAction 定义。关键字 enum 是在 Java $1.5$ 中引人的新特性, 通过该关键字, 用户可 以方便地定义一个常量集合。对于枚举类型, Java 内部将其转换为 java.lang.Enum 的子类, 并提供了一些方法。其中, 成员变量 ordinal 用于返回枚举常量的序数, 也就是它在枚举声明中的位置, 初始常量序数为 0 。FsAction 中权限常量的定义利用了枚举 常量序数, 如 FsAction.NONE, 它是这个枚举类型的第一个枚举值, 它的序数为 0 , 正好也 是对应 Linux 形式权限的表示 000 , 而 WRITE_EXECUTE, 其表示为 011, 序数为 3 。

FsAction 还提供了 implies() 方法, 用于判断权限 1 是否隐含权限 2, 如权限 FsAction. WRITE_EXECUTE隐含了权限 FsAction. WRITE 和 FsAction.EXECUTE, 但不隐含权限 FsAction.READ_EXECUTE。利用这些权限的枚举常量序数, 可以很容易实现 implies() 方 法。FsAction 类的主要代码如下:

```java
public enum FsAction {
  //POSIX 风格的权限
  NONE (" - -"),
  EXECUTE (" - -x"),
  WRITE ("-W-"),
  WRITE_EXECUTE (" -wX"),
  READ ("r-""),
  READ_EXECUTE (" $left.r-x^{prime prime}right)$,
  READ_WRITE ("IW-"),
  $A L Lleft(" r w X^{n}right)$;
  public boolean implies (FsAction that) {
    if (that $!=$ null) {
    	return (ordinal () & that.ordinal ()$)==$ that. ordinal () ;
    }
    return false;
  }
}
```

## 八、FileSystem静态方法

### **FileSystem的子类（具体文件系统）**

我们可以把FS的有关类分为2部分，一个是工具类，一个是实现类。

**工具类：**

1. FilterFileSystem

   FilterFileSystem继承FileSystem，声明了一个FileSystem的对象，自己做一些处理，交给FS处理

2. CheckSumFileSystem

   ChecksumFileSystem类继承FilterFileSystem类,提供校验CRC

3. LocalFilesystem

   继承自ChecksumFileSystem，重写父类的文件复制的类，添加内部对象rfs,不过没咋用，判断为基本同父类一样


**实现类：**

1. NativeS3FileSystem

2. 实现具体功能的文件系统子类与其父类FileSystem的差异显而易见，除去父类中的抽象函数如append，Create，delete，getUri，getFileStatus，getWorkingDirectory等需要根据子类的情况实现重写的函数之外，一些函数子类利用了其系统中独有的类或者接口进行了自己的实现。

3. FTPFileSystem

   子类FTPFileSystem与父类FileSystem的函数实现差异很大，主要原因在于FTPFileSystem引入了Client对象并利用其完成了很多与父类不同的函数实现机制

4. KosmosFileSystem

   子类KosmosFileSystem与父类FileSystem的函数实现差异较大，主要原因在于KosmosFileSystem利用了KfsImpl接口

5. S3FileSystem

   子类S3FileSystem与父类FileSystem的函数实现差异较大，子类利用INode节点的特性对函数的实现方式进行重写

6. RawLocalFileSytem

   自己重写filesystem的大多数方法，去操作文件，所依赖的流都是自己重载过的内部类流

7. HDFS

   文件系统实现主体依靠DFSClient来实现，只是为了包装一层filesystem

### FileSystem的内部类和属性

#### Cache

![image-20220516175305859](https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/06/01/e179dabee82c31305b9d191e29fcf65f-image-20220516175305859-16526947868302-038d47.png)

<center>
    Cache类图，Cache 中有两个内部类，ClientFinalizer 和Key。
</center>

**1. ClientFinalizer类**

ClientFinalizer 类为一线程类，当Java 虚拟机停止运行时，该线程才会运行。而运行时，run()方法会调用Cache.closeAll(true)方法，进行清理工作。

**2. Key类**

内部静态类 Key，顾名思意，它作为Cache 中HashMap<Key, FileSystem> 的关键字。保存了
有关文件系统的Uri 的信息。

FileSystem的内部类Cache保存了已经打开的、可以共享的文件系统，其实现不是很复杂。唯一需要注意的是可共享的条件，这个条件体现在FileSystem.CACHE.Key 中。

也就是说，只有在**authority, ugi,unique,scheme**这4个值都相等的情况下，文件实例才会被共享。

- **schema**是URI模式，这个很自然要相等﹔
- **authority**部分，用相同授权打开的具体文件系统才能共享，用户可以打开URI模式相同，但有不同授权的具体文件系统;
- **unique**是一个整数，如果没有特殊情况，默认为0;
- **ugi**的类型为UserGroupInformation时，则保存了打开具体文件系统的本地用户的信息，也就是说，即使其他3部分都相等，不同本地用户打开的具体文件系统也是不能共享的。

FileSystem.CACHE.Key通过这4个字段，明确地定义了可共享具体文件系统的条件。
其中，unique字段比较特殊，它提供了一种机制，如果用户需要创建其他3个字段都相同，但又不共享的具体文件系统时，可以使用该字段，以作区分。FileSystem.CACHE.getUnique()实现了这个机制，通过它调用者可以得到一个被Cache管理的，且不被共享的具体文件系统，gerUnique()函数将在第6条中详细介绍。

**3. Map**

cache类中内含一个 final Map<Key, FileSystem> map(为了能够快速获取FileSystem，这个map采用的是HashMap)，用于存储和管理由{scheme,authority,username}作为Key，而以FileSystem引用作为值的键值对。

```java
Map<Key, FileSystem> map = new HashMap<Key, FileSystem>();  
```

**4. toAutoClose**

集合 toAutoClose 属性用来表示是否需要自动关闭Key 所对应的文件系统。

**5. get()方法**

FileSystem中获得具体文件系统主要通过get()方法，它有多种重载形式 ，get()方法需要一个URI，文件系统根据这个URI的模式“ramfs”，了解到用户需要打开一个内存文件系统，则创建或返回对应的文件系统。

cache.get()方法内部仅仅是简单的调用了getInternal()方法。

```java
   //根据URI与Configuration，从缓存中取出一个FileSystem实例，要求同步缓存操作  
	FileSystem get(URI uri, Configuration conf) throws IOException{
        Key key = new Key(uri, conf);
        return getInternal(uri, conf, key);
    }
```

**6. getUnique()方法**

cache.getUnique()方法与cache.get()方法类似，都是在内部简单的调用了cache.getInternal()方法。

```java
    /** The objects inserted into the cache using this method are all unique */
    FileSystem getUnique(URI uri, Configuration conf) throws IOException{
        Key key = new Key(uri, conf, unique.getAndIncrement());
        return getInternal(uri, conf, key);
    }
```

**7. getInternal()方法**

```java
private FileSystem getInternal(URI uri, Configuration conf, Key key) throws IOException{

    FileSystem fs;
     synchronized (this) {
       fs = map.get(key);
     }
     if (fs != null) {//如果在cache中，即命中，直接返回
       return fs;
     }
	//未命中时，需要创建新的文件系统并返回
     fs = createFileSystem(uri, conf);
 //以下涉及同步问题
     synchronized (this) { // refetch the lock again
       FileSystem oldfs = map.get(key);
       if (oldfs != null) { // 存在一个一样的系统更早被创建
         fs.close(); // 关闭新的文件系统
         return oldfs;  // 返回旧的文件系统
       }

       // 加入新创建的文件系统到map当中
       if (map.isEmpty() && !clientFinalizer.isAlive()) {
         Runtime.getRuntime().addShutdownHook(clientFinalizer);
       }
       fs.key = key;
       map.put(key, fs);   //加入map中
       if (conf.getBoolean("fs.automatic.close", true)) {
         toAutoClose.add(key);       //如果需要自动关闭，将其加入到自动关闭的链表
       }
       return fs;
     }
   }
```

**8. remove()方法**

```java
//根据指定的缓存Key实例，从缓存中删除该Key对应的FileSystem实例，要求同步缓存操作。  
synchronized void remove(Key key, FileSystem fs)  
```

**9. closeAll()方法**

```java
//迭代缓存Map，删除缓存中的缓存的全部文件系统实例，要求同步缓存操作。  
synchronized void closeAll() throws IOException  
```

 当关闭FS的时候(用户手动关闭或JVM在程序运行结束，ClientFinalizer对文件系统进行关闭)，首先会调用cache的closeAll 方法，对map进行清空(先清空cache中其他FS的键值对，再清空本FS的键值对)；然后再调用FS的processDeleteOnExit方法对 一些temp目录进行清空。

#### 5.2.2.2 Statistics

![image-20220516205700566](https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/06/01/07508017da1ff232ee4a3c126478d5dc-image-20220516205700566-16527058240453-2d89ac.png)

<center>
    Statistic类图
</center>


 **statisticsTable**是一个**IdentityHashMap<Class<? extends FileSystem>, Statistics>**，用于保存并管理每个文件系统对应的统计信息Statistics。

**Statistics**类使用了java.util.concurrent.atomic包中的原子变量属性，保证线程安全的原子读写操作的同时，提高并行性能。如下所示

```java
private final String scheme;  
private AtomicLong bytesRead = new AtomicLong();  
private AtomicLong bytesWritten = new AtomicLong();  
private AtomicInteger readOps = new AtomicInteger();  
private AtomicInteger largeReadOps = new AtomicInteger();  
private AtomicInteger writeOps = new AtomicInteger();  
```

#### GlobFilter（在2.7.3中已经不是内部类了）

![image-20220516220509090](https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/06/01/a4aa5030a6a107878552ef6069be1217-image-20220516220509090-16527099100524-5caf5e.png)

<center>
    GlobFilter类图
</center>




GlobFilter 实现了接口PathFilter，该类判断一个路径是否匹配指定模式。该模式通过
构造函数GlobFilter(String filePattern)和GlobFilter(String filePattern, PathFilter filter)中的filePattern
指定，经过检查是否有效之后，再通过Pattern.compile 编译，并将结果存于属性regex 中。

### 文件系统的获取

FileSystem 是一个普通的文件系统API，所以首要任务是检索我们要用的文件系统实例。取得FileSystem 实例有三种静态工厂方法get()。这些get()方法是Hadoop 0.21 版本之前就存在的，但在0.21 版本中又添加了与之功能相同的三个方法newInstance()。

```java
public static FileSystem get(final URI uri, final Configuration conf,
  final String user) throws IOException, InterruptedException {
  UserGroupInformation ugi;
  if (user == null) {
  ugi = UserGroupInformation.getCurrentUser();
  } else {
  ugi = UserGroupInformation.createRemoteUser(user);
  }
  return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
  public FileSystem run() throws IOException {
  return get(uri, conf);
  }
  });
  }
  /** Returns the configured filesystem implementation.*/
  public static FileSystem get(Configuration conf) throws IOException {
  return get(getDefaultUri(conf), conf);
  }
```

**几种形式的FileSystem.get()最终调用的get()方法如下：**

```java
public static FileSystem get(URI uri, Configuration conf) throws IOException {
    String scheme = uri.getScheme();//获取URI模式
    String authority = uri.getAuthority();//鉴权信息

    if (scheme == null) {                       // URI模式为空，返回默认文件系统
      return get(conf);
    }

    if (authority == null) {                       // no authority
      URI defaultUri = getDefaultUri(conf);       //如果没有权限，从默认配置里获取URI
      if (scheme.equals(defaultUri.getScheme())    // if scheme matches default
          && defaultUri.getAuthority() != null) {  // 
        return get(defaultUri, conf);              // 返回默认的参数
      }
    }

    String disableCacheName = String.format("fs.%s.impl.disable.cache", scheme);
    //是否可以从cache里获取
    if (conf.getBoolean(disableCacheName, false)) {
      return createFileSystem(uri, conf);//创建对应文件系统
    }

    return CACHE.get(uri, conf);//在Cache中获取对应的文件系统
  }
```



在FileSystem.get()处理过程中，会判断是否使用FileSystem.Cache中保存的文件系统，因为打开一个文件系统是一个比较耗资源的操作，因此共享文件系统是一个不错的选择。当disableCacheName为true时，则每次创建一个新的文件系统实例，否则，返回Cache中保存的共享文件系统。

- 当给定uri调用get获取指定的 FileSystem时，最终还是调用cache.get。
- cache.get会查找相应的键值对，不存在时调用createFileSystem新建一 个FileSystem并将其插入map中。



当不适用共享文件系统时，则需要用FileSystem的私有方法createFileSystem()创建相应的文件系统。

```java
//FileSystem.createFileSystem()
private static FileSystem createFileSystem(URI uri, Configuration conf
      ) throws IOException {
      //获取文件系统实现的Class类对象
  Class<?> clazz = conf.getClass("fs." + uri.getScheme() + ".impl", null); 
  if (clazz == null) {
  throw new IOException("No FileSystem for scheme: " + uri.getScheme());
  }
  //通过反射创建对象，并初始化文件系统
  FileSystem fs = (FileSystem)ReflectionUtils.newInstance(clazz, conf);
  fs.initialize(uri, conf);
  return fs;
  }
```



### 查询文件

#### 文件元数据：FilStatus

任何文件系统的一个重要特征是定位其目录结构及检索其存储的文件和目录信息的能力。

FileStatus 是一个简单的类，封装了文件系统中文件和目录的元数据，包括文件长度、块大小、副本、修改时间、所有者以及许可信息。FileStatus 实现了Writeable 接口，可以序列化。

![image-20220516220935569](https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/06/01/ca12fe7fbd829d1ba5fa7524f03064a2-image-20220516220935569-16527101767475-cebb4d.png)

<center>
    FileStatus 类
</center>


FileSystem 的getFileStatus()提供了获取一个文件或目录的状态对象的方法。

#### 列出文件

查找一个文件或目录的信息很实用，但有时我们还需要能够列出目录的内容。这就是listStatus()方法的功能：

1. 使用指定的过滤器过滤指定path的文件，然后将剩余的文件的状态信息保存到用户给定的ArrayList集合中。

```java
/**
   * Filter files/directories in the given path using the user-supplied path
   * filter. Results are added to the given array <code>results</code>.
   * @throws FileNotFoundException when the path does not exist
   * @throws IOException see specific implementation
   */
  private void listStatus(ArrayList<FileStatus> results, Path f,
      PathFilter filter) throws FileNotFoundException, IOException {
    FileStatus listing[] = listStatus(f);
    Preconditions.checkNotNull(listing, "listStatus should not return NULL");
    for (int i = 0; i < listing.length; i++) {
      if (filter.accept(listing[i].getPath())) {
        results.add(listing[i]);
      }
    }
  }
```

2. 和上面的区别是，保存状态信息的集合是内部新建的，不是用户指定的。

```java
public FileStatus[] listStatus(Path f, PathFilter filter)
                                   throws FileNotFoundException, IOException {
    ArrayList<FileStatus> results = new ArrayList<>();
    listStatus(results, f, filter);
    return results.toArray(new FileStatus[results.size()]);
  }
```

3. 使用默认的过滤器来过滤指定path集合中的文件，然后将剩余的文件的状态信息保存到内部新建的列表中。

```java
/**
   * Filter files/directories in the given list of paths using default
   * path filter.
   * <p>
   * Does not guarantee to return the List of files/directories status in a
   * sorted order.
   *
   * @param files
   *          a list of paths
   * @return a list of statuses for the files under the given paths after
   *         applying the filter default Path filter
   * @throws FileNotFoundException when the path does not exist
   * @throws IOException see specific implementation
   */
  public FileStatus[] listStatus(Path[] files)
      throws FileNotFoundException, IOException {
    return listStatus(files, DEFAULT_FILTER);
  }
```

4. 使用指定的过滤器来过滤指定path集合中的文件，然后将剩余的文件的状态信息保存到内部新建的列表中。

```java
public FileStatus[] listStatus(Path[] files, PathFilter filter)
      throws FileNotFoundException, IOException {
    ArrayList<FileStatus> results = new ArrayList<FileStatus>();
    for (int i = 0; i < files.length; i++) {
      listStatus(results, files[i], filter);
    }
    return results.toArray(new FileStatus[results.size()]);
  }
```

传入参数是一个文件时，它会简单地返回长度为1 的FileStatus 对象的一个数组。当传入参数是一个目录时，它会返回0 或者多个FileStatus 对象，代表着此目录所包含的文件和目录。
重载方法允许我们使用 PathFilter 来限制匹配的文件和目录。如果把路径数组作为参数来调用listStatus 方法，其结果是与依次对每个路径调用此方法，再将FileStatus 对象数组收集在一个单一数组中的结果是相同的，但是前者更为方便。这在建立从文件系统树的不同部分执行的输入文件的列表时很有用。

## 九、Hadoop协议处理

相关注释文件为:

- `org/apache/hadoop/fs/FsUrlStreamHandlerFactory.java`
- `org/apache/hadoop/fs/FsUrlStreamHandler.java`
- `org/apache/hadoop/fs/FsUrlConnection.java`

---

### 引入

Hadoop 中通过集成 FileSystem 抽象类，使其能在不同的文件系统中可移植，要从 Hadoop 文件系统中读取文件，最简单的方法是使用 `java.net.URL` 打开数据流，从中读数据。

无论是 JAVA 原生的 `File` 类还是 Hadoop 的 `FileSystem` 类，他们在将文件对象转换成对应 URI(Uniform Resource Identifier, 统一资源标识符) 时的方法都继承了 `java.io.Serializable`。所标识的资源可以是文件，也可以是电子邮件地址等。如绝对 URI 由 URI 模式和模式特有部分组成，它们之间由冒号隔开，常用的模式有: file, http, hftp, mailto, telnet, hdfs, har, s3 等标准与非标准自定义模式。

并且很多模式如 http、ftp 和 hdfs 都定义了标准的层次式的模式特有部分，为:

```bash
//授权机构/路径?查询( [//<authority>]<path>[?query] )
以 URI "http://www.xdfilesystem.org/url.txt" 为例
- 模式：http
- 授权机构：www.xdfilesystem.org
- 路径资源映射：/url.txt
```

目前大部分 URI 使用 Internet 主机作授权机构:

```bash
用户信息@主机:端口 ( [userInfor@]<host>[:port] )
以 URI “ftp://xdu:passwd1@xdfilesystem.com:631/hadoopFS.pdf” 为例
- 授权机构：xdu:passwd1@xdfilesystem.com:631
- 用户信息：xdu:passwd1
- 主机：xdfilesystem.com
- 端口：83
- 模式：ftp( 占用端口 21 )
```

而为了让 JAVA 程序能够识别 Hadoop 的 hdfs URL 在 Hadoop 2 及后续版本中做了一些修改，其采用了两种方法: 

1. Hadoop 中不常用: `FsUrlStreamHandlerFactory` 实例调用 `java.net.URL` 对象的 `setURLStreamHandlerFactory()` 方法。但是由于每个 JVM 都只能调用一次这个方法，因此如果程序的其他组件已经声明了一个 `URLStreamHandlerFactory` 实例，其将无法使用这种方法从 Hadoop 中读取数据。
2. Hadoop 使用: 用 `FileSystem` API 来打开文件的输入流，Hadoop 文件系统中通过 Hadoop Path 对象来代表文件，将路径视为一个 Hadoop 文件系统的 URI，如 `file://localhost/document/test/1.txt`。

分别对这两种方法做检验确定 URL/URI 是如何被 Hadoop 处理的：

### 从 Hadoop URL 读取数据

```java
public class URLCat {
    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }
    public static void main(String[] args) throws Exception {
        InputStream in = null;
        try {
            in = new URL(args[0]).openStream();
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
```

开启 hadoop 服务：

```bash
hadoop URLCat file://localhost/home/parallels/Documents/text/1.txt
```

此时 URL 传入进来的 args[0] 为文件的地址，通过 openStream 建立输入流:

<img src="https://raw.githubusercontent.com/Warmchay/img/main/%E6%88%AA%E5%B1%8F2022-05-18%20%E4%B8%8A%E5%8D%888.21.32.png" style="zoom:50%;" />

`in` 表示有着相应 checksum 的 `localBoundedInputStream`，因为查找的是本地 file，因此 `fs = "LocalFS"`,`conf` 为 hadoop 的配置文件。

<img src="https://raw.githubusercontent.com/Warmchay/img/main/%E6%88%AA%E5%B1%8F2022-05-18%20%E4%B8%8A%E5%8D%888.24.01.png" style="zoom:67%;" />

`in` 初始 `readbuffer` 为空，copyBytes 从对应 URL 读取连续 4096 非空 Bytes。

<img src="https://raw.githubusercontent.com/Warmchay/img/main/%E6%88%AA%E5%B1%8F2022-05-18%20%E4%B8%8A%E5%8D%888.27.35.png" style="zoom:67%;" />

通过 `copyBytes` 中连续调用的` read`，读取 `file` 的 ASCII 码:

<img src="https://raw.githubusercontent.com/Warmchay/img/main/%E6%88%AA%E5%B1%8F2022-05-18%20%E4%B8%8A%E5%8D%888.42.44.png" style="zoom:67%;" />

由 RawLocalFileSystem 统计文本中共有 24 Bytes，返回通知 `in` 能够持续读取 `value=24` 的字节。

<img src="https://raw.githubusercontent.com/Warmchay/img/main/%E6%88%AA%E5%B1%8F2022-05-18%20%E4%B8%8A%E5%8D%888.44.34.png" style="zoom:50%;" />

经 `read` 读好的 24 个字节存入 Byte 数组中，赋值给 `InputStream` 内的 `buf` 。

<img src="https://raw.githubusercontent.com/Warmchay/img/main/%E6%88%AA%E5%B1%8F2022-05-18%20%E4%B8%8A%E5%8D%888.49.07.png" style="zoom:50%;" />

而后由 `OutStream` 输出从 url 读取的 `InputStream` 内的 `buf` 值:

<img src="https://raw.githubusercontent.com/Warmchay/img/main/%E6%88%AA%E5%B1%8F2022-05-18%20%E4%B8%8A%E5%8D%888.57.04.png" style="zoom:67%;" />

此时，控制台会返回读取的文本数据是什么：

<img src="https://raw.githubusercontent.com/Warmchay/img/main/%E6%88%AA%E5%B1%8F2022-05-18%20%E4%B8%8A%E5%8D%888.57.51.png" style="zoom:67%;" />

此处说明 Hadoop 通过 `IOUtils` 类，在 finally 子句中 `closeStream` 关闭对 `InputStream` 的读取。

`InputStream` 和 `OutPutStream` 通过 `read` 方法在 `System.out` 中复制数据。

而 `copyBytes` 的两个参数，第一个设置了用于复制的缓冲区 buf 大小为 4096，第二个设置了是否自行关闭数据流，默认参数 false 表示选择自行关闭输入流，因此 `System.out` 不用关闭其输入流。

#### 通过 FileSystem

```java
public class FileSystemCat {
    public static void main(String[] args) throws Exception {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        InputStream in = null;
        try {
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
```

开启 Hadoop 服务:

```bash
hadoop FileSystemCat file://localhost/home/parallels/Documents/text/1.txt
```

首先由 FileSystem.get 静态方法获取服务器的配置，通过设置配置文件的读取类文件来实现(如 `etc/hadoop/core-site.xml` )。在我们的调试代码中，通过给定的 URI 和权限 conf 文件确定要使用的文件系统:

<img src="https://raw.githubusercontent.com/Warmchay/img/main/%E6%88%AA%E5%B1%8F2022-05-18%20%E4%B8%8A%E5%8D%889.50.17.png" style="zoom:67%;" />

接着 InputStream 类对象 in 读取文件系统中给定 FileSystem path 的 URI 文件，后续的 copyBytes 与 URLCat 方法相同: 通过 InputStream 读取，经 System.out 复制给 OutPutStream 输出。

<img src="https://raw.githubusercontent.com/Warmchay/img/main/%E6%88%AA%E5%B1%8F2022-05-18%20%E4%B8%8A%E5%8D%889.54.15.png" style="zoom:67%;" />

**补充说明:** 

1. 无论是 URL 还是 FileSystem，他们在读取 URL/URI 时都会由 InputStream 确定读取的 fs 是 Local 类型 (LocalRawFileSystem) 还是 Distributed 类型 (HDFS)。
2. FileSystem 读取的 URI 为绝对路径，URL 读取的 URL 为相对路径。

### 协议处理

在引入部分实操了 Hadoop URL 类，其通过 FsUrlStreamHandlerFactory() 安装协议处理系统，再由 setURLStreamHandlerFactory() 确定 URL 获取文件资源输入流的系统位置在何处。

协议处理所做的是什么呢？举个例子: 用户如果在支持 hdfs 模式的浏览器地址栏上输入 URL "hdfs://xdFileSystem:port/path/xdu.jpg" 时，浏览器能正确出现流媒体文件 "xdu.jpg"  。在这个过程中 Java 所做的两部分内容为:

1. **协议处理** - 设计 Client 端与 Server 端间的交互，如 HDFS 中，Client 必须与 NameNode 通信确定 DataNode 位置后，与 DataNode 通信获取内容的正确路径。
2. **内容处理** - 将协议处理获得的内容展示出来，如显示 "xdu.jpg"。

Java 已经将协议处理分成 java.net 包中的四个部分来一起实现协议处理器，这 4 个类分别为: URL (具体类)、URLStreamHandler (抽象类)、URLConnection (抽象类) 和 URLStreamHandlerFactory (接口)。

Hadoop 文件系统的协议处理则继承了 Java 的后三个类，分别为 FsUrlStreamHandler、FsUrlConnection 和 FsUrlStreamHandlerFactory。

<img src="https://raw.githubusercontent.com/Warmchay/img/main/%E6%88%AA%E5%B1%8F2022-05-18%20%E4%B8%8A%E5%8D%8810.35.40.png" style="zoom:100%;" />

#### 安装协议处理系统

结合类图和第一节引用中调试的代码可以知道 Hadoop URL 中调用 URL.openStream() 获得相应 InputStream 对象时发生了什么：由于在引用部分使用的 URL 模式为 file, 在 URL 的构造函数中，file 模式通过 createURLStreamHandler() 由 setURLStreamHandler() 注册，实现 URLStreamHandlerFactory 接口的具体对象，此时的 Factory 任务为接受协议 file，为 file 协议找到合适的 URLStreamHandler，创建 file 流处理器对象并保存在 URL 对象的内部成员变量中。

由于每个应用程序最多只有一个 URLStramHandlerFactory，对于 Hadoop 这样的分布式系统若其试图实现另一个 Factory 时会跑出 Error。因此，Hadoop 用户可以通过 FsUrlStreamHandlerFactory 传入相应的 Hadoop Path(URI) 作文件的多次访问。

#### URL 字符串解析

获得文件流处理器后，Hadoop 中通过解析 URL 字符串创建知道使用 对应 protocol(协议) 来与 server 通信的 FsUrlConnection 子类。如在引用 1.2 的例子中，FsUrlConnection.getInputStream() 被调用来打开 "1.txt" 对应 URL 的一个输入流。

到这里，构造 URL 并打开对应 URL 资源的过程结束。

### 具体分析

#### 具体过程

1. 由引用 1.1 代码来做 URL 模式分析:

   ```java
   public class URLCat {
       static {
           URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
       }
       public static void main(String[] args) throws Exception {
           InputStream in = null;
           try {
               in = new URL(args[0]).openStream();
               IOUtils.copyBytes(in, System.out, 4096, false);
           } finally {
               IOUtils.closeStream(in);
           }
       }
   }
   ```

   由 FsUrlStreamHandlerFactory 根据输入的 URL 模式返回相应的 URLStreamHandler，如果没有指定 conf，则默认从 `core-default.xml` 中获取 Hadoop 已默认存储好的 FileSystem Class。

2. 由引用 1.2 代码做 URLStreamHandler 子类 FsUrlStreamHandler 读取文件的分析:

   ```java
   public class FileSystemCat {
       public static void main(String[] args) throws Exception {
           String uri = args[0];
           Configuration conf = new Configuration();
           FileSystem fs = FileSystem.get(URI.create(uri), conf);
           InputStream in = null;
           try {
               in = fs.open(new Path(uri));
               IOUtils.copyBytes(in, System.out, 4096, false);
           } finally {
               IOUtils.closeStream(in);
           }
       }
   }
   ```

   FsUrlStreamHandler 采用  FileSystem.get() 实现通过指定 conf 访问对应 URI ，能够根据传入的 conf 设置来支持用户实现好的 Hadoop 抽象文件系统，并能匹配在系统中已经进行了配置的多个具体文件系统的 URL 模式。 

   这样不需要为每一个具体文件系统对应的 URLStreamHandler 子类，而 FsUrlStreamHandlerFactory 和 FsUrlStreamHandler 也能够根据 conf 和 URI.create() 的模式做出使用什么 FileSystem 的判断。

   

#### 具体类分析

1. `FsUrlStreamHandlerFactory`

   <img src="https://raw.githubusercontent.com/Warmchay/img/main/%E6%88%AA%E5%B1%8F2022-05-18%20%E4%B8%8B%E5%8D%8812.09.48.png" style="zoom:67%;" />

   - 为 URLStreamHanlerFactory 的子类。

   - 两个构造函数，第一个提供默认的 Configuration 对象，因为 Configuration 类由延迟加载，所以通过在构造函数中调用 this.conf.getClass()，能确保 Configuration 对象加载了配置项。第二个指定 conf，引用1.1中使用的是默认 Configuration 对象，返回为: `Configuration: core-default.xml, core-site.xml, mapred-default.xml, mapped-default.xml, yarn-default.xml, yarn-site.xml`。

   - 调用 createURLStreamHandler() 以 protocol 形式指定 URL 模式，如引用 1.1 中以 file 作为 String 传递给这个方法。

     <img src="https://raw.githubusercontent.com/Warmchay/img/main/%E6%88%AA%E5%B1%8F2022-05-18%20%E4%B8%8B%E5%8D%8812.18.16.png" style="zoom:67%;" />

     该方法中首先判断目前的 conf 是否支持传入的 URL 模式，由 getFileSystemClass(protocol, conf) 完成。如果支持，返回万能 FsUrlStreamHandler 对象；否则返回 null，交由 JVM 处继续处理。

     

2. `FsUrlStreamHandler`

   <img src="https://raw.githubusercontent.com/Warmchay/img/main/%E6%88%AA%E5%B1%8F2022-05-18%20%E4%B8%8B%E5%8D%8812.25.26.png" style="zoom:67%;" />

   - openConnection() 返回新的 FsUrlConnection 对象用于建立 URL 的输入输出流。
   - 该类的两个构造函数的意义与 FsUrlStreamHandlerFactory 相同。

3. `FsUrlConnection`

   <img src="https://raw.githubusercontent.com/Warmchay/img/main/%E6%88%AA%E5%B1%8F2022-05-18%20%E4%B8%8B%E5%8D%8812.30.51.png" style="zoom:67%;" />

   - 构造函数需要确定 Configuration 对象和有可用的 URL 模式。

   - getInputStream() 方法继承了 URL.openStream()，返回对应 URL 的输入流。

   - 若获取的 InputStream is 为空，调用 connect()  使用 FileSystem.get() 获取 URL 对应的具体文件系统，并调用 open 的方法获取对应的输入流对象:

     <img src="https://raw.githubusercontent.com/Warmchay/img/main/%E6%88%AA%E5%B1%8F2022-05-18%20%E4%B8%8B%E5%8D%8812.36.59.png" style="zoom:67%;" />

## 十、再谈Hadoop filesystem的规范

官网; This is a specification of the Hadoop FileSystem APIs, which models the contents of a filesystem as a set of paths that are either directories, symbolic links, or files.

也就是说hadoop将文件系统的**内容**建模为一套路径，每个路径可以对应文件、目录或符号链接。

这里说到的文件系统的内容，可以认为就是一组数据，只要FileSystem能将这些数据按照目录的形式组织起来，并且可以对他们读写，你就可以将这些数据抽象为一个文件系统。

###### LocalFileSystem

对应uri为：`file://path/to/file`

最基础的例子就是`LocalFileSystem`

`LocalFileSystem`中的目录树与所运行主机本地的文件系统目录树一致

###### DistributedFileSystem

即hdfs的实现，文件以块的形式分布在多个主机的磁盘中存储

###### FtpFileSystem

对应的uri为：`ftp://path/to/file`

还有就是ftp，ftp也是符合这种规范的，我们可以用java的一些sdk去读写ftp文件，hadoop的开发者将这些操作封装在了FtpFileSystem中。

### S3FileSystem

云存储，比如：hadoop很早就支持的amazon的对象存储——s3、以及后续的阿里的OSS和腾讯云的COS

请注意，对于原有存储系统的抽象是个非常自由的过程，我们拿S3举例。大家可以把S3当成一个网盘，里面有你的文件。咱们就简单点，比如你在s3上只有1个文件

```
/a.txt
```

![image-20220520145433301](https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/06/01/fddc65408a6f598a2544845dac4275f7-image-20220520145433301-7eaee6.png)

amazon云官方提供了java sdk，你可以通过这个sdk访问你的s3

```
import org.jets3t.service.S3Service;
...
service.open("/a.txt")
...
```

现在你想要将S3抽象为一个文件系统，于是在hadoop中写了一个S3FileSystem，其中封装了这个SDK提供的接口，对外暴露出FileSystem的接口，也就是 rename，delete，open， write，create等

抽象出来的文件系统的目录树和S3中目录树完全一致

![image-20220520150651088](https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/06/01/cb7a386a3130549b51fc727bf7e3a1d9-image-20220520150651088-e390ec.png)

现在咱们换一种思路，把S3当做**磁盘**，回忆一下inode，我们可以在S3中使用inode和block来对文件进行管理。我们假设`a.txt`大小为100kb，定义一个块的大小是50k，则`a.txt`需要两个块来存储信息

请注意这里说是块和inode，其实inode和块在S3中仍然是文件，也就是用三个文件抽象为一个文件。

则最终转换关系变更如下

![image-20220520153116512](https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/06/01/d3a41aa4e08e02344b8d0d7fbe14660a-image-20220520153116512-7b4afc.png)

则在`S3FileSystem`中当我们要访问某个文件（"/a.txt"）时，首先读取该文件的inode（"/a.txt.inode"），读取到`a.txt`的元数据（创建时间，修改时间，文件大小等）和对应block的名字。然后通过读取块文件来实现读写文件

这里前者称为S3NativeFileSystem，后者称为S3FileSystem

## 十一、对Windows文件系统的支持

其实hadoop一直以来都有对windows的支持，在2.2.0之前，hadoop支持windows的方式是使用cygwin。Cygwin是一个在windows平台上运行的类UNIX模拟环境，包括了一套库，该库在win32系统下实现了POSIX系统调用的API；在这种情况下，jdk也需要使用linux的，很明显这种实现的方法更类似于使用一个虚拟机

2.2.0之后，hadoop增加了对hadoop的**原生支持**。hadoop发行包中增加了若干个dll链接库。以及windows中使用的`hadoop.cmd`等文件

### 主要矛盾点

这个问题的主要矛盾点在于**windows的某些行为与POSIX规范不一致**，以下两个是比较点型

1. Permission权限问题，windows的权限不符合POSIX权限模型。在某些特殊情况下，windows系统下的操作将会不符合预期

   比如文件的owner在linux中的格式是`username/groupname`

   而在windows中是`DOMAIN\\user`

   + username = `user`
   + groupname = `DOMAIN\\user`

2. 重命名问题

我们以这两个问题为例阐述如何添加对Windows文件系统的支持

### 重命名问题

POSIX规范中，rename时，比如将目录test名字改为test1时，如果test1原本就存在并且为空目录，test将会取代test1目录。大多数平台支持该规范，但是Windows不可以。

为了让LocalFileSystem在windows平台运行时符合该规范，可以

1. 如果目标目录存在且为空，删除该目录
2. 执行重命名操作

**实际代码实现**

![image-20220520215619060](https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/06/01/2534d5adb5beb37f947c912302f2405d-image-20220520215619060-bf1a40.png)

### 权限问题

在hadoop中有一个`NativeIO`类，这个类里面定义了一些针对不同操作系统（linux，macos，windows）的原生操作。

针对Windows主要是对权限的特殊设置

![image-20220520225244410](https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/06/01/a75e6444e0f5690362e5541072a82025-image-20220520225244410-857fd5.png)

![image-20220520215706996](https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/06/01/6f35b390828133af39e4a15cf70f4266-image-20220520215706996-00ee8c.png)

#### 借助winutil.exe 和 hadoop.dll

**hadoop原文**

```
Hadoop requires native libraries on Windows to work properly -that includes accessing the file:// filesystem, where Hadoop uses some Windows APIs to implement posix-like file access permissions.

This is implemented in HADOOP.DLL and WINUTILS.EXE.

In particular, %HADOOP_HOME%\BIN\WINUTILS.EXE must be locatable
```

**winutil源代码是c程序**，通过使用winutil，可以实现**某些**posix-like指令，其内部调用的是Win32 API，与Cygwin的实现方式不同。

实现的命令如下

+ chmod
+ chown
+ groups
+ hardlinks
+ ls

在`LocalFileSystem`中在`setOwner`和`getOwner`方法中都是通过执行命令行指令实现的，分别为

1. `ls`
2. `chown`

前者可以获取文件owner信息，后者用于修改owner信息。

这两个命令都是linux支持但windows不原生支持的，借助winutil可以实现这两个指令，执行方法

```
winutil.exe ls C:/path/to/file
winutil.exe chown [OWNER][:[GROUP]] C:/path/to/file
```

## 十二、XdFileSystem

我们组本次实现了可以做到不借助winutil和hadoop自带的NativeIO类在hadoop-2.0.0中运行一个可以在Windows7上运行的文件系统，借助java.nio包的某些操作实现了权限的获取，可以做到大部分基本操作，并且成功运行了MapReduce的wordcount例子

### 目录结构

```shell
.
├── README.md
├── pom.xml					// 配置文件
└── src							// 源码
    └── main
        └── java
            └── org.apache.hadoop.fs.xd				// 确定包名
                ├── XdFSFileInputStream.java	// InputStream实现
                ├── XdFSFileOutputStream.java // OutputStram实现
                └── XdFileSystem.java					// XidianFileSystem类实现
```

### 关键代码

关键代码是三个`.java`文件。InputStram和OutputStram实现主要是封装了`java.io.FileInputStream`和`FileOutputStream`，增加了随机访问的支持

![image-20220608224800654](https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/06/08/23a6320bf0f8aecbb87022f4e5823672-image-20220608224800654-7cbb6c.png)

### 原生支持实现

1. 权限问题
2. 特定系统差异问题

#### 权限问题

权限问题的解决没有使用winutil.exe。而是使用了`java.nio`中的`FileOwnerAttributeView `以及`UserPrincipal`等

比如，以下是获取windows下文件权限的代码

```java
private void loadPermissionInfo() {
            IOException e = null;
            try {
                String owner = null;
                String group = null;
                java.nio.file.Path absolutePath = Paths.get(path.toUri().getPath());
                FileOwnerAttributeView ownerAttributeView = Files.getFileAttributeView(absolutePath, FileOwnerAttributeView.class);
                UserPrincipal up = ownerAttributeView.getOwner();
                StringTokenizer t = new StringTokenizer(up.getName(), "\\");
//                System.out.println("owner: " + owner.getName());
                group = t.nextToken();
                owner = t.nextToken();
                setOwner(owner);

                if(checkGroup(up.getName(), path.toUri().getPath()) != null){
                    setGroup(up.getName());
                } else {
                    setGroup(group + "\\None");
                }
            } catch (Shell.ExitCodeException ioe) {
                if (ioe.getExitCode() != 1) {
                    e = ioe;
                } else {
                    setPermission(null);
                    setOwner(null);
                    setGroup(null);
                }
            } catch (IOException ioe) {
                setOwner(null);
                setGroup(null);
            } finally {
                if (e != null) {
                    throw new RuntimeException("Error while running command to get " +
                            "file permissions : " +
                            StringUtils.stringifyException(e));
                }
            }
        }
```

### 运行实例和截图

以下是我们执行的命令，运行截图和核心代码截图

```shell
hadoop fs -ls xidian:///test
hadoop fs -mkdir xidian:///test/new_folder
hadoop fs -cp xidian:///test/test.txt xidian://test/data
hadoop fs -cp xidian:///test/test.txt xidian:///test/data
hadoop fs -copyFromLocal file:///tmp/back.txt xidian:///test/data
hadoop fs -cat xidian:///test/test.txt
```

![img](https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/05/21/fe694c6ffcefbbfb561246c7cebacc6f-1652711523355-a6477375-a5e0-40ad-8e6d-74e755e6ab81-157900.png)

![img](https://cdn.jsdelivr.net/gh/xinwuyun/pictures@main/2022/05/21/ace457e4135d42d3d7c80424b184a17a-1652711633592-5fc73dee-b02b-4206-a964-7138ac46a28c-72ec6e.png)

### MapReduce运行结果

```
PS C:\hadoop\hadoop-2.7.3\share\hadoop\mapreduce> hadoop jar hadoop-mapreduce-examples.jar wordcount xidian:///test/data xidian:///test/output1
22/05/16 23:10:12 INFO xd.XdFileSystem: *** Using Xd file system ***
22/05/16 23:10:12 INFO Configuration.deprecation: session.id is deprecated. Instead, use dfs.metrics.session-id
22/05/16 23:10:12 INFO jvm.JvmMetrics: Initializing JVM Metrics with processName=JobTracker, sessionId=
22/05/16 23:10:12 INFO input.FileInputFormat: Total input paths to process : 3
...
...
22/05/16 23:10:14 INFO mapred.LocalJobRunner: reduce task executor complete.
22/05/16 23:10:14 INFO mapreduce.Job: Job job_local1418680143_0001 running in uber mode : false
22/05/16 23:10:14 INFO mapreduce.Job:  map 100% reduce 100%
22/05/16 23:10:14 INFO mapreduce.Job: Job job_local1418680143_0001 completed successfully
22/05/16 23:10:14 INFO mapreduce.Job: Counters: 35
        File System Counters
                FILE: Number of bytes read=1186519
                FILE: Number of bytes written=2338765
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                XIDIAN: Number of bytes read=0
                XIDIAN: Number of bytes written=43
                XIDIAN: Number of read operations=0
                XIDIAN: Number of large read operations=0
                XIDIAN: Number of write operations=0
        Map-Reduce Framework
                Map input records=15
                Map output records=15
                Map output bytes=146
                Map output materialized bytes=151
                Input split bytes=274
                Combine input records=15
                Combine output records=11
                Reduce input groups=5
                Reduce shuffle bytes=151
                Reduce input records=11
                Reduce output records=5
                Spilled Records=22
                Shuffled Maps =3
                Failed Shuffles=0
                Merged Map outputs=3
                GC time elapsed (ms)=0
                Total committed heap usage (bytes)=1507328000
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=0
        File Output Format Counters
                Bytes Written=43
```

该代码实际上对于权限问题的很多细节不能做到**任何情况下**都能完美执行。

实际上缺少的就是hadoop在后续的NativeIO类中的接口支持。其中的很多细节我们还没能搞清楚。





# hadoop-filesystem-analysis

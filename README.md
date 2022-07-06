# Webcrawl analysis
In this blog post I will take you through my hands-on experience with programming an analysis on webcrawl data using Spark in Scala. In the analysis we start of with extracting all the text written on the website(s) and counting the occurences of words, after this we perform a letter frequency and at last we extract email addresses.

## Getting the data
Before we can do any analysis, we first have to have data to do the analysis on. While programming and testing I used data from my favorite blog [Wait but Why](https://waitbutwhy.com/). I retrieved the data from the website using `wget`.

```sh
wget -r -l 3 "https://waitbutwhy.com/" --delete-after --no-directories --warc-file="waitbutwhy"
```

## Setting up Spark and Warc
Once the data was downloaded I created the start of my analysis program which simply sets up the Spark session and loads the Warc file into Hadoop.

```scala
// ------------------------------------------------------------
// setup spark configuration
// ------------------------------------------------------------

val sparkConf = new SparkConf()
        .setAppName("RUBigData WARC4Spark 2021")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .registerKryoClasses(Array(classOf[WarcRecord])
    )
implicit val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
val sc = sparkSession.sparkContext

// ------------------------------------------------------------
// load the warcfile into hadoop
// ------------------------------------------------------------

val fname = "waitbutwhy"
val warcfile = s"file:///opt/hadoop/rubigdata/${fname}.warc.gz"

val warcs = sc.newAPIHadoopFile(
        warcfile,
        classOf[WarcGzInputFormat],             // InputFormat
        classOf[NullWritable],                  // Key
        classOf[WarcWritable]                   // Value
    ).cache()
```

## Filtering the records
Now that we have the Warc file available to us, we can start extracting the data we want for our analysis. We filter out all of the records which have a succesfull HTTP status code (2xx) and we filter out those which contain either plain text or html, so we will not process images or videos and such.

```scala
// ------------------------------------------------------------
// filter the records to only text and html files
// ------------------------------------------------------------

val records_filtered = warcs.
    map(wr => wr._2.getRecord()).
    filter(_.isHttp()).
    filter(r => {
        200 <= r.getHttpResponse().getMessage().getStatus() &&
        299 >= r.getHttpResponse().getMessage().getStatus()
    }).
    filter{
        case r => r.getHttpHeaders().get("Content-Type") match {
            case null => false
            case _ => (
                r.getHttpHeaders().get("Content-Type").startsWith("text/html") ||
                r.getHttpHeaders().get("Content-Type").startsWith("text/plain")
            )
        }
    }
```

## Textifying the records
With the records filtered to html and plain text we continue by removing all of the html markup still left in these records. We do this using `Jsoup`, which has a useful function that can parse and extract all the text from an html file. After this step we are left with just the bits we want to analyze.

```scala
// ------------------------------------------------------------
// take out the text from the html files
// ------------------------------------------------------------

val records_texts = records_filtered.
    map(_.getHttpStringBody()).
    map(wb => {
        val doc = Jsoup.parse(wb)
        doc.body().text()
    })
```

## Tokenizing the records
Now that we have reduced our records to the texts we want to analyze we can tokenize these texts into words. We cast all the words to lowercase, split on spaces and remove all the characters which do not belong to words such as `"`, `(`, `)`, `.`, `?`, all numbers, etc. Doing it this way leaves us with a lot more sensible tokens.

```scala
// ------------------------------------------------------------
// tokenize the texts into words
// ------------------------------------------------------------

val records_words = records_texts.
    map(_.toLowerCase()).
    flatMap(_.split(" ")).
    map(strip(_, "#.(),_1234567890&…?!:-–—»→©*/”“↩")).
    filter(_.length > 0)
```

## Counting the words
We have now reached the point were we can actually count the words and display our results. In order to do this, we first convert all of our tokens to a key-value pair, with the word being the key and a starting value of one. After we have created all of these pairs we reduce them by their key and add up all the values. Finally, we sort the pairs by value in descending order.

```scala
// ------------------------------------------------------------
// process the words and do the actual counting
// ------------------------------------------------------------

val records_words_processed = records_words.
    map(word => (word, 1)).
    reduceByKey((a, b) => a + b).
    sortBy(_._2, false)

println(records_words_processed.count)
records_words_processed.take(20).foreach({ case (word, count) => println(f"$word\t$count") })
```

## Compiling and running
Now that our program is complete we can compile and run it, on our own machine this is fairly easy using the Docker container. Here, we can run the following two commands to compile the program into a single `jar`-file which is executed by Spark.

```sh
$ sbt assembly
$ spark-submit target/scala-2.12/RUBigDataApp-assembly-1.0.jar
```

Running these commands yields the following results!

```
the	11758
a	5900
to	5718
of	5594
and	5234
in	3423
that	2700
you	2696
is	2688
it	2056
on	1622
your	1581
with	1488
i	1475
for	1460
be	1428
this	1283
are	1257
about	1249
but	1247
```

## Letter frequencies
We know how to perform a word count now, a letter frequency analysis follows quite logically from here. This time we tokenize the texts into characters and filter out all of the letters. After this we tally the letters and convert them to percentages.

```scala
// ------------------------------------------------------------
// tokenize the text into letters
// ------------------------------------------------------------

val records_letters = records_texts.
    map(_.toLowerCase()).
    flatMap(_.toCharArray()).
    filter(letter => 'a' <= letter && letter <= 'z')

// ------------------------------------------------------------
// process the letters and do the actual counting
// ------------------------------------------------------------

val records_letters_counted = records_letters.
    map(letter => (letter, 1)).
    reduceByKey((a, b) => a + b)

val number_of_letters = records_letters.count()
val records_letters_processed = records_letters_counted.
    map({ case (letter, count) => (letter, count.toDouble / number_of_letters * 100) }).
    sortBy(_._2, false)

records_letters_processed.foreach({ case (letter, frequency) => println(f"$letter $frequency%.2f%%") })
```

Executing this code we get the list of frequencies shown below! This is quite on par with the data shown on [this Wikipedia page](https://en.wikipedia.org/wiki/Letter_frequency).

```
e 11.99%
t 9.53%
o 7.99%
a 7.71%
i 7.43%
n 6.90%
s 6.32%
r 5.83%
h 5.00%
l 4.31%
u 3.32%
d 3.23%
c 2.91%
m 2.51%
y 2.28%
p 2.26%
g 2.16%
w 2.14%
f 1.95%
b 1.74%
v 1.03%
k 0.91%
x 0.20%
j 0.14%
z 0.11%
q 0.10%
```

## Email extraction
Apart from doing language analysis one could also use all of the data from webcrawls for evil use, one such uses is email extraction to send well-beloved spam. Extracting emails from a piece of text is extremely simple, we just use a regular expression (taken from the web) and match it against all our data!

```scala
// ------------------------------------------------------------
// extract the emails from the text
// ------------------------------------------------------------
val pattern = """([\w\.!#$%&*+/=?^_`{|}~-]+)@([\w]+)([\.]{1}[\w]+)+""".r
val records_emails = records_texts.
    flatMap(pattern.findAllIn(_)).
    distinct()

println(records_emails.count)
records_emails.foreach(println)
```

Using this code on the Wait but Why data provides us the following email addresses.

```
office@genyamamoto.jp
andrew@waitbutwhy.com
mailbag@waitbutwhy.com
```

## Cluster preparations
We are now ready to prepare for the big boy cluster! We first connect to the VPN at university and then create the docker container which we will use to interface with the cluster.
```
$ export GITHUB_USERNAME=Borroot
# docker create --name redbad -e HADOOP_USER_NAME=${GITHUB_USERNAME} -it rubigdata/redbad
```

With the cluster created we copy our program from the previous docker container into the new one, we also had to log in as root to set the file ownership correctly.

```
# docker cp program redbad:/opt/hadoop
# sudo docker start redbad
# docker exec -it --user root redbad /bin/bash
# chown -R hadoop:hadoop program
```

We also change the warc file that is being read, first to a single small one.

```scala
val warcfile = "/single-warc-segment/CC-MAIN-20210410105831-20210410135831-00000.warc.gz"
```

After creating the container we first tested the provided example program, which worked! So I very optimistically continued and gave my program a run, if only I knew...

## Null pointer exception!
```
$ sbt assembly
$ spark-submit --deploy-mode cluster --queue default target/scala-2.12/RUBigDataApp-assembly-1.0.jar
```

When I ran my program for the first time `NullPointerException`'s appeared all over the place. After getting familiar with the web interface and hours of debugging later I [discovered](http://rbdata01.cs.ru.nl:19888/jobhistory/logs/rbdata11:36755/container_e02_1623272363921_0559_01_000003/container_e02_1623272363921_0559_01_000003/Borroot/stderr/?start=0&start.time=0&end.time=9223372036854775807) (first exception trace shows line 75 of the source file throws the error) that the problem lies with Jsoup. Remember this code?

```scala
// ------------------------------------------------------------
// take out the text from the html files
// ------------------------------------------------------------

val records_texts = records_filtered.
    map(_.getHttpStringBody()).
    map(wb => {
        val doc = Jsoup.parse(wb)
        doc.body().text()
    })
```

Apparently the `body()` function sometimes returns null, even though [the documentation](https://jsoup.org/apidocs/org/jsoup/nodes/Document.html#body()) says it should return an empty body if there is no body in the HTML structure, meaning it should never return null. Consequently the `text()` function would be called on the null object causing the exception. Once found out this was easily solved by adding a filter for null, I additionally added a `hasText()` check, just to be sure.

```scala
// ------------------------------------------------------------
// take out the text from the html files
// ------------------------------------------------------------

val records_texts = records_filtered.
    map(_.getHttpStringBody()).
    map(Jsoup.parse(_).body()).
    filter(_ != null).
    filter(_.hasText()).
    map(_.text())
```

## Memory management
'`OutOfMemoryError`: Java heap space', that is the sentence that haunted me in my sleep, I got memory problems, but these were way more difficult to track down than the `NullPointerException`. Every time that I executed my program some executers would fail due to [this error](http://rbdata01.cs.ru.nl:19888/jobhistory/logs/rbdata05:35205/container_e02_1623272363921_0775_02_000003/container_e02_1623272363921_0775_02_000003/Borroot/stderr/?start=0&start.time=0&end.time=9223372036854775807), or they would simply be [killed](http://rbdata01.cs.ru.nl:19888/jobhistory/logs/rbdata11:36755/container_e02_1623272363921_0775_02_000005/container_e02_1623272363921_0775_02_000005/Borroot/stderr/?start=0&start.time=0&end.time=9223372036854775807) because of memory management issues resulting in an `ExecutorLostFailure`.

After a long time of debugging I found out (with the great help of our teacher) that the problem is that I was caching the whole warc file into memory, but the file is way to big, removing the `cache()` call solved the memory problem!

## Outputting to a file
Now that we can finally run all the analysis we want to improve output a bit by writing to a file instead of to `stdout`. An extremely simplified version of writing to a file is shown below.

```scala
val outputfile = "hdfs:///user/Borroot/test"
var output = Seq[String]()

output = output :+ "hello world"
output = output :+ "this is a test"

val outputrdd = sc.parallelize(output)
outputrdd.coalesce(1).saveAsTextFile(outputfile)
```

If we now read this file we can see the content is succesfully written there.

```
$ hdfs dfs -cat /user/Borroot/test/part-00000
hello world
this is a test
```

The final code is rewritten a bit just so that it using this file outputting method writing to the directories `count`, `frequency` and `email`. The RDD's can be converted using `take()` or `collect()` to something writable. Make sure to remove all the directories before every run, using the command shown below.

```
$ hdfs dfs -rm -r -f /user/Borroot/{count,frequency,email}/
```

Just to give an example, below you can see the outputting for the letter frequencies.

```scala
val outputdir_frequency = "hdfs:///user/Borroot/frequency"
sc.parallelize(
        records_letters_processed.
        collect().
        map({ case (letter, frequency) => f"$letter $frequency%.2f%%" })
    ).coalesce(1).saveAsTextFile(outputdir_frequency)
```

## Number of executors
When we look at the [number of stages](http://rbdata01.cs.ru.nl:18080/history/application_1623272363921_0887/1/stages/) we can see that most tasks take 10 stages, therefore it can be very efficient to use 10 executors, we can instruct Spark to use 10 by passing the commandline argument `--num-executors 10` to `spark-submit`.

## Final test
With everything working now it is time to put my program to its final test and run it over way more data. We can do this by simply changing the `warcfile` value in our program to the segment folder instead of a single warc file in there.

```scala
val warcfile = "/single-warc-segment/"
```

After running this program for about 1.3 hours on the gold queue, the [web ui](http://redbad01.cs.ru.nl:8088/cluster/scheduler?openQueues=Queue:%20gold#Queue:%20bronze%23Queue:%20gold%23Queue:%20gold) showed that it has only progressed towards 10%... It did not even start the email extraction which usually takes by far the longest due to the complex regular expression matching. So instead I decided to test my program on a bit smaller dataset of about 11.1GB by using all the files matching the name stated below.

```scala
val warcfile = "/single-warc-segment/CC-MAIN-20210410105831-20210410135831-0000?.warc.gz"
```

Running my program over this data went very smoothly, it finished in about 45 minutes and collected over 75.000 email addresses. Some of the results are shown below.

```
5885901 the
4029906 to
3930830 and
3911746 a
3585254 de
3392601 of
3246932 in
1951304 for
1494705 i
1490106 is
1335040 you
1316450 on
1303159 tracking
...
```

Should we be worried about 'tracking' being the 13th most common word found in the crawl data...?

```
e 11.45%
a 8.98%
i 7.60%
t 7.17%
n 7.08%
o 7.03%
r 6.94%
s 6.66%
l 4.64%
c 3.97%
d 3.67%
u 3.34%
m 3.08%
h 2.74%
p 2.73%
g 2.32%
b 1.83%
f 1.60%
y 1.49%
k 1.48%
v 1.37%
w 1.21%
z 0.55%
j 0.51%
x 0.34%
q 0.22%
```

All of the results can also be viewed using the commands shown below.

```
$ hdfs dfs -cat /user/Borroot/count/part-00000 | less
$ hdfs dfs -cat /user/Borroot/frequency/part-00000 | less
$ hdfs dfs -cat /user/Borroot/email/part-00000 | less
```

## Resit optimalisations
The regular expression matching is, as said in the previous section, rather slow, since it is quite a complex expression and there is a huge amount of data. In order to improve upon this bottleneck we will filter the data more before the regex matching, this way the complex regular expression will run over a lot less data than before thus improving the performance of the algorithm.

The old and new way of email extraction are shown below.

```scala
val pattern = """([\w\.!#$%&*+/=?^_`{|}~-]+)@([\w]+)([\.]{1}[\w]+)+""".r
val records_emails = records_texts.
  flatMap(pattern.findAllIn(_)).
  distinct()
```

```scala
val pattern = """([\w\.!#$%&*+/=?^_`{|}~-]+)@([\w]+)([\.]{1}[\w]+)+""".r
val records_emails = records_texts.
  flatMap(_.split(" ")).
  filter(_.contains("@")).
  map(pattern.findFirstIn(_)).
  filter(_ != None).
  map(_.get).
  distinct()
```

In the new version instead of running the regular expression over the whole text blobs, we first split the text into tokens seperated by spaces and filter out those tokens which contain a @. These operations are fast, especially the `contains` test is rather straightforward. Now we run the email regular expression over those tokens which contained a @, this is an immenseley smaller number of tokens than before! After this we filter out the tokens which were no email addresses and at last we map the `Some` entities to `String` entities using the `get` property.

This seemingly small change improved the running time we had [previously](http://rbdata01.cs.ru.nl:18080/history/application_1623272363921_1003/1/jobs/) (see previous section) from 44 minutes to a mere 15 minutes [now](http://rbdata01.cs.ru.nl:18080/history/application_1623272363921_3080/1/jobs/)! Before the optimisation I also tried to run the email extraction on the WARC segment at `/single-warc-segment`, after several days (about six) I killed the program because it seemed to never finish. After the optimisation I ran the email extraction again over this segment, it now [finished in 15.6 hours](http://rbdata01.cs.ru.nl:18080/history/application_1623272363921_3083/1/jobs/), huge performance boost! During this final big test I managed to collect a whopping 1.176.572 email addresses.

## Code reference
Just as a reference the final code is included below.

```scala
package org.rubigdata

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.hadoop.io.NullWritable
import de.l3s.concatgz.io.warc.{WarcGzInputFormat,WarcWritable}
import de.l3s.concatgz.data.WarcRecord

import org.jsoup.Jsoup
import org.jsoup.nodes.{Document,Element}
import collection.JavaConverters._

import org.apache.commons.lang.StringUtils.strip

object RUBigDataApp {
  def main(args: Array[String]) {

    // ------------------------------------------------------------
    // setup files for output
    // ------------------------------------------------------------

    val outputdir_count = "hdfs:///user/Borroot/count"
    val outputdir_frequency = "hdfs:///user/Borroot/frequency"
    val outputdir_email = "hdfs:///user/Borroot/email"

    // ------------------------------------------------------------
    // setup spark configuration
    // ------------------------------------------------------------

    val sparkConf = new SparkConf()
      .setAppName("Big Boy Analyzer!")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[WarcRecord])
    )
    val session = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = session.sparkContext

    // ------------------------------------------------------------
    // load the warcfile into hadoop
    // ------------------------------------------------------------

    val warcfile = "/single-warc-segment"
    val warcs = sc.newAPIHadoopFile(
        warcfile,
        classOf[WarcGzInputFormat],             // InputFormat
        classOf[NullWritable],                  // Key
        classOf[WarcWritable]                   // Value
      )

    // ------------------------------------------------------------
    // filter the records to only text and html files
    // ------------------------------------------------------------

    val records_filtered = warcs.
      map(wr => wr._2.getRecord()).
      filter(_.isHttp()).
      filter(r => {
        200 <= r.getHttpResponse().getMessage().getStatus() &&
        299 >= r.getHttpResponse().getMessage().getStatus()
      }).
      filter({
        case r => r.getHttpHeaders().get("Content-Type") match {
          case null => false
          case _ => (
            r.getHttpHeaders().get("Content-Type").startsWith("text/html") ||
            r.getHttpHeaders().get("Content-Type").startsWith("text/plain")
          )
        }
      })

    // ------------------------------------------------------------
    // take out the text from the html files
    // ------------------------------------------------------------

    val records_texts = records_filtered.
      map(_.getHttpStringBody()).
      map(Jsoup.parse(_).body()).
      filter(_ != null).
      filter(_.hasText()).
      map(_.text())

    // ------------------------------------------------------------
    // tokenize the texts into words
    // ------------------------------------------------------------

    val records_words = records_texts.
      map(_.toLowerCase()).
      flatMap(_.split(" ")).
      map(strip(_, "|+@$^=#.(),_1234567890&…?!:-–—»→©*/”“↩")).
      filter(_.length > 0)

    // ------------------------------------------------------------
    // process the words and do the actual counting
    // ------------------------------------------------------------

      val records_words_processed = records_words.
        map(word => (word, 1)).
        reduceByKey((a, b) => a + b).
        sortBy(_._2, false)

      sc.parallelize(
          records_words_processed.
          take(1000).
          map({ case (word, count) => f"$count $word" })
        ).coalesce(1).saveAsTextFile(outputdir_count)

    // ------------------------------------------------------------
    // tokenize the text into letters
    // ------------------------------------------------------------

    val records_letters = records_texts.
      map(_.toLowerCase()).
      flatMap(_.toCharArray()).
      filter(letter => 'a' <= letter && letter <= 'z')

    // ------------------------------------------------------------
    // process the letters and do the actual counting
    // ------------------------------------------------------------

    val records_letters_counted = records_letters.
      map(letter => (letter, 1)).
      reduceByKey((a, b) => a + b)

    val number_of_letters = records_letters.count()
    val records_letters_processed = records_letters_counted.
      map({ case (letter, count) => (letter, count.toDouble / number_of_letters * 100) }).
      sortBy(_._2, false)

    sc.parallelize(
        records_letters_processed.
        collect().
        map({ case (letter, frequency) => f"$letter $frequency%.2f%%" })
      ).coalesce(1).saveAsTextFile(outputdir_frequency)

    // ------------------------------------------------------------
    // extract the emails from the text
    // ------------------------------------------------------------

    val pattern = """([\w\.!#$%&*+/=?^_`{|}~-]+)@([\w]+)([\.]{1}[\w]+)+""".r
    val records_emails = records_texts.
      flatMap(_.split(" ")).
      filter(_.contains("@")).
      map(pattern.findFirstIn(_)).
      filter(_ != None).
      map(_.get).
      distinct()

    sc.parallelize(
        records_emails.
        collect()
      ).coalesce(1).saveAsTextFile(outputdir_email)

    // ------------------------------------------------------------
    // stop spark
    // ------------------------------------------------------------

    session.stop()
  }
}
```

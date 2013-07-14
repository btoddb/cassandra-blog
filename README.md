cassandra-blog
==============

An example Cassandra + Hector application to show how a blog application could be implemented using Cassandra.

Installation
============

1 – get latest Cassandra and install (http://cassandra.apache.org/)
2 – mkdir /var/log/Cassandra /var/lib/Cassandra
3 – chown above dirs If needed
4 – run it ($CASS_HOME/bin/cassandra)
5 – create schema ($CASS_HOME/bin/cassandra-cli < blog_schema.txt)
6 – build code (mvn clean package)

Running
=======

Run the code (java -cp target/cassandra-blog-1.0.0-SNAPSHOT.jar com.btoddb.blog.BlogMain) without any parameters and "usage" should be displayed.

Blog away!


# Here are the docker-compose files for development

Requirements: 
* docker desktop 4.22+ is required to use the `include` keyword. See more in the [release]( https://www.docker.com/blog/docker-desktop-4-22/).
* If you can't upgrade the docker version to use the new features, then use [legacy-compose-mysql-pulsar3.yaml](legacy-compose-mysql-pulsar3.yaml). You can modify it for other databases.

Compose files:
* mysql8-pulsar.yaml is for using MySQL8 + Pulsar as xdb dependencies
* postgres12-pulsar.yaml is for using Postgres 12 + Pulsar as xdb dependencies
* pulsar3.yaml is the shared component of Apache Pulsar3.1. It's "included" by other files
* integ-all.yaml is for run all dependencies for integ tests locally

To run it:
docker-compose -f <filename> up

## How it works
* For now XDB uses [Pulsar Debezium connector to connect from a database CDC](https://pulsar.apache.org/docs/3.1.x/io-cdc-debezium/#usage-1)
  * Underneath, Pulsar Debezium connector is using Debezium connector to consume CDC, and implements a KafkaConnect adapter to send to Pulsar directly(without Kafka)
  * This means that it doesn't contain other Debezium's features. It is just on top of Debezium connector.
  * In the future, we may improve this architecture (e.g. we may need [filtering](https://debezium.io/documentation/reference/2.3/transformations/filtering.html) to improve the efficiency)
* A database is required to enabled CDC. The setup is different per database.
  * Postgres
  * MySQL
  * [Others](https://debezium.io/documentation/reference/2.3/connectors/index.html)
* Debezium also provides [a set of database images](https://github.com/debezium/container-images) that include CDC setup
  * [All versions of Postgres](https://github.com/debezium/container-images/tree/main/postgres)
  * [All versions of MySQL with example schema](https://github.com/debezium/container-images/tree/main/examples/mysql)
  * For production setup, users can refer to the documentation, and follow the setup like: 
    * [MySQL config](https://github.com/debezium/container-images/blob/main/examples/mysql/2.4/mysql.cnf)
    * [Postgres config](https://github.com/debezium/container-images/blob/main/postgres/16/postgresql.conf.sample) which used in [startup](https://github.com/debezium/container-images/blob/main/postgres/16/Dockerfile#L51)

### Tips
To test consuming messages, you can use command like(Java 11+ is required for JAVA_HOME):
```shell
bin/pulsar-client consume -s "sub-products" public/default/dbserver1.inventory.products -n 0
```
or 
```shell
JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.7.jdk/Contents/Home bin/pulsar-client consume -s "sub-test2" public/default/dbserver1.public.test2 -n 0
```

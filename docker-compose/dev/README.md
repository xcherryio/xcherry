# Here are the docker-compose files for development

Requirements: 
* docker desktop 4.22+ is required to use the `include` keyword. See more in the [release]( https://www.docker.com/blog/docker-desktop-4-22/).

Compose files:
* mysql8-pulsar.yaml is for using MySQL8 + Pulsar as xdb dependencies
* postgres12-pulsar.yaml is for using Postgres 12 + Pulsar as xdb dependencies
* pulsar3.yaml is the shared component of Apache Pulsar3.1. It's "included" by other files
* integ-all.yaml is for run all dependencies for integ tests locally

To run it:
docker-compose -f <filename> up


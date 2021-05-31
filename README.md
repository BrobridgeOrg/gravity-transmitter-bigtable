# Gravity Transmitter for GCP BigTable

The gravity transmitter is used to write data to GCP BigTable database.

## Pre-require

You need create Table to Bigtable first

first need to install gcp cloud SDK 

and run command under to create table

Important: please set up the InstanceID and ProjectID

```shell
go run bigtable_create_table
```

## Installation and Run

You can compile gravity-transmitter-bigtable with the following commands:

```shell
go build ./cmd/gravity-transmitter-bigtable
go ./gravity-transmitter-bigtable
```
## License

Licensed under the MIT License

## Authors

Copyright(c) 2020 Dagin Wu <<daginwu@brobridge.com>>

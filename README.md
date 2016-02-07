Run the proxy:

```go run cmd/main.go --vitess_server=localhost:15999 --keyspace=test_keyspace --shard=0```

Connect to the proxy:

```mysql -h127.0.0.1 -P4000 -uroot -p```

Now all your MySQL requests will be redirected to Vitess.





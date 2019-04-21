package main

import clientv3 "go.etcd.io/etcd/clientv3"

// whenever leader processes a request, it should see if it has any messages in lead

func StartLeaderProcess(cli *clientv3.Client, lead chan bool) {

}

# Control Flow
All the files or directory given here are found in `~/go/src/napoleon/`, unless mentioned particularly. This will create a test setup on `localhost`.
1. Instantiate `etcd` datastore nodes on `localhost` by running shell script files present in directory `etcd/`.  
1.1 Download `etcd` from this [link](https://github.com/etcd-io/etcd/releases), `etcd-v3.3.12-linux-amd64.tar.gz` on `~/Desktop/`.  
1.2 Execute <pre>sh etcd.sh</pre>this command will extract `etcd-v3.3.12-linux-amd64.tar.gz` into `/tmp/test-etcd/`  
1.3 Instantiate `etcd datastore` by running <pre>sh one.sh 
sh two.sh 
sh three.sh
</pre>
This will create three instances of etcd datastore on `localhost` listening on port numbers `2379`, `2380`, `22379`, `22380`, `32379`, `32380`.  
1.4 You can also create `etcd` instances on various machines by following [play.etcd.io](http://play.etcd.io/install)  
2. Now, we need to insert the physical machines that can be added into the `cluster`     
2.1 You can send a `gRPC client request` to `etcd datastore` according to format of `PM` presented in `types/types.proto`  
2.2 You can also use `~/go/src/physical/pm.go` to create by changing parametes in file and running <pre>go run pm.go</pre> 
2.3 You can add as many physical machines as you want.  
3. Once the physical machines are added , you need to run the `master` by running `main.go`<pre>go run main.go -cport=portno -rpc=noofreadrpc -name=hostname -ip=ipaddress -log=logname</pre>   
This will start the master with listening on `portno`, and set no. of read rpc with `no of readrpc` and set hostname as `hostname` with ipaddress `ipaddress` and the output log of master will be stored in `logname`.  
Example:<pre>go run main.go -cport 8079 -rpc 1 -name=s1 -ip=localhost -log=controller1</pre>
The above command will start controller `s1` on `localhost` listening on port no `8079` with no of read rpc `1` and the log of master will be sent to `controller1.log`   
4. The physical machines can be simulated by running `~/go/src/stub/stub.go`<pre>go run stub.go -ip=ipaddress -port=portno -pmid=pmid -cpu=noofcpu -mem=memory</pre>  
This will simulate the `pm` with `pmid` by running with ip `ipaddress`, portno `portno`, no of cpu's `noofcpu`, amount of memory `memory`  
Example: <pre>go run stub.go -port=10007 -pmid=pm_8</pre>
In the above example, this will create a stub of physical machine `pm_8` with default cpu `25000` and default memory `10000` on `localhost` listening on port no `10007`. Here both memory and cpu is normalized on a scale.  
5. To create a vm, use <pre>go run client.go -ip=ipaddress -port=portno -vm=vmname -cpu=noofcpu -mem=memory</pre>  
On exceuting above request, the create vm request will be sent to `master` with ip `ipaddress`, listening on `portno`. This will send a create vm request with vm name `vmname` and noofcpu's `noofcpu` and memory `memory`.  
Example: <pre>go run client.go -ip localhost -port 8079 -vm=986962601 -cpu=6874 -mem=3109</pre>
The above command will send a vm create request to master hosted on `localhost` listening on port `8079`. The create vm request parameters creates a vm with name `986962601` with cpu `6874` and memory `3109`. Here both memory and cpu is normalized on a scale.

# Integration with Xen Server
The controller can be easily integrated with any cloud provider who implements the `Server` interface for `napolet/napolet.proto`.  
One should implement methods for `CreateVM`, `DeleteVM`, `GetStat`, `MigrateVM` in XAPI.  
The `vmid` and `pmid` are given by `master`, which are different from `vmid's` and `pmid's` of `Xen Server`. So, they should map the `vmid` of master with `vmid` of Xen Server.  
All the creation, deletion and migration of vm's should be taken care by implementor of this interface.  
For `GetStat`, napolet should send the cpu and memory usage for every `vm` for the coming `t` minutes residing on that physical machine. One can use wiener filter for predicting cpu and memory resources.

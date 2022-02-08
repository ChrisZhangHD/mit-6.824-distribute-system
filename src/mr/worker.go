package mr

import (
	"strings"
	"encoding/json"
	"os"
	"io/ioutil"
	"fmt"
	"log"
	"net/rpc"
	"hash/fnv"
)


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		var args Args
		var reply Reply

		ok := call("Coordinator.GetTask", &args, &reply)

		if !ok {
			os.Exit(1)
		}
		if reply.Phase == MapPhase {
			execMapTask(mapf, reply)
			args.Index = reply.Index
			args.Phase = reply.Phase
			call("Coordinator.CommitTask", &args, &reply)
		}
		
		if (reply.Phase == ReducePhase) {
			execReduceTask(reducef, reply)
			args.Index = reply.Index
			args.Phase = reply.Phase
			call("Coordinator.CommitTask", &args, &reply)
		}
	}

}

func execMapTask(mapf func(string, string) []KeyValue, reply Reply) {
	fileName := reply.FileNames[0]
	nReduce := reply.NReduce
	content, err := ioutil.ReadFile(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
		return
	}
	kvs := mapf(fileName, string(content))
	reduces := make([][]KeyValue, nReduce)
	for _, kv := range kvs {
		idx := ihash(kv.Key) % nReduce
		reduces[idx] = append(reduces[idx], kv)
	}
	mapTaskIndex := reply.Index
	for idx, list := range reduces {
		newFileName := reduceName(mapTaskIndex, idx)
		f, err := os.Create(newFileName)
		if err != nil {
			log.Fatalf("cannot create %v", fileName)
			return
		}
		enc := json.NewEncoder(f)
		for _, kv := range list {
			if err := enc.Encode(&kv); err != nil {
				return
			}
		}
		f.Close()
	}
	
}

func execReduceTask(reducef func(string, []string) string, reply Reply) {
	maps := make(map[string][]string)
	for i := 0; i < len(reply.FileNames); i ++ {
		fileName := reply.FileNames[i]
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
			return
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
		file.Close()
	}
	res := make([]string, 0, 100)
	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v %v\n", k, reducef(k, v)))
	}
	if err := ioutil.WriteFile(mergeName(reply.Index), []byte(strings.Join(res, "")), 0600); err != nil {
		log.Fatalf("cannot write result")
		return
	}

}

func mergeName(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}



//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

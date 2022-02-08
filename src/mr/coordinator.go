package mr

import (
	"time"
	"fmt"
	"sync"
	"log"
	"net"
	"os"
	"net/rpc"
	"net/http"
)


type Coordinator struct {
	// Your definitions here.
	Mutex        sync.Mutex
	NReduce      int
	Files        []string
	MapDone      []bool
	ReduceDone   []bool
	WorkerMap    map[int]int64
	WorkerReduce map[int]int64
	done         bool

}

const MAX_WORKER_TIME int64 = 10

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *Args, reply *Reply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	for i := 0; i < len(c.Files); i ++ {
		if !c.MapDone[i] {
			mapTime, ok := c.WorkerMap[i]
			if !ok {
				reply.FileNames = make([]string, 1)
				reply.FileNames[0] = c.Files[i]
				reply.Index = i
				reply.NReduce = c.NReduce
				reply.Phase = MapPhase
				c.WorkerMap[i] = time.Now().Unix()
				return nil
			}else {
				timeNow := time.Now().Unix()
				if timeNow - mapTime > MAX_WORKER_TIME {
					reply.FileNames = make([]string, 1)
					reply.FileNames[0] = c.Files[i]
					reply.Index = i
					reply.NReduce = c.NReduce
					reply.Phase = MapPhase
					c.WorkerMap[i] = time.Now().Unix()
					return nil
				}
			}
		}
	}
	for i := 0; i < len(c.Files); i ++ {
		if !c.MapDone[i] {
			return nil
		}
	}
	for i := 0; i < c.NReduce; i ++ {
		if !c.ReduceDone[i] {
			reduceTime, ok := c.WorkerReduce[i]
			if !ok {
				for j := 0; j < len(c.Files); j ++ {
					fileName := reduceName(j, i)
					reply.FileNames = append(reply.FileNames, fileName)
				}
				reply.Index = i
				reply.NReduce = c.NReduce
				reply.Phase = ReducePhase
				c.WorkerReduce[i] = time.Now().Unix()
				return nil
			}else {
				timeNow := time.Now().Unix()
				if timeNow - reduceTime > MAX_WORKER_TIME {
					for j := 0; j < len(c.Files); j ++ {
						fileName := reduceName(j, i)
						reply.FileNames = append(reply.FileNames, fileName)
					}
					reply.Index = i
					reply.NReduce = c.NReduce
					reply.Phase = ReducePhase
					c.WorkerReduce[i] = time.Now().Unix()
					return nil
				}
			}

		}
	}
	for i := 0; i < c.NReduce; i ++ {
		if !c.ReduceDone[i] {
			return nil
		}
	}
	c.done = true
	return fmt.Errorf("all done")
}

func (c *Coordinator) CommitTask(args *Args, reply *Reply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	if args.Phase == MapPhase {
		if time.Now().Unix() - c.WorkerMap[args.Index] <= 10 {
			c.MapDone[args.Index] = true
		}
	}
	if args.Phase == ReducePhase {
		if time.Now().Unix() - c.WorkerReduce[args.Index] <= 10 {
			c.ReduceDone[args.Index] = true
		}
	}
	return nil
}

func reduceName(mapIdx, reduceIdx int) string {
	return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// ret := false

	// Your code here.
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	return c.done
	// return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.Mutex = sync.Mutex{}
	c.Files = files
	c.NReduce = nReduce
	c.MapDone = make([]bool, len(files))
	c.ReduceDone = make([]bool, nReduce)
	c.WorkerMap = make(map[int]int64)
	c.WorkerReduce = make(map[int]int64)
	c.done = false
	c.server()
	return &c
}

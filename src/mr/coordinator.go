package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// task state
const (
	idle = iota
	inProgress
	completed
)

type Coordinator struct {
	//debugMutex           sync.Mutex
	nReduce              int
	mapFilename          []string
	mapTaskState         []int
	mapTaskStateMutex    sync.Mutex
	mapTaskComplete      bool
	reduceFilename       [][]string
	reduceTaskState      []int
	reduceTaskStateMutex sync.Mutex
	reduceTaskComplete   bool
}

func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	//c.debugMutex.Lock()
	//defer c.debugMutex.Unlock()

	// if timeout, then assign same task to new worker
	timeoutDetect := func(state *int, mutex *sync.Mutex) {
		time.Sleep(10 * time.Second)
		mutex.Lock()
		defer mutex.Unlock()
		if *state == inProgress {
			*state = idle
		}
	}

	// check map complete or not, assign map task
	c.mapTaskStateMutex.Lock()
	defer c.mapTaskStateMutex.Unlock()
	for !c.mapTaskComplete {
		for i, v := range c.mapTaskState {
			if v != idle {
				continue
			}
			reply.NReduce = c.nReduce
			reply.TaskType = MapTask
			reply.Index = i
			reply.Filename = []string{c.mapFilename[i]}
			c.mapTaskState[i] = inProgress
			go timeoutDetect(&c.mapTaskState[i], &c.mapTaskStateMutex)
			return nil
		}

		// wait for all map tasks to complete
		c.mapTaskStateMutex.Unlock()
		time.Sleep(time.Second)
		c.mapTaskStateMutex.Lock()
	}

	//log.Println("mapTaskComplete: ", c.mapTaskComplete)
	//log.Println("mapTaskState: ", c.mapTaskState)

	// check reduce complete or not, assign reduce task
	c.reduceTaskStateMutex.Lock()
	defer c.reduceTaskStateMutex.Unlock()
	for !c.reduceTaskComplete {
		for i, v := range c.reduceTaskState {
			if v != idle {
				continue
			}
			reply.TaskType = ReduceTask
			reply.Index = i
			reply.Filename = c.reduceFilename[i]
			c.reduceTaskState[i] = inProgress
			go timeoutDetect(&c.reduceTaskState[i], &c.reduceTaskStateMutex)
			return nil
		}

		// wait for all reduce tasks to complete
		c.reduceTaskStateMutex.Unlock()
		time.Sleep(time.Second)
		c.reduceTaskStateMutex.Lock()
	}

	// reply all tasks complete
	reply.TaskType = CompleteTask
	return nil
}

func (c *Coordinator) MapComplete(args *MapCompleteArgs, reply *MapCompleteReply) error {
	//c.debugMutex.Lock()
	//defer c.debugMutex.Unlock()

	c.mapTaskStateMutex.Lock()
	defer c.mapTaskStateMutex.Unlock()
	c.mapTaskState[args.Index] = completed

	c.reduceTaskStateMutex.Lock()
	defer c.reduceTaskStateMutex.Unlock()
	for i := range args.Filename {
		c.reduceFilename[i][args.Index] = args.Filename[i]
	}

	// is all map tasks complete or not
	if !c.mapTaskComplete {
		ok := true
		for _, v := range c.mapTaskState {
			if v != completed {
				ok = false
				break
			}
		}
		if ok {
			c.mapTaskComplete = true
		}
	}
	return nil
}

func (c *Coordinator) ReduceComplete(args *ReduceCompleteArgs, reply *ReduceCompleteReply) error {
	//c.debugMutex.Lock()
	//defer c.debugMutex.Unlock()

	c.reduceTaskStateMutex.Lock()
	defer c.reduceTaskStateMutex.Unlock()
	c.reduceTaskState[args.Index] = completed

	// is all reduce tasks complete or not
	if !c.reduceTaskComplete {
		ok := true
		for _, v := range c.reduceTaskState {
			if v != completed {
				ok = false
				break
			}
		}
		if ok {
			c.reduceTaskComplete = true
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	//c.debugMutex.Lock()
	//defer c.debugMutex.Unlock()

	c.reduceTaskStateMutex.Lock()
	defer c.reduceTaskStateMutex.Unlock()
	return c.reduceTaskComplete
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.nReduce = nReduce
	c.mapFilename = files
	c.mapTaskState = make([]int, len(files))
	c.reduceTaskState = make([]int, nReduce)
	c.reduceFilename = make([][]string, nReduce)
	for i := range c.reduceFilename {
		c.reduceFilename[i] = make([]string, len(files))
	}

	c.server()
	return &c
}


package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	mapTask := func(r *AssignTaskReply, mapf func(string, string) []KeyValue) {
		// read file, then use map function
		kva := []KeyValue{}
		for _, filename := range r.Filename {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva = append(kva, mapf(filename, string(content))...)
		}

		// hash the map result nReduce buckets
		buckets := make([][]KeyValue, r.NReduce)
		for i := range kva {
			p := &buckets[ihash(kva[i].Key)%r.NReduce]
			*p = append(*p, kva[i])
		}

		// write result to nReduce files, e.g. mr-x-y, use atomic renaming
		oname := make([]string, r.NReduce)
		for i := range buckets {
			oname[i] = "mr-" + strconv.Itoa(r.Index) + "-" + strconv.Itoa(i)
			ofile, _ := os.CreateTemp("/tmp", oname[i])
			enc := json.NewEncoder(ofile)
			for j := range buckets[i] {
				err := enc.Encode(&buckets[i][j])
				if err != nil {
					log.Fatalf("cannot write %v", oname[i])
				}
			}
			os.Rename(ofile.Name(), "./"+oname[i])
			ofile.Close()
		}

		// send filenames to coordinator
		args, reply := MapCompleteArgs{}, MapCompleteReply{}
		args.Index = r.Index
		args.Filename = oname
		call("Coordinator.MapComplete", &args, &reply)
	}

	reduceTask := func(r *AssignTaskReply, reducef func(string, []string) string) {
		// read file
		kva := []KeyValue{}
		for _, filename := range r.Filename {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
			file.Close()
		}

		// sort k/v pairs, handle kva and use reduce function
		// then write result, e.g. mr-out-x, use atomic renaming
		sort.Sort(ByKey(kva))
		oname := "mr-out-" + strconv.Itoa(r.Index)
		ofile, _ := os.CreateTemp("/tmp", oname)
		for i := 0; i < len(kva); {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			output := reducef(kva[i].Key, values)
			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
			i = j
		}
		os.Rename(ofile.Name(), "./"+oname)
		ofile.Close()

		// send filenames to coordinator
		args, reply := ReduceCompleteArgs{}, ReduceCompleteReply{}
		args.Index = r.Index
		call("Coordinator.ReduceComplete", &args, &reply)
	}

	for {
		args, reply := AssignTaskArgs{}, AssignTaskReply{}
		ok := call("Coordinator.AssignTask", &args, &reply)
		if ok == false {
			break
		}
		if reply.TaskType == MapTask {
			//log.Printf("map: %v\n", reply)
			mapTask(&reply, mapf)
		} else if reply.TaskType == ReduceTask {
			//log.Printf("reduce: %v\n", reply)
			reduceTask(&reply, reducef)
		} else {
			break
		}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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


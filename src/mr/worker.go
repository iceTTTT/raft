package mr

import (
	"fmt"
	"log"
	"net/rpc"
	"hash/fnv"
	"time"
	"os"
	"io/ioutil"
	"encoding/json"
	"strings"
	"sort"
)


//
// Map functions return a slice of KeyValue.
//
// Worker type
const (
	reduce_ = uint32(0)
	map_ = uint32(1)
)
type KeyValue struct {
	Key   string
	Value string
}

type Dummy struct {}	

type Cansortkv []KeyValue

func (c Cansortkv) Len() int { return len(c) }

func (c Cansortkv) Swap(i, j int) { c[i], c[j] = c[j], c[i] }

func (c Cansortkv) Less(i, j int) bool { return c[i].Key < c[j].Key }

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

	for  {
		gettask := Task{}
		dum := Dummy{}
		ok := call("Coordinator.GetTask", dum, &gettask)
		for ok && !gettask.Isget {
			time.Sleep(100 * time.Millisecond)
			ok = call("Coordinator.GetTask", dum, &gettask)
		}
		if !ok {
			return
		}
		switch gettask.Tasktype {
		case map_ :
			filename := gettask.Taskfile[0]
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("Can't open file %v", filename)
			}
			content, err_ := ioutil.ReadAll(file)
			if err_ != nil {
				log.Fatalf("Can't read file %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			location := Location{}
			fileslice := make([]*os.File, gettask.Nofr)
			encslice := make([]*json.Encoder, gettask.Nofr)
			for i := uint32(0); i < gettask.Nofr; i++ {
				var b strings.Builder
				fmt.Fprintf(&b, "mr-%v-%v", gettask.Tasknum, i)
				fileslice[i], err = os.Create(b.String())
				if err != nil {
					log.Fatalf("Can't create file %v", b.String())
				}
				encslice[i] = json.NewEncoder(fileslice[i])
				location.Filename = append(location.Filename, b.String())
				location.Mpair = append(location.Mpair, Pair{gettask.Tasknum, i})
			}
			// Save the intermidiate kv-pair in json file
			for _, kv := range kva {
				encslice[ihash(kv.Key)%int(gettask.Nofr)].Encode(&kv)
			}
			// Close all file
			for i := uint32(0); i < gettask.Nofr; i++ {
				fileslice[i].Close()
			}
			// Notify the coordinator this map part complete, and the location of intermidiate content.
			ok := call("Coordinator.PutLocation", location, &dum)
			if !ok {
				return
			}
		case reduce_ :
			// Read and sort all the intermidiate k-v pair.
			var interkv []KeyValue
			for _, filename := range gettask.Taskfile {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("Can't open file %v reduce", filename)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					interkv = append(interkv, kv)
				}
				file.Close()
			}
			sort.Sort(Cansortkv(interkv))
			// Create temp file, rename after output all k-v.
			var s strings.Builder
			fmt.Fprintf(&s, "%v", gettask.Tasknum)
			tempfile, err := ioutil.TempFile("", s.String())
			if err != nil {
				log.Fatal("Can't create Tempfile")
			}
			var b strings.Builder
			fmt.Fprintf(&b, "mr-out-%v", gettask.Tasknum)
			for i := 0; i < len(interkv); {
				j := i+1
				for j != len(interkv) && interkv[j].Key == interkv[i].Key {
					j++
				}
				values := []string{}
				for ; i != j; i++ {
					values = append(values, interkv[i].Value)
				}
				routput := reducef(interkv[i-1].Key, values)
				// Output the generated k-v pair.
				fmt.Fprintf(tempfile,"%v %v\n", interkv[i-1].Key, routput)
			}
			os.Rename(tempfile.Name(), b.String())
			tempfile.Close()
			// Notify the coordinator.
			rn := ReduceNum(gettask.Tasknum)
			ok := call("Coordinator.ReduceDone", rn, &dum)
			if !ok {
				return
			}
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
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
		// log.Fatal("dialing:", err)
		os.Exit(1)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	// fmt.Println(err)
	return false
}

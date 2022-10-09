package mr

import (
	"log"
	"net"
	"os"
	"net/rpc"
	"net/http"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex
	numreduce uint32
	// Record for map task
	mapdone map[uint32]bool
	mapassigned map[uint32]bool
	// Record for Reduce task
	reducedone map[uint32]bool
	reduceassigned map[uint32]bool
	// File name for intermediate k-v pair
	interkvfile map[Pair]string
	files []string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) PutLocation(args Location, reply *Dummy) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mapdone[args.Mpair[0].M] = true
	for index, pair := range args.Mpair {
		c.interkvfile[pair] = args.Filename[index]
	}
	return nil
}

func (c *Coordinator) ReduceDone(args ReduceNum, reply *Dummy) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reducedone[uint32(args)] = true
	return nil
}


// Get Task and wait for that worker to end.
func (c *Coordinator) WaitMap(worknum uint32, y *uint32) error {
	count := 0
	c.mu.Lock()
	for !c.mapdone[worknum] {
		c.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
		c.mu.Lock()
		count++
		if count == 101 {
			if !c.mapdone[worknum] {
				c.mapassigned[worknum] = false
			}
			c.mu.Unlock()
			return nil
		}
	}
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) WaitReduce(worknum uint32, y *uint32) error  {
	count := 0
	c.mu.Lock()
	for !c.reducedone[worknum] {
		c.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
		c.mu.Lock()
		count++
		if count == 101 {
			if !c.reducedone[worknum] {
				c.reduceassigned[worknum] = false
			}
			c.mu.Unlock()
			return nil
		}
	}
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) GetTask(args Dummy,reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := uint32(0); i != uint32(len(c.files)); i++ {
		if !c.mapassigned[i] {
			reply.Tasktype = map_
			reply.Tasknum = i
			reply.Taskfile = append(reply.Taskfile, c.files[i])
			reply.Isget = true
			reply.Nofr = c.numreduce
			c.mapassigned[i] = true
			go c.WaitMap(i, &i)
			return nil
		}
	}
	for i := uint32(0); i != uint32(len(c.files)); i++ {
		if !c.mapdone[i] {
			reply.Isget = false
			return nil
		}
	}
	for i := uint32(0); i != c.numreduce; i++ {
		if !c.reduceassigned[i] {
			reply.Tasktype = reduce_
			reply.Tasknum = i
			for j := uint32(0); j < uint32(len(c.files)); j++ {
				reply.Taskfile = append(reply.Taskfile, c.interkvfile[Pair{j, i}])
			}
			reply.Isget = true
			reply.Nofr = c.numreduce
			c.reduceassigned[i] = true
			go c.WaitReduce(i, &i)
			return nil
		}
	}
	reply.Isget = false
	return nil
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
func (c *Coordinator) Done(x, y *int) error {

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := uint32(0); i != c.numreduce; i++ {
		if !c.reducedone[i] {
			return nil
		}
	}
	return Myint(0)
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.numreduce = uint32(nReduce)
	c.mapdone = make(map[uint32]bool)
	c.mapassigned = make(map[uint32]bool)
	c.reducedone = make(map[uint32]bool)
	c.reduceassigned = make(map[uint32]bool)
	c.interkvfile = make(map[Pair]string)
	c.files = files
	// Your code here.
	c.server()
	return &c
}

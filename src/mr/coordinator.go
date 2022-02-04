package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type taskStatus int

const (
	NOT_STARTED taskStatus = iota
	ASSIGNED
	COMPLETED
)

type taskInfo struct {
	taskName     string
	taskType     string
	taskSeq      int
	status       taskStatus
	reduceFiles  []string
	assignedTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	mu             sync.Mutex
	tasks          []*taskInfo
	taskInfoByName map[string]*taskInfo
	stage          string
	nMap           int
	nReduce        int
	// mapTaskSeq     int
}

// Your code here -- RPC handlers for the worker to call.
// func (c *Coordinator) AddTasks(tasks []string, taskType string) {
// 	c.tasks = append(c.tasks, tasks)
// 	for _, i := range c.tasks {
// 		c.taskStatus[i] = NOT_STARTED
// 	}
// }

func (c *Coordinator) AddTask(taskName string, taskType string, taskSeq int) *taskInfo {
	c.mu.Lock()
	defer c.mu.Unlock()

	task := taskInfo{taskName: taskName, taskType: taskType, status: NOT_STARTED, taskSeq: taskSeq}

	c.tasks = append(c.tasks, &task)
	c.taskInfoByName[taskName] = &task
	return &task
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	reply.TaskName = ""
	reply.TaskType = ""
	reply.NumReduce = c.nReduce
	if c.stage == "COMPLETED" {
		reply.TaskType = "COMPLETE"
		reply.TaskName = "COMPLETE"
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	for _, info := range c.tasks {

		if info.status == NOT_STARTED {
			reply.TaskName = info.taskName
			reply.TaskType = info.taskType
			info.status = ASSIGNED
			reply.TaskSeqNum = info.taskSeq
			if c.stage == "REDUCE" {
				reply.ReduceFiles = info.reduceFiles
			}
			info.assignedTime = time.Now()
			return nil
		}
		if info.status == ASSIGNED {
			timeDiff := time.Since(info.assignedTime)
			if timeDiff > time.Second*10 {
				reply.TaskName = info.taskName
				reply.TaskType = info.taskType
				reply.TaskSeqNum = info.taskSeq
				if c.stage == "REDUCE" {
					reply.ReduceFiles = info.reduceFiles
				}
				// info.status = ASSIGNED
				info.assignedTime = time.Now()
				return nil
			}
		}

	}
	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteArgs, reply *CompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.taskInfoByName[args.TaskName].status = COMPLETED
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

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
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stage == "COMPLETED" {
		fmt.Println("Coordinator COMPLETED")
		// wait for workers to exit
		time.Sleep(time.Second * 3)
		return true
	}
	return false
}

func (c *Coordinator) watchStage() {
	completed := false
	for !completed {
		completedCnt := 0
		for _, info := range c.tasks {
			if info.status == COMPLETED {
				completedCnt++
			} else {
				break
			}
		}
		if completedCnt == len(c.tasks) {
			c.proceedStage()
		} else {
			time.Sleep(time.Second * 2)
		}

		if c.stage == "COMPLETED" {
			completed = true
		}
	}
}

func (c *Coordinator) proceedStage() {
	c.resetTasks()
	switch c.stage {
	case "MAP":
		{
			c.stage = "REDUCE"
			c.BuildReduceTasks()
			break
		}
	case "REDUCE":
		{
			c.stage = "COMPLETED"
			break
		}
	}
}

func (c *Coordinator) BuildReduceTasks() {
	// c.mu.Lock()
	// defer c.mu.Un
	for nR := 0; nR < c.nReduce; nR++ {
		taskName := fmt.Sprintf("reduce-%v", nR+1)
		task := c.AddTask(taskName, "REDUCE", nR+1)
		for nM := 0; nM < c.nMap; nM++ {
			intermediateFileName := fmt.Sprintf("mr-%v-%v", nM+1, nR+1)
			task.reduceFiles = append(task.reduceFiles, intermediateFileName)
		}
	}

}

func (c *Coordinator) resetTasks() {
	c.tasks = nil
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range c.taskInfoByName {
		delete(c.taskInfoByName, k)
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{nReduce: nReduce, stage: "MAP", nMap: len(files), taskInfoByName: make(map[string]*taskInfo)}

	// Your code here.
	for i := 0; i < len(files); i++ {
		c.AddTask(files[i], "MAP", i+1)
	}
	c.server()
	go c.watchStage()
	return &c
}

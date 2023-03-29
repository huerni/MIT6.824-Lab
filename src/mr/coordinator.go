package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const MaxTaskRunTime time.Duration = 1e10

type TaskState struct {
	Status    string
	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	Files      []string
	TaskChan   chan Task
	NumMap     int
	NumReduce  int
	Mutex      sync.Mutex
	TaskStates []TaskState
	TaskPhase  string
	AllDone    bool
}

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

func (c *Coordinator) HandleTaskRequest(args *RequestTaskArgs, reply *RequestTaskReply) error {
	if !args.WorkerStatus {
		return errors.New("当前worker已下线\n")
	}
	task, err := <-c.TaskChan
	if err {
		reply.Task = task
		c.Mutex.Lock()
		defer c.Mutex.Unlock()
		// fmt.Printf("send %v task %v\n", c.TaskPhase, task.TaskIndex)
		c.TaskStates[reply.Task.TaskIndex].Status = TaskStatusRunning
		c.TaskStates[reply.Task.TaskIndex].StartTime = time.Now()
	} else if c.AllDone {
		reply.AllDone = true
	} else {
		reply.Wait = true
	}

	return nil
}

func (c *Coordinator) HandleTaskReport(args *ReportTaskArgs, reply *ReportTaskReply) error {
	if !args.WorkerStatus {
		reply.Ack = false
		return errors.New("当前worker已下线\n")
	}
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if c.TaskStates[args.TaskIndex].Status == TaskStatusRunning {
		c.TaskStates[args.TaskIndex].Status = TaskStatusFinish
	}
	reply.Ack = true
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

func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	isfinish := true
	for key, val := range c.TaskStates {
		switch val.Status {
		case TaskStatusReady:
			isfinish = false
			// 添加进队列
			c.AddTask(key)
		case TaskStatusQueue:
			isfinish = false
		case TaskStatusRunning:
			isfinish = false
			// 判断是否超时
			c.CheckTimeout(key)
		case TaskStatusFinish:
		case TaskStatusErr:
			isfinish = false
			// 处理出错，重新加入队列处理
			c.AddTask(key)
		default:
			log.Fatal("状态错误")
		}
	}

	if isfinish == true {
		if c.TaskPhase == MapPhase {
			c.TaskPhase = ReducePhase
			// 将所有任务初始化
			// fmt.Printf("初始化reduce任务\n")
			c.TaskStates = make([]TaskState, c.NumReduce)
			for i := 0; i < c.NumReduce; i++ {
				c.TaskStates[i] = TaskState{TaskStatusReady, time.Now()}
			}
		} else {
			ret = true
			c.AllDone = true
			close(c.TaskChan)
		}
	}

	return ret
}

func (c *Coordinator) AddTask(key int) {
	task := Task{
		NumMap:    len(c.Files),
		NumReduce: c.NumReduce,
		TaskPhase: c.TaskPhase,
		TaskIndex: key,
		FileName:  "",
		IsDone:    false,
	}
	if c.TaskPhase == MapPhase {
		task.FileName = c.Files[key]
	}
	c.TaskStates[key].Status = TaskStatusQueue
	c.TaskChan <- task
}

func (c *Coordinator) CheckTimeout(key int) {
	timeDuration := time.Now().Sub(c.TaskStates[key].StartTime)
	if timeDuration > MaxTaskRunTime {
		// fmt.Printf("time out: %v %v\n", c.TaskPhase, key)
		c.AddTask(key)
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.NumMap = len(files)
	c.NumReduce = nReduce
	c.Files = files
	c.TaskPhase = MapPhase
	c.TaskStates = make([]TaskState, c.NumMap)
	c.TaskChan = make(chan Task, 10)
	c.AllDone = false

	for i := 0; i < c.NumMap; i++ {
		c.TaskStates[i] = TaskState{TaskStatusReady, time.Now()}
	}

	c.server()
	return &c
}

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

type TaskStatus int

const (
	MapTaskTimeOut    = 20 * time.Second
	ReduceTaskTimeOut = MapTaskTimeOut
)

const (
	Ready TaskStatus = iota
	Running
	Completed
)

type MapTask struct {
	FileName string
	Status   TaskStatus
}

type ReduceTask struct {
	FileNames []string
	Status    TaskStatus
}

type Coordinator struct {
	mapTasks        []MapTask
	reduceTasks     []ReduceTask
	nMap            int
	nReduce         int
	nFinishedMap    int
	nFinishedReduce int
	mtx             sync.Mutex
	mapCond         *sync.Cond
	reduceCond      *sync.Cond
}

func NewCoordinator(fileNames []string, nReduce int) *Coordinator {
	c := &Coordinator{
		mapTasks:        make([]MapTask, len(fileNames)),
		reduceTasks:     make([]ReduceTask, nReduce),
		nMap:            len(fileNames),
		nReduce:         nReduce,
		nFinishedMap:    0,
		nFinishedReduce: 0,
		mtx:             sync.Mutex{},
	}
	c.mapCond = sync.NewCond(&c.mtx)
	c.reduceCond = sync.NewCond(&c.mtx)

	for i, fileName := range fileNames {
		c.mapTasks[i] = MapTask{
			FileName: fileName,
			Status:   Ready,
		}
	}

	for i := 0; i < nReduce; i++ {
		reduceFileNames := make([]string, c.nMap)
		for j := 0; j < c.nMap; j++ {
			reduceFileNames[j] = GetMapResFileName(j, i)
		}
		c.reduceTasks[i] = ReduceTask{
			FileNames: reduceFileNames,
			Status:    Ready,
		}
	}
	return c
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(request *GetTaskRequest, response *GetTaskResponse) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	for c.nFinishedMap < c.nMap { // Map phase
		for i := 0; i < c.nMap; i++ {
			if c.mapTasks[i].Status == Ready {
				response.TaskType = Map
				response.TaskID = i
				response.MapFileName = c.mapTasks[i].FileName
				response.NReduce = c.nReduce
				c.mapTasks[i].Status = Running
				go func() {
					time.Sleep(MapTaskTimeOut)
					c.mtx.Lock()
					defer c.mtx.Unlock()
					if c.mapTasks[i].Status == Running {
						c.mapTasks[i].Status = Ready
						c.mapCond.Signal()
					}
				}()
				return nil
			}
		}
		c.mapCond.Wait()
	}

	for c.nFinishedReduce < c.nReduce { // Reduce phase
		for i := 0; i < c.nReduce; i++ {
			if c.reduceTasks[i].Status == Ready {
				response.TaskType = Reduce
				response.TaskID = i
				response.ReduceFileNames = c.reduceTasks[i].FileNames
				response.NReduce = c.nReduce
				c.reduceTasks[i].Status = Running
				go func() {
					time.Sleep(ReduceTaskTimeOut)
					c.mtx.Lock()
					defer c.mtx.Unlock()
					if c.reduceTasks[i].Status == Running {
						c.reduceTasks[i].Status = Ready
						c.reduceCond.Signal()
					}
				}()
				return nil
			}
		}
		c.reduceCond.Wait()
	}

	response.TaskType = NoTask
	return nil
}

func (c *Coordinator) CommitTask(request *CommitTaskRequest, response *CommitTaskResponse) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	switch request.TaskType {
	case Map:
		if request.TaskID < 0 || request.TaskID >= c.nMap {
			// return fmt.Errorf("invalid map task id: %v", request.TaskID)
			return nil
		}

		if c.mapTasks[request.TaskID].Status != Running {
			// return fmt.Errorf("map task %v is not running", request.TaskID)
			return nil
		}

		c.mapTasks[request.TaskID].Status = Completed
		c.nFinishedMap++
		if c.nFinishedMap == c.nMap {
			c.mapCond.Broadcast()
		}
	case Reduce:
		if request.TaskID < 0 || request.TaskID >= c.nReduce {
			// return fmt.Errorf("invalid reduce task id: %v", request.TaskID)
			return nil
		}

		if c.reduceTasks[request.TaskID].Status != Running {
			// return fmt.Errorf("reduce task %v is not running", request.TaskID)
			return nil
		}

		c.reduceTasks[request.TaskID].Status = Completed
		c.nFinishedReduce++
		if c.nFinishedReduce == c.nReduce {
			c.reduceCond.Broadcast()
		}
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	c.mtx.Lock()
	defer c.mtx.Unlock()
	return c.nFinishedMap == c.nMap && c.nFinishedReduce == c.nReduce
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := NewCoordinator(files, nReduce)
	c.server()
	return c
}

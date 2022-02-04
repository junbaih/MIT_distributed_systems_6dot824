package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	for {
		taskName, taskType, taskSeq, nReduce, reduceFiles := GetTask()
		switch taskType {
		case "":
			{
				break
			}
		case "COMPLETE":
			{
				// fmt.Println("RECEIVED COMPLETED")
				return
			}
		case "MAP":
			{
				// fmt.Printf("MAP TASK RECEIVED %v %v %v\n", os.Getpid(), taskName, taskSeq)

				RunMapTask(mapf, taskName, nReduce, taskSeq)
				CompleteTask(taskName)
				// fmt.Printf("MAP TASK COMPLETED %v %v %v\n", os.Getpid(), taskName, taskSeq)

				break
			}
		case "REDUCE":
			{
				// fmt.Printf("REDUCE TASK RECEIVED %v %v %v\n", os.Getpid(), taskName, taskSeq)
				RunReduceTask(reducef, reduceFiles, taskSeq)
				CompleteTask(taskName)
				// fmt.Printf("REDUCE TASK COMPLETED %v %v %v\n", os.Getpid(), taskName, taskSeq)
				break
			}
		}
		time.Sleep(time.Second)

	}
	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func RunMapTask(mapf func(string, string) []KeyValue, filename string, nReduce int, taskSeq int) {
	intermediateByBucket := make([][]KeyValue, nReduce)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	for _, kv := range kva {
		k := kv.Key
		bucketNum := ihash(k) % nReduce
		intermediateByBucket[bucketNum] = append(intermediateByBucket[bucketNum], kv)
	}
	// intermediate = append(intermediate, kva...)

	for bucketNum, kva := range intermediateByBucket {
		file, err := ioutil.TempFile(".", "temp*")
		if err != nil {
			log.Fatal("cannot create temp file")
		}
		encoder := json.NewEncoder(file)
		err = encoder.Encode(kva)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		intermediateFileName := fmt.Sprintf("./mr-%v-%v", taskSeq, bucketNum+1)
		os.Rename(file.Name(), intermediateFileName)
	}
	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//

	// sort.Sort(intermediate)
}

func RunReduceTask(reducef func(string, []string) string, reduceFiles []string, taskSeq int) {
	kva := []KeyValue{}
	for _, filename := range reduceFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		decoder := json.NewDecoder(file)
		var intermediateKV []KeyValue
		err = decoder.Decode(&intermediateKV)
		// content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva = append(kva, intermediateKV...)
	}
	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("./mr-out-%v", taskSeq)
	ofile, _ := os.Create(oname)
	// ofile, _ := ioutil.TempFile("", "temp*")

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kva) {
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
		bytes, err := fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		// fmt.Printf("reduce-%v RESULT %v %v, write %v\n", taskSeq, kva[i].Key, output, bytes)

		if err != nil {
			log.Fatal(bytes, err)
		}
		i = j
	}

	ofile.Close()
	os.Rename(ofile.Name(), oname)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func GetTask() (string, string, int, int, []string) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	succ := call("Coordinator.GetTask", &args, &reply)
	if !succ {
		panic("Worker GetTask failed")
	}
	return reply.TaskName, reply.TaskType, reply.TaskSeqNum, reply.NumReduce, reply.ReduceFiles
}

func CompleteTask(taskName string) {
	args := CompleteArgs{TaskName: taskName}
	reply := CompleteReply{}
	succ := call("Coordinator.CompleteTask", &args, &reply)
	if !succ {
		panic("Worker GetTask failed")
	}
}

// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	call("Coordinator.Example", &args, &reply)

// 	// reply.Y should be 100.
// 	fmt.Printf("reply.Y %v\n", reply.Y)
// }

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

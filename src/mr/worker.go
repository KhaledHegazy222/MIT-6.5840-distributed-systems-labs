package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func init() {
	rand.New(rand.NewSource(time.Now().UnixNano()))
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

// main/mrworker.go calls this function.

func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	for {
		t, err := FetchTask()
		if err != nil {
			log.Fatalf("unexpected error fetching task: %v", err)
		}

		switch t.TaskType {
		case TypeExit:
			return

		case TypeWait:
			time.Sleep(100 * time.Millisecond) // backoff instead of busy-spin
			continue

		case TypeMap:
			if err := handleMapTask(t, mapf); err != nil {
				log.Fatalf("map task failed: %v", err)
			}

		case TypeReduce:
			if err := handleReduceTask(t, reducef); err != nil {
				log.Fatalf("reduce task failed: %v", err)
			}

		default:
			log.Fatalf("unknown task type: %v", t.TaskType)
		}

		// mark task complete
		MarkFinshed(t.Idx)
	}
}

func FetchTask() (Task, error) {
	// declare an argument structure.
	args := FetchTaskArgs{}

	// // fill in the argument(s).
	// args.X = 99

	// declare a reply structure.
	reply := FetchTaskReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.FetchTask", &args, &reply)
	if ok {
		return reply.Task, nil
	} else {
		return Task{}, fmt.Errorf("RPC Call Failed")
	}
}

func MarkFinshed(idx int) error {
	// declare an argument structure.
	args := MarkFinishedArgs{
		Idx: idx,
	}

	// // fill in the argument(s).
	// args.X = 99

	// declare a reply structure.
	reply := MarkFinishedReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.MarkFinished", &args, &reply)
	if ok {
		return nil
	} else {
		return fmt.Errorf("RPC Call Failed")
	}
}

func createTempWriters(outputs []string) ([]*os.File, string, error) {
	randomStr := RandStringRunes(10)
	files := make([]*os.File, len(outputs))

	for i, outputFile := range outputs {
		tempName := "temp-" + randomStr + outputFile
		f, err := os.OpenFile(tempName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return nil, "", err
		}
		files[i] = f
	}
	return files, randomStr, nil
}

func finalizeFiles(files []*os.File, outputs []string, prefix string) error {
	for i, f := range files {
		if err := f.Close(); err != nil {
			return err
		}
		tempName := "temp-" + prefix + outputs[i]
		if err := os.Rename(tempName, outputs[i]); err != nil {
			return err
		}
	}
	return nil
}

func handleMapTask(t Task, mapf func(string, string) []KeyValue) error {
	files, randStr, err := createTempWriters(t.Output)
	if err != nil {
		return err
	}
	defer finalizeFiles(files, t.Output, randStr)

	inputFile := t.Input[0]
	content, err := os.ReadFile(inputFile)
	if err != nil {
		return fmt.Errorf("reading input file %s: %w", inputFile, err)
	}

	kvs := mapf(inputFile, string(content))
	for _, kv := range kvs {
		bucket := ihash(kv.Key) % len(t.Output)
		if _, err := fmt.Fprintf(files[bucket], "%v %v\n", kv.Key, kv.Value); err != nil {
			return err
		}
	}
	return nil
}

func handleReduceTask(t Task, reducef func(string, []string) string) error {
	files, randStr, err := createTempWriters(t.Output)
	if err != nil {
		return err
	}
	defer finalizeFiles(files, t.Output, randStr)

	var kvs []KeyValue
	for _, inputFile := range t.Input {
		f, err := os.Open(inputFile)
		if err != nil {
			return fmt.Errorf("open reduce input %s: %w", inputFile, err)
		}

		for {
			var kv KeyValue
			_, err := fmt.Fscanf(f, "%v %v", &kv.Key, &kv.Value)
			if err == io.EOF {
				break
			}
			if err != nil {
				f.Close()
				return fmt.Errorf("reading %s: %w", inputFile, err)
			}
			kvs = append(kvs, kv)
		}
		f.Close()
	}

	sort.Sort(ByKey(kvs))
	for i := 0; i < len(kvs); {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := make([]string, j-i)
		for k := i; k < j; k++ {
			values[k-i] = kvs[k].Value
		}
		output := reducef(kvs[i].Key, values)

		if _, err := fmt.Fprintf(files[0], "%v %v\n", kvs[i].Key, output); err != nil {
			return err
		}
		i = j
	}

	return nil
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

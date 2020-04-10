package mapreduce

import "container/list"
import "fmt"
import "log"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	mapJobsNum := mr.nMap
	reduceJobsNum := mr.nReduce

	// create channels for map and reduce jobs separately
	mapChannel := make(chan string, mapJobsNum)
	reduceChannel := make(chan string, reduceJobsNum)

	// loop through map jobs first before reduce
	mapCurr := 0
	for mapCurr < mapJobsNum{
		log.Printf("Starting map jobs...")
		go func(mapNum int){
			assignWorkerJobs(mr, mapChannel, Map, mapNum, mapJobsNum, reduceJobsNum)
			// mapCurr += 1
		}(mapCurr)
		mapCurr += 1
	}
	// check for completion by pinging
	for j:=0; j < mapJobsNum; j++{
		// wait for ping - completed
		status := <-mapChannel
		log.Printf("Map job status: %s", status)
	}

	// after map is completed, do reduce jobs
	// methods are similar to map jobs
	// seperated to ensure that the reduce jobs are done AFTER map jobs
	reduceCurr := 0
	for reduceCurr < reduceJobsNum{
		log.Printf("Starting reduce jobs...")
		go func(reduceNum int){
			assignWorkerJobs(mr, reduceChannel, Reduce, reduceNum, reduceJobsNum, mapJobsNum)
			// reduceCurr += 1
		}(reduceCurr)
		reduceCurr += 1
	}
	for j:=0; j < reduceJobsNum; j++{
		// wait for ping - completed
		status := <-reduceChannel
		log.Printf("Reduce job status: %s", status)
	}
	
	return mr.KillWorkers()
}

func assignWorkerJobs(mr *MapReduce, jobChannel chan string, jobType JobType, currNum int, jobNum int, otherJobNum int){
	msg := false
	for msg != true{
		var reply DoJobReply
		// ERROR HANDLING: doing this will reassign workers if failed
		worker := <- mr.registerChannel

		// send RPC message
		log.Printf("Sending RPC message to worker...")
		msg = call(worker, "Worker.DoJob", DoJobArgs{mr.file, jobType, currNum, otherJobNum}, &reply)

		// if msg is true, then job is completed
		// send message back to the channel and update registerChannel
		if msg == true{
			jobChannel <- "Done"
			mr.registerChannel <- worker
			log.Printf("Assigned worker")
		}
	}
}


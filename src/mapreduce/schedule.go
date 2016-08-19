package mapreduce

import (
	"fmt"
	"log"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	args := make([]*DoTaskArgs, 0)
	for i := 0; i < ntasks; i++ {
		arg := &DoTaskArgs{
			JobName:       mr.jobName,
			File:          mr.files[i],
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: nios,
		}
		args = append(args, arg)
	}
	nrFinished := 0

	mr.Lock()
	initChan := make(chan string, len(mr.workers))
	for _, work := range mr.workers {
		initChan <- work
	}
	mr.Unlock()

	finishedChan := make(chan string)
	for _, arg := range args {
		// get a worker

		var wk string
		select {
		case wk = <-mr.registerChannel:
			mr.Lock()
			mr.workers = append(mr.workers, wk)
			mr.Unlock()
		case wk = <-finishedChan:
			nrFinished++
		case wk = <-initChan:
		}

		go func(wk string, arg *DoTaskArgs) {
			if call(wk, "Worker.DoTask", arg, new(struct{})) == false {
				log.Fatal("schedule: can't call")
			}
			finishedChan <- wk
		}(wk, arg)

	}
	// wait util all tasks are done
	for ; nrFinished < ntasks; nrFinished++ {
		<-finishedChan
	}
	fmt.Printf("Schedule: %v phase done\n", phase)
}

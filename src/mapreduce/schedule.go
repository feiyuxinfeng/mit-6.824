package mapreduce

import "fmt"

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
	debug("in schedule")
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

	finishedChan := make(chan string)
	// mr.Lock()
	// for i := 0; i < len(mr.workers); i++ {
	// 	finishedChan <- mr.workers[i]
	// }
	// mr.Unlock()
	for _, arg := range args {
		// get a worker

		var wk string
		select {
		case wk = <-mr.registerChannel:
		case wk = <-finishedChan:

		}
		// wk := <-finishedChan

		go func(wk string, arg *DoTaskArgs) {
			call(wk, "Worker.DoTask", arg, new(struct{}))
			finishedChan <- wk
		}(wk, arg)

	}
	fmt.Printf("Schedule: %v phase done\n", phase)
}

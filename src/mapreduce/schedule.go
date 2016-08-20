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
	taskChan := make(chan *DoTaskArgs, ntasks)
	finishedChan := make(chan bool, ntasks)
	doneChan := make(chan bool)

	for i := 0; i < ntasks; i++ {
		arg := &DoTaskArgs{
			JobName:       mr.jobName,
			File:          mr.files[i],
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: nios,
		}
		taskChan <- arg
	}

	mr.Lock()
	workerChan := make(chan string, len(mr.workers))

	for _, work := range mr.workers {
		workerChan <- work
	}
	mr.Unlock()

	// check the number of finished task, and close the doneChan to notify.
	go func(ntasks int) {
		nrFinished := 0
		for nrFinished < ntasks {
			select {
			case <-finishedChan:
				nrFinished++
			}
		}
		close(doneChan)
	}(ntasks)

	for {
		fmt.Print("hello")
		select {
		case <-doneChan:
			goto outfor
		default:
		}

		// get a task
		var arg *DoTaskArgs
		select {
		case arg = <-taskChan:
		// case <-time.After(0.1):
		default:
			arg = nil
		}

		if arg == nil {
			continue
		}

		// get a worker
		var wk string
		select {
		case wk = <-mr.registerChannel:
			mr.Lock()
			mr.workers = append(mr.workers, wk)
			mr.Unlock()
		case wk = <-workerChan:
		}

		// do task
		go func(wk string, arg *DoTaskArgs) {
			if call(wk, "Worker.DoTask", arg, new(struct{})) == false {
				log.Print("schedule: can't call")
				// remove the work from mr.works
				mr.Lock()
				for idx, val := range mr.workers {
					if val == wk {
						log.Print("Schedule: delete worker", wk)
						mr.workers[idx] = mr.workers[len(mr.workers)-1]
						mr.workers = mr.workers[:len(mr.workers)-1]
						break
					}
				}
				mr.Unlock()
				taskChan <- arg
			} else {
				finishedChan <- true

				select {
				case <-doneChan:
					return
				case workerChan <- wk:
					return
				}
			}
		}(wk, arg)

	}
outfor:
	fmt.Printf("Schedule: %v phase done\n", phase)
}

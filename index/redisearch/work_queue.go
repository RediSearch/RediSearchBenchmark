package redisearch

import (
	"time"
)

type resultChan chan interface{}

type workUnit struct {
	f   func(interface{}) interface{}
	v   interface{}
	out resultChan
}

type workQueue chan workUnit

func (wq workQueue) poolWorker() {

	for work := range wq {
		work.out <- work.f(work.v)
	}
}

func NewWorkQueue(numWorkers int) workQueue {
	wc := make(workQueue)
	for i := 0; i < numWorkers; i++ {
		go wc.poolWorker()

	}
	return wc
}

func (wq workQueue) NewTaskGroup() *taskGroup {
	return &taskGroup{
		wq:  wq,
		rc:  make(resultChan, 8),
		num: 0,
	}
}

type taskGroup struct {
	wq  workQueue
	rc  resultChan
	num int
}

func (t *taskGroup) Submit(f func(interface{}) interface{}, v interface{}) {
	t.wq <- workUnit{f, v, t.rc}
	t.num++
}
func (t *taskGroup) Wait(timeout time.Duration) ([]interface{}, error) {
	returns := 0
	//end := time.Now().Add(timeout)
	ret := make([]interface{}, 0, t.num)
	var err error

	for returns < t.num { // && time.Now().Before(end) {

		var res interface{}
		select {
		case res = <-t.rc:
			ret = append(ret, res)
			returns++
			//		case <-time.After(end.Sub(time.Now())):
			//			goto endit
		}

	}

	return ret, err
}

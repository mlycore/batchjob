package batchjob
import(
	"testing"
	"sync"
	mlog "github.com/maxwell92/gokits/log"
)

var log = mlog.Log
const (
	LEN = 18
	PARAL = 4
)

func Test_Run(t *testing.T)  {
	numlist := make([]int, LEN)
	for i:=0; i<LEN; i++ {
		numlist[i] = i
	}
	log.Infof("numlist: %v", numlist)
	f := func(wg *sync.WaitGroup, jobs interface{}){
		dataset := jobs.(int)
		log.Infof("%d", dataset)
		wg.Done()
	}

	batch := NewBatchJob(PARAL)
	batch.Map(numlist)
	batch.Run(f)
}
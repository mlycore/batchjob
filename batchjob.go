package batchjob

import (
	"fmt"
	"sync"
	"time"
)

type BatchJob struct {
	parallism int
	sleep int
	jobs      [][]int
}

func NewBatchJob(seg, sleep int) *BatchJob {
	return &BatchJob{
		parallism: seg,
		sleep: sleep,
	}
}

func (b *BatchJob) Map(data interface{}) {
	set := data.([]int)
	segment := b.parallism
	slice := make([][]int, 0)
	var i, j int
	for i = 0; i < len(set); i += segment {
		slice = append(slice, make([]int, 0))
		if i+segment > len(set)-1 {
			break
		}
		slice[j] = make([]int, 0)
		fmt.Printf("%d, %d\n", i, j)
		fmt.Printf("%p\n", slice[j])
		fmt.Printf("%p\n", set[i:i+segment])
		slice[j] = append(slice[j], set[i:i+segment]...)
		j++
	}
	if i < len(set)-1 {
		slice[j] = make([]int, 0)
		slice[j] = append(slice[j], set[i:len(set)]...)
	}
	b.jobs = slice
	fmt.Printf("jobs: %v\n", b.jobs)
}

func (b *BatchJob) Run(f func(wg *sync.WaitGroup, jobs interface{})) {
	wave := len(b.jobs)
	fmt.Printf("wave total: %d, parallism: %d\n", wave, b.parallism)
	var i int
	for i = 0; i < wave; i++ {
		var actualP int
		if len(b.jobs[i]) < b.parallism {
			actualP = len(b.jobs[i])
		} else {
			actualP = b.parallism
		}
		fmt.Printf("wave: %d, started: %s\n", i, time.Now().String())
		wg := &sync.WaitGroup{}
		wg.Add(actualP)
		for j := 0; j < actualP; j++ {
			go f(wg, b.jobs[i][j])
		}
		wg.Wait()
		time.Sleep(time.Duration(b.sleep) * time.Second)
		fmt.Printf("wave: %d, ended: %s\n", i, time.Now().String())
	}
}

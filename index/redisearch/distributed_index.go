package redisearch

import (
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	"github.com/RedisLabs/RediSearchBenchmark/index"
	"github.com/RedisLabs/RediSearchBenchmark/query"
)

type DistributedIndex struct {
	partitions []index.Index
	part       Partitioner
	timeout    time.Duration
	wq         workQueue
}

func NewDistributedIndex(name string, hosts []string, partitions int, md *index.Metadata) *DistributedIndex {

	part := ModuloPartitioner{len(hosts)}

	subs := make([]index.Index, 0, partitions)
	for i := 0; i < partitions; i++ {
		addr := hosts[i%len(hosts)]
		subs = append(subs, NewIndex(addr, fmt.Sprintf("%s{%d}", name, i), md))
	}

	wq := NewWorkQueue(partitions * 50)

	return &DistributedIndex{
		part:       part,
		partitions: subs,
		timeout:    100 * time.Millisecond,
		wq:         wq,
	}

}

func (i *DistributedIndex) Create() error {
	for _, s := range i.partitions {
		if err := s.Create(); err != nil {
			return err
		}
	}
	return nil
}
func (i *DistributedIndex) Drop() error {
	for _, s := range i.partitions {
		if err := s.Drop(); err != nil {
			return err
		}
	}
	return nil
}
func (i *DistributedIndex) Index(docs []index.Document, options interface{}) error {

	splitDocs := make([][]index.Document, len(i.partitions))
	for _, d := range docs {

		splitDocs[i.part.PartitionFor(d.Id)] = append(splitDocs[i.part.PartitionFor(d.Id)], d)

	}

	var err error
	var wg sync.WaitGroup
	for x, split := range splitDocs {
		wg.Add(1)
		go func(x int, split []index.Document) {
			if e := i.partitions[x].Index(split, options); err != nil {
				err = e
			}
			wg.Done()
		}(x, split)
	}
	wg.Wait()
	return err

}

type searchResult struct {
	docs  []index.Document
	total int
	err   error
}

func (i *DistributedIndex) mergeResults(rs []interface{}, num int) ([]index.Document, int) {

	ret := make([]index.Document, 0, num)
	total := 0
	for _, v := range rs {
		r, ok := v.(searchResult)
		if !ok {
			continue
		}
		if len(ret) < num {
			ret = append(ret, r.docs...)
		}
		total += r.total
	}

	if len(ret) > num {
		ret = ret[:num]
	}
	return ret, total

}
func (i *DistributedIndex) Search(q query.Query) (docs []index.Document, total int, err error) {

	tg := i.wq.NewTaskGroup()

	for n := 0; n < len(i.partitions); n++ {
		tg.Submit(
			func(v interface{}) interface{} {
				sub := v.(index.Index)
				res, total, err := sub.Search(q)
				return searchResult{res, total, err}
			},
			i.partitions[n])
	}

	results, err := tg.Wait(i.timeout)
	docs, total = i.mergeResults(results, int(q.Paging.Num))
	// merge results
	return docs, total, err

}

// Partitioner is the interface that generates partition keys for index keys
type Partitioner interface {
	PartitionFor(id string) uint32
}

// ModuloPartitioner partitions keys based on simple static modulo based hashing function (using crc32)
type ModuloPartitioner struct {
	n int
}

// PartitionFor returns a partition number of a given key
func (m ModuloPartitioner) PartitionFor(id string) uint32 {

	return crc32.ChecksumIEEE([]byte(id)) % uint32(m.n)

}

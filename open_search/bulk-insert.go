package open_search

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	es8 "github.com/elastic/go-elasticsearch/v8"
)

const (
	bulkSize    = 5000
	workerCount = 2
	indexName   = "test-index"
	csvFilePath = "../csv-output/output_00001.csv"
)

type Record struct {
	ID      string `json:"id"`
	Message string `json:"message"`
}

func Bulk_insert_csv() {
	es, err := es8.NewDefaultClient()
	if err != nil {
		panic(err)
	}

	f, _ := os.Open(csvFilePath)
	defer f.Close()

	reader := csv.NewReader(bufio.NewReader(f))
	reader.FieldsPerRecord = -1
	reader.Read() // skip header

	recordCh := make(chan Record, 10000)
	var wg sync.WaitGroup

	// Worker 起動
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go bulkWorker(i, es, recordCh, &wg)
	}

	// データ投入
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(line) < 2 {
			continue
		}
		rec := Record{ID: line[0], Message: line[1]}
		recordCh <- rec
	}

	close(recordCh)
	wg.Wait()
	fmt.Println("完了")
}

func bulkWorker(id int, es *es8.Client, recordCh <-chan Record, wg *sync.WaitGroup) {
	defer wg.Done()
	var (
		count  int
		batch  []Record
		ticker = time.NewTicker(3 * time.Second)
	)

	for {
		select {
		case rec, ok := <-recordCh:
			if !ok {
				flushBulk(id, es, batch)
				return
			}
			batch = append(batch, rec)
			count++
			if count >= bulkSize {
				flushBulk(id, es, batch)
				batch = nil
				count = 0
			}
		case <-ticker.C:
			if count > 0 {
				flushBulk(id, es, batch)
				batch = nil
				count = 0
			}
		}
	}
}

func flushBulk(workerID int, es *es8.Client, records []Record) {
	var buf bytes.Buffer
	for _, rec := range records {
		meta := fmt.Sprintf(`{ "index" : { "_index" : "%s", "_id": "%s" } }%s`, indexName, rec.ID, "\n")
		data, _ := json.Marshal(rec)
		buf.WriteString(meta)
		buf.Write(data)
		buf.WriteString("\n")
	}
	res, err := es.Bulk(bytes.NewReader(buf.Bytes()), es.Bulk.WithContext(context.Background()))
	if err != nil {
		fmt.Printf("[worker %d] bulk error: %v\n", workerID, err)
		return
	}
	defer res.Body.Close()
	if res.IsError() {
		fmt.Printf("[worker %d] response error: %s\n", workerID, res.String())
	}
}

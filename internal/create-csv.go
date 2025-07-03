package internal

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	totalRecords   = 100_000
	recordsPerFile = 100_000
	memoLength     = 4000
	parallelism    = 8
	sentencesFile  = "sentences.txt"
	outputDir      = "output_csv"
)

var lastNames = []string{"山田", "佐藤", "高橋", "田中", "伊藤", "中村", "小林", "加藤", "吉田", "斎藤"}
var firstNames = []string{"太郎", "花子", "一郎", "美咲", "健太", "優子", "翔", "玲奈", "直樹", "舞"}

func loadSentences(path string) ([]string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(content), "\n")
	filtered := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if len(line) > 0 {
			filtered = append(filtered, line+"。")
		}
	}
	return filtered, nil
}

func generateMemo(sentences []string, rng *rand.Rand) string {
	var sb strings.Builder
	for sb.Len() < memoLength {
		sb.WriteString(sentences[rng.Intn(len(sentences))])
	}
	return sb.String()[:memoLength]
}

func randomName(r *rand.Rand) string {
	last := lastNames[r.Intn(len(lastNames))]
	first := firstNames[r.Intn(len(firstNames))]
	return last + first
}

func generateFile(fileIndex int, sentences []string, wg *sync.WaitGroup) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	defer wg.Done()

	fileName := fmt.Sprintf("part_%03d.csv", fileIndex)
	filePath := filepath.Join(outputDir, fileName)

	file, err := os.Create(filePath)
	if err != nil {
		fmt.Printf("error: failed to create file %s: %v\n", filePath, err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// ヘッダー
	writer.Write([]string{"id", "name", "memo"})

	for i := 1; i <= totalRecords; i++ {
		id := fmt.Sprintf("%d", i)
		name := randomName(rng)
		memo := sentences[rng.Intn(len(sentences))]
		writer.Write([]string{id, name, memo})
	}

	fmt.Printf("✅ File generated: %s\n", fileName)
}

func CreateCsvFile() {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		panic(err)
	}

	sentences, err := loadSentences(sentencesFile)
	if err != nil {
		panic(fmt.Errorf("failed to load sentences: %w", err))
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, parallelism)

	totalFiles := totalRecords / recordsPerFile
	for i := 0; i < totalFiles; i++ {
		sem <- struct{}{}
		wg.Add(1)
		go func(index int) {
			defer func() { <-sem }()
			generateFile(index, sentences, &wg)
		}(i)
	}
	wg.Wait()
	fmt.Println("🎉 All files generated.")
}

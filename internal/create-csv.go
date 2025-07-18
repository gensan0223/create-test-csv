package internal

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"
)

const (
	outputFile    = "output.csv"
	numRecords    = 100_000
	memoLength    = 4000 // 文字数（バイト数ではない）
	sentencesFile = "sentences.txt"
)

var lastNames = []string{"山田", "佐藤", "高橋", "田中", "伊藤", "中村", "小林", "加藤", "吉田", "斎藤", "鈴木"}
var firstNames = []string{"太郎", "花子", "一郎", "美咲", "健太", "優子", "翔", "玲奈", "直樹", "舞"}

func loadSentences(path string) ([]rune, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var runes []rune
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		runes = append(runes, []rune(line)...)
	}
	return runes, scanner.Err()
}

func generateMemo(allRunes []rune, rng *rand.Rand) string {
	var builder strings.Builder
	for i := 0; i < memoLength; i++ {
		builder.WriteRune(allRunes[rng.Intn(len(allRunes))])
	}
	return builder.String()
}

func randomName(rng *rand.Rand) string {
	return lastNames[rng.Intn(len(lastNames))] + firstNames[rng.Intn(len(firstNames))]
}

func CreateCsvFile() {
	start := time.Now()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	allRunes, err := loadSentences(sentencesFile)
	if err != nil {
		panic(err)
	}
	if len(allRunes) == 0 {
		panic("sentences.txt に文字がありません")
	}

	file, err := os.Create(outputFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	writer.Write([]string{"id", "name", "memo"})

	for i := 1; i <= numRecords; i++ {
		id := fmt.Sprintf("%d", i)
		name := randomName(rng)
		memo := generateMemo(allRunes, rng)
		writer.Write([]string{id, name, memo})
	}

	fmt.Printf("✅ 完了: %d件, 所要時間: %.2f秒\n", numRecords, time.Since(start).Seconds())
}

package internal

import (
	"fmt"
	"strings"

	faker "github.com/jaswdr/faker/v2"
)

func CreateCsvFile() {
	fake := faker.New()
	var sb strings.Builder

	// 文字数がちょうど4000文字になるまで繰り返す
	for sb.Len() < 4000 {
		sb.WriteString(fake.Lorem().Paragraph(100)) // 100文字ずつ追加
	}

	text := sb.String()[:4000] // ちょうど4000文字に切り詰め
	fmt.Println(text)
	fmt.Println(len(text)) // 4000
}

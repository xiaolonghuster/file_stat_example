package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

/*
# @File : nars_label_stat
# @Author : lixiaolong
# @Email :
# @Desc: Created by GoLand on 2026/1/18 15:28
*/

// Config 配置结构体
type Config struct {
	LabelKey     string `json:"label_key"`      // 指定的label字段名
	Workers      int    `json:"workers"`        // 工作goroutine数量
	BatchSize    int    `json:"batch_size"`     // 批处理大小
	UseFastParse bool   `json:"use_fast_parse"` // 使用快速解析
	Output       string `json:"output"`         // 输出文件目录
	OutputSuffix string `json:"output_suffix"`  // 输出文件后缀
}

// Result 统计结果
type Result struct {
	Directory      string           `json:"directory"`
	FilesProcessed int              `json:"files_processed"`
	TotalLines     int64            `json:"total_lines"`
	UniqueLabels   int              `json:"unique_labels"`
	ProcessingTime float64          `json:"processing_time_seconds"`
	LinesPerSecond float64          `json:"lines_per_second"`
	LabelCounts    map[string]int64 `json:"label_counts"`
	SortedLabels   []LabelCount     `json:"sorted_labels,omitempty"`
	Errors         []string         `json:"errors,omitempty"`
}

// LabelCount 标签计数结构体
type LabelCount struct {
	Label string `json:"label"`
	Count int64  `json:"count"`
}

// Job 工作单元
type Job struct {
	FilePath string
	LabelKey string
}

// WorkerResult worker处理结果
type WorkerResult struct {
	Counts map[string]int64
	Lines  int64
	Error  error
}

// 快速解析label（比完整JSON解析快3-5倍）
func fastExtractLabel(line string) string {
	// 查找 "label": 位置
	labelKey := `"label":`
	pos := strings.Index(line, labelKey)
	if pos == -1 {
		return ""
	}

	// 跳过 "label": 和空白字符
	start := pos + len(labelKey)
	for start < len(line) && (line[start] == ' ' || line[start] == '\t' || line[start] == '\n') {
		start++
	}

	if start >= len(line) {
		return ""
	}

	// 根据值的类型解析
	firstChar := line[start]

	// 字符串值（双引号）
	if firstChar == '"' {
		end := start + 1
		for end < len(line) && line[end] != '"' {
			// 处理转义字符
			if line[end] == '\\' && end+1 < len(line) {
				end += 2
				continue
			}
			end++
		}
		if end < len(line) {
			return line[start+1 : end]
		}
	}

	// 字符串值（单引号）
	if firstChar == '\'' {
		end := start + 1
		for end < len(line) && line[end] != '\'' {
			if line[end] == '\\' && end+1 < len(line) {
				end += 2
				continue
			}
			end++
		}
		if end < len(line) {
			return line[start+1 : end]
		}
	}

	// 数字或其他简单值
	end := start
	for end < len(line) && line[end] != ',' && line[end] != '}' && line[end] != '\n' {
		end++
	}

	value := strings.TrimSpace(line[start:end])

	// 去除可能的引号
	if len(value) >= 2 && ((value[0] == '"' && value[len(value)-1] == '"') ||
		(value[0] == '\'' && value[len(value)-1] == '\'')) {
		value = value[1 : len(value)-1]
	}

	return value
}

// 确保目录存在，如果不存在则创建
func ensureDir(dirPath string) error {
	if dirPath == "" {
		return nil // 空目录路径表示使用当前目录
	}

	// 检查路径是否存在
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		// 创建目录（包括父目录）
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			return fmt.Errorf("无法创建目录 %s: %v", dirPath, err)
		}
		fmt.Printf("已创建输出目录: %s\n", dirPath)
	} else if err != nil {
		// 其他错误
		return fmt.Errorf("无法访问目录 %s: %v", dirPath, err)
	}

	// 检查是否是目录
	if info, err := os.Stat(dirPath); err == nil && !info.IsDir() {
		return fmt.Errorf("输出路径 %s 已存在但不是目录", dirPath)
	}

	return nil
}

// 获取输出文件完整路径
func getOutputFilePath(directory, outputDir, suffix string) (string, error) {
	// 清理路径，确保是绝对路径
	absDir, err := filepath.Abs(directory)
	if err != nil {
		return "", fmt.Errorf("无法获取输入目录的绝对路径: %v", err)
	}

	// 获取目录名
	dirName := filepath.Base(absDir)
	if dirName == "." || dirName == "/" || dirName == "" {
		// 如果是当前目录或根目录，使用默认名称
		dirName = "data"
	}

	// 清理目录名中的特殊字符
	dirName = strings.ReplaceAll(dirName, " ", "_")
	dirName = strings.ReplaceAll(dirName, "/", "_")
	dirName = strings.ReplaceAll(dirName, "\\", "_")
	dirName = strings.ReplaceAll(dirName, ":", "")

	// 构建输出文件名
	if suffix == "" {
		suffix = "_label_stats.json"
	}

	fileName := dirName + suffix

	// 如果有输出目录，组合完整路径
	if outputDir != "" {
		// 确保输出目录存在
		if err := ensureDir(outputDir); err != nil {
			return "", err
		}

		// 获取输出目录的绝对路径
		absOutputDir, err := filepath.Abs(outputDir)
		if err != nil {
			return "", fmt.Errorf("无法获取输出目录的绝对路径: %v", err)
		}

		// 组合完整路径
		return filepath.Join(absOutputDir, fileName), nil
	}

	// 没有指定输出目录，使用当前目录
	return fileName, nil
}

// 处理单个文件
func processFile(filePath, labelKey string, fastParse bool) (map[string]int64, int64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, 0, fmt.Errorf("打开文件失败: %v", err)
	}
	defer file.Close()

	counts := make(map[string]int64)
	var lineCount int64

	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 256*1024)  // 256KB缓冲区
	scanner.Buffer(buf, 10*1024*1024) // 最大10MB的行

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		var label string

		if fastParse {
			label = fastExtractLabel(line)
		} else {
			// 完整JSON解析
			var data map[string]interface{}
			if err := json.Unmarshal([]byte(line), &data); err != nil {
				continue // 跳过无效JSON行
			}

			if labelVal, ok := data[labelKey]; ok {
				switch v := labelVal.(type) {
				case string:
					label = v
				case float64:
					label = fmt.Sprintf("%g", v)
				case int:
					label = fmt.Sprintf("%d", v)
				case bool:
					label = fmt.Sprintf("%v", v)
				default:
					label = fmt.Sprintf("%v", v)
				}
			}
		}

		if label != "" {
			counts[label]++
			lineCount++
		}
	}

	if err := scanner.Err(); err != nil {
		return counts, lineCount, fmt.Errorf("读取文件失败[%s]: %v", filePath, err)
	}

	return counts, lineCount, nil
}

// worker处理文件
func worker(id int, jobs <-chan Job, results chan<- WorkerResult, fastParse bool, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobs {
		counts, lines, err := processFile(job.FilePath, job.LabelKey, fastParse)
		results <- WorkerResult{
			Counts: counts,
			Lines:  lines,
			Error:  err,
		}
	}
}

// 收集JSONL文件
func collectJSONLFiles(directory string) ([]string, error) {
	var files []string

	err := filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			ext := strings.ToLower(filepath.Ext(info.Name()))
			if ext == ".jsonl" || ext == ".json" {
				files = append(files, path)
			}
		}

		return nil
	})

	return files, err
}

// 主统计函数
func countLabels(config Config, directory string) (*Result, error) {
	startTime := time.Now()

	// 收集文件
	files, err := collectJSONLFiles(directory)
	if err != nil {
		return nil, fmt.Errorf("收集文件失败: %v", err)
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("未找到JSONL或JSON文件")
	}

	fmt.Printf("找到 %d 个JSONL/JSON文件\n", len(files))
	fmt.Printf("使用 %d 个worker\n", config.Workers)
	fmt.Printf("快速解析模式: %v\n", config.UseFastParse)

	// 创建工作通道
	jobs := make(chan Job, len(files))
	results := make(chan WorkerResult, len(files))

	// 启动worker
	var wg sync.WaitGroup
	for i := 0; i < config.Workers; i++ {
		wg.Add(1)
		go worker(i, jobs, results, config.UseFastParse, &wg)
	}

	// 发送工作
	go func() {
		for _, file := range files {
			jobs <- Job{FilePath: file, LabelKey: config.LabelKey}
		}
		close(jobs)
	}()

	// 等待worker完成
	go func() {
		wg.Wait()
		close(results)
	}()

	// 收集结果
	totalCounts := make(map[string]int64)
	var totalLines int64
	var errors []string

	for result := range results {
		if result.Error != nil {
			errors = append(errors, result.Error.Error())
			continue
		}

		for label, count := range result.Counts {
			totalCounts[label] += count
		}

		totalLines += result.Lines
	}

	processingTime := time.Since(startTime).Seconds()

	// 创建排序后的标签列表
	var sortedLabels []LabelCount
	for label, count := range totalCounts {
		sortedLabels = append(sortedLabels, LabelCount{
			Label: label,
			Count: count,
		})
	}

	// 按计数降序排序
	sort.Slice(sortedLabels, func(i, j int) bool {
		if sortedLabels[i].Count == sortedLabels[j].Count {
			return sortedLabels[i].Label < sortedLabels[j].Label
		}
		return sortedLabels[i].Count > sortedLabels[j].Count
	})

	return &Result{
		Directory:      directory,
		FilesProcessed: len(files),
		TotalLines:     totalLines,
		UniqueLabels:   len(totalCounts),
		ProcessingTime: processingTime,
		LinesPerSecond: float64(totalLines) / processingTime,
		LabelCounts:    totalCounts,
		SortedLabels:   sortedLabels,
		Errors:         errors,
	}, nil
}

// 保存结果为JSON
func saveResult(result *Result, outputFile string) error {
	// 为了更好的可读性，添加一些格式
	formatted := map[string]interface{}{
		"metadata": map[string]interface{}{
			"directory":         result.Directory,
			"files_processed":   result.FilesProcessed,
			"total_lines":       result.TotalLines,
			"unique_labels":     result.UniqueLabels,
			"processing_time_s": result.ProcessingTime,
			"lines_per_second":  result.LinesPerSecond,
			"errors_count":      len(result.Errors),
			"timestamp":         time.Now().Format("2006-01-02 15:04:05"),
		},
		"label_counts":  result.LabelCounts,
		"sorted_labels": result.SortedLabels,
	}

	if len(result.Errors) > 0 {
		formatted["errors"] = result.Errors
	}

	data, err := json.MarshalIndent(formatted, "", "  ")
	if err != nil {
		return fmt.Errorf("序列化JSON失败: %v", err)
	}

	if err := ioutil.WriteFile(outputFile, data, 0644); err != nil {
		return fmt.Errorf("写入文件失败: %v", err)
	}

	return nil
}

// 打印统计摘要
func printSummary(result *Result, outputFile string) {
	fmt.Printf("\n%s\n", strings.Repeat("=", 70))
	fmt.Println("统计完成!")
	fmt.Printf("输出文件: %s\n", outputFile)
	fmt.Printf("处理了 %d 个文件\n", result.FilesProcessed)
	fmt.Printf("共处理了 %d 行数据\n", result.TotalLines)
	fmt.Printf("发现 %d 个不同的label\n", result.UniqueLabels)
	fmt.Printf("处理时间: %.2f 秒\n", result.ProcessingTime)
	fmt.Printf("处理速度: %.0f 行/秒\n", result.LinesPerSecond)

	if len(result.Errors) > 0 {
		fmt.Printf("错误数量: %d\n", len(result.Errors))
		for i, err := range result.Errors {
			if i < 3 { // 只显示前3个错误
				fmt.Printf("  - %s\n", err)
			}
		}
		if len(result.Errors) > 3 {
			fmt.Printf("  ... 还有 %d 个错误未显示\n", len(result.Errors)-3)
		}
	}

	// 显示Top 10标签
	if len(result.SortedLabels) > 0 {
		fmt.Println("\nTop 10 最常见的label:")

		for i := 0; i < len(result.SortedLabels) && i < 10; i++ {
			percentage := float64(result.SortedLabels[i].Count) / float64(result.TotalLines) * 100
			label := result.SortedLabels[i].Label
			if len(label) > 50 {
				label = label[:47] + "..."
			}
			fmt.Printf("  %2d. %-50s : %10d (%.2f%%)\n",
				i+1, label, result.SortedLabels[i].Count, percentage)
		}
	}

	fmt.Printf("%s\n", strings.Repeat("=", 70))
}

// 显示使用帮助
func showUsage() {
	fmt.Println("JSONL Label Counter - 统计JSONL文件中label字段的分布")
	fmt.Println()
	fmt.Println("使用方法:")
	fmt.Println("  jsonl-counter <目录路径> [选项]")
	fmt.Println()
	fmt.Println("选项:")
	fmt.Println("  -label-key <标签key>  解析标签key (默认: label)")
	fmt.Println("  -output <输出目录>   输出目录 (默认: .)")
	fmt.Println("  -workers <数量>     worker数量 (默认: CPU核心数*2)")
	fmt.Println("  -suffix <后缀>      输出文件后缀 (默认: _label_stats.json)")
	fmt.Println("  -full-parse         使用完整JSON解析 (默认使用快速解析)")
	fmt.Println("  -help               显示此帮助信息")
	fmt.Println()
	fmt.Println("示例:")
	fmt.Println("  jsonl-counter ./data")
	fmt.Println("  jsonl-counter /path/to/dataset -workers 8 -suffix _statistics.json")
	fmt.Println("  jsonl-counter ./logs -full-parse")
}

func main() {
	// 检查参数
	if len(os.Args) < 2 {
		showUsage()
		os.Exit(1)
	}

	// 检查帮助参数
	if os.Args[1] == "-help" || os.Args[1] == "--help" || os.Args[1] == "-h" {
		showUsage()
		os.Exit(0)
	}

	// 默认配置
	config := Config{
		LabelKey:     "label",
		Workers:      runtime.NumCPU() * 2, // CPU核心数 * 2
		BatchSize:    10000,
		UseFastParse: true, // 默认使用快速解析
		Output:       "/Users/lixiaolong/tmp/20260118/file_stat/",
		OutputSuffix: "_label_stats.json",
	}

	// 解析参数
	directory := os.Args[1]
	args := os.Args[2:]

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-label-key":
			if i+1 < len(args) {
				config.LabelKey = args[i+1]
				i++
			}
		case "-output":
			if i+1 < len(args) {
				config.Output = args[i+1]
				i++
			}
		case "-workers":
			if i+1 < len(args) {
				var workers int
				_, err := fmt.Sscanf(args[i+1], "%d", &workers)
				if err == nil && workers > 0 {
					config.Workers = workers
				}
				i++
			}
		case "-suffix":
			if i+1 < len(args) {
				config.OutputSuffix = args[i+1]
				i++
			}
		case "-full-parse":
			config.UseFastParse = false
		default:
			fmt.Printf("警告: 未知参数 %s\n", args[i])
		}
	}

	// 检查目录
	dirInfo, err := os.Stat(directory)
	if os.IsNotExist(err) {
		log.Fatalf("错误: 目录不存在: %s", directory)
	}
	if err != nil {
		log.Fatalf("错误: 无法访问目录 %s: %v", directory, err)
	}
	if !dirInfo.IsDir() {
		log.Fatalf("错误: %s 不是一个目录", directory)
	}

	fmt.Printf("开始处理目录: %s\n", directory)

	// 获取输出文件完整路径
	outputFile, err := getOutputFilePath(directory, config.Output, config.OutputSuffix)
	if err != nil {
		log.Fatalf("获取输出文件路径失败: %v", err)
	}
	fmt.Printf("输出文件: %s\n", outputFile)

	// 执行统计
	result, err := countLabels(config, directory)
	if err != nil {
		log.Fatalf("统计失败: %v", err)
	}

	// 保存结果
	if err := saveResult(result, outputFile); err != nil {
		log.Fatalf("保存结果失败: %v", err)
	}

	// 打印摘要
	printSummary(result, outputFile)

	fmt.Printf("结果已保存到: %s\n", outputFile)
}

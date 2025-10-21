package main

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/tencentyun/cos-go-sdk-v5"
)

// initCOSClient 初始化COS客户端
func initCOSClient() (*cos.Client, error) {
	// 加载.env文件
	err := godotenv.Load()
	if err != nil {
		fmt.Printf("警告: 无法加载.env文件: %v\n", err)
		fmt.Println("将使用环境变量中的配置")
	}

	// 从环境变量中获取腾讯云密钥
	secretId := os.Getenv("TENCENTCLOUD_SECRET_ID")
	secretKey := os.Getenv("TENCENTCLOUD_SECRET_KEY")

	if secretId == "" || secretKey == "" {
		return nil, fmt.Errorf("腾讯云密钥未配置，请在.env文件或环境变量中设置TENCENTCLOUD_SECRET_ID和TENCENTCLOUD_SECRET_KEY")
	}

	// 从环境变量中获取存储桶名称和地域
	bucketName := os.Getenv("COS_BUCKET_NAME")
	if bucketName == "" {
		return nil, fmt.Errorf("存储桶名称未配置，请在.env文件中设置COS_BUCKET_NAME")
	}

	region := os.Getenv("COS_REGION")
	if region == "" {
		return nil, fmt.Errorf("地域未配置，请在.env文件中设置COS_REGION")
	}

	fmt.Printf("使用存储桶: %s, 地域: %s\n", bucketName, region)

	// CI 任务需要提供 CIURL
	bu, _ := url.Parse(fmt.Sprintf("https://%s.cos.%s.myqcloud.com", bucketName, region))
	cu, _ := url.Parse(fmt.Sprintf("https://%s.ci.%s.myqcloud.com", bucketName, region))
	b := &cos.BaseURL{BucketURL: bu, CIURL: cu}

	// 创建客户端
	client := cos.NewClient(b, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  secretId,
			SecretKey: secretKey,
		},
	})

	return client, nil
}

// ensureCOSDirectory 确保COS目录存在，不存在则创建
func ensureCOSDirectory(client *cos.Client, dirPath string) error {
	// 处理空路径的情况
	if dirPath == "" {
		return nil // 根目录不需要检查
	}

	// 确保路径不以斜杠开头或结尾
	cleanPath := strings.Trim(dirPath, "/")
	if cleanPath == "" {
		return nil // 空路径表示根目录
	}

	// 检查目录是否存在（通过尝试获取目录属性）
	_, err := client.Object.Head(context.Background(), cleanPath+"/", nil)
	if err == nil {
		// 目录已存在
		fmt.Printf("目录已存在: %s\n", cleanPath)
		return nil
	}

	// 如果是404错误，说明目录不存在，需要创建
	if cos.IsNotFoundError(err) {
		fmt.Printf("目录不存在，正在创建: %s\n", cleanPath)

		// 创建一个空对象作为目录标记
		emptyReader := strings.NewReader("")
		_, err = client.Object.Put(context.Background(), cleanPath+"/", emptyReader, nil)
		if err != nil {
			return fmt.Errorf("创建目录失败: %v", err)
		}

		fmt.Printf("目录创建成功: %s\n", cleanPath)
		return nil
	}

	// 其他错误
	return fmt.Errorf("检查目录失败: %v", err)
}

// generateFileName 根据路径生成带时间戳的文件名
func generateFileName(sourcePath string, isDir bool) string {
	now := time.Now()
	timeStamp := now.Format("20060102_150405")

	// 获取文件或文件夹名称
	fileName := filepath.Base(sourcePath)

	if isDir {
		// 文件夹压缩为ZIP
		return fmt.Sprintf("%s_%s.zip", fileName, timeStamp)
	} else {
		// 文件保持原格式，添加时间戳
		ext := filepath.Ext(fileName)
		nameWithoutExt := fileName[:len(fileName)-len(ext)]
		return fmt.Sprintf("%s_%s%s", nameWithoutExt, timeStamp, ext)
	}
}

// parseSourcePaths 解析SOURCEFOLDER环境变量，支持多个路径
func parseSourcePaths(sourceFolders string) []string {
	// 按逗号分割路径
	paths := strings.Split(sourceFolders, ",")
	var result []string

	for _, path := range paths {
		// 去除前后空格
		trimmedPath := strings.TrimSpace(path)
		if trimmedPath != "" {
			result = append(result, trimmedPath)
		}
	}

	return result
}

// isDirectory 检查路径是否为目录
func isDirectory(path string) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		return false, err
	}
	return info.IsDir(), nil
}

// zipFolder 将文件夹压缩为ZIP文件
func zipFolder(source, target string) error {
	// 创建目标ZIP文件
	zipFile, err := os.Create(target)
	if err != nil {
		return fmt.Errorf("创建ZIP文件失败: %v", err)
	}
	defer zipFile.Close()

	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()

	// 遍历源文件夹
	return filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// 计算相对路径
		relPath, err := filepath.Rel(source, path)
		if err != nil {
			return fmt.Errorf("计算相对路径失败: %v", err)
		}

		// 跳过根目录本身
		if relPath == "." {
			return nil
		}

		// 创建ZIP文件头
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return fmt.Errorf("创建文件头失败: %v", err)
		}

		// 设置ZIP文件头中的路径
		header.Name = relPath
		if info.IsDir() {
			header.Name += "/"
		}

		// 创建文件写入器
		writer, err := zipWriter.CreateHeader(header)
		if err != nil {
			return fmt.Errorf("创建ZIP写入器失败: %v", err)
		}

		// 如果是文件，复制文件内容
		if !info.IsDir() {
			file, err := os.Open(path)
			if err != nil {
				return fmt.Errorf("打开文件失败: %v", err)
			}
			defer file.Close()

			_, err = io.Copy(writer, file)
			if err != nil {
				return fmt.Errorf("复制文件内容失败: %v", err)
			}
		}

		return nil
	})
}

// getNextCleanupTime 计算下次清理时间
func getNextCleanupTime() (time.Time, error) {
	cleanupTime := os.Getenv("CLEANUP_TIME")
	if cleanupTime == "" {
		return time.Time{}, fmt.Errorf("CLEANUP_TIME未配置")
	}

	// 解析清理时间
	parts := strings.Split(cleanupTime, ":")
	if len(parts) != 2 {
		return time.Time{}, fmt.Errorf("CLEANUP_TIME格式错误，应为HH:MM格式，当前为: %s", cleanupTime)
	}

	hour, err1 := strconv.Atoi(parts[0])
	minute, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil {
		return time.Time{}, fmt.Errorf("CLEANUP_TIME解析失败: %v, %v", err1, err2)
	}

	now := time.Now()

	// 构造今天的清理时间
	cleanupToday := time.Date(now.Year(), now.Month(), now.Day(), hour, minute, 0, 0, now.Location())

	// 如果今天的清理时间已过，则设置为明天的清理时间
	if cleanupToday.Before(now) {
		cleanupToday = cleanupToday.Add(24 * time.Hour)
	}

	return cleanupToday, nil
}

// getWhiteList 获取白名单前缀
func getWhiteList() []string {
	whitelistStr := os.Getenv("CLEANUP_WHITELIST")
	if whitelistStr == "" {
		return []string{}
	}

	prefixes := strings.Split(whitelistStr, ",")
	var result []string
	for _, prefix := range prefixes {
		trimmed := strings.TrimSpace(prefix)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

// parseFileName 解析文件名，提取前缀和时间戳
func parseFileName(fileName string) (prefix string, timeStamp string, isOurFormat bool) {
	// 匹配我们的文件格式：前缀_YYYYMMDD_HHMMSS.扩展名
	// 例如：test1_20251021_095449.txt 或 VCPToolBox_20251021_095449.zip
	re := regexp.MustCompile(`^(.+?)_(\d{8}_\d{6})\..+$`)
	matches := re.FindStringSubmatch(fileName)

	if len(matches) == 3 {
		return matches[1], matches[2], true
	}

	// 如果不匹配我们的格式，返回空值
	return "", "", false
}

// isFileOlderThanDays 检查文件是否超过指定天数
func isFileOlderThanDays(timeStamp string, maxDays int) bool {
	// 解析时间戳：YYYYMMDD_HHMMSS (本地时间)
	parsedTime, err := time.ParseInLocation("20060102_150405", timeStamp, time.Local)
	if err != nil {
		fmt.Printf("警告: 时间戳解析失败: %s, 错误: %v\n", timeStamp, err)
		return false
	}

	// 计算文件年龄
	now := time.Now()
	age := now.Sub(parsedTime)
	older := age.Hours() > float64(maxDays*24)

	// 调试信息
	if maxDays == 0 {
		fmt.Printf("调试: 文件时间 %s, 当前时间 %s, 年龄 %.1f 小时, 超过 %d 天: %v\n",
			parsedTime.Format("2006-01-02 15:04:05"),
			now.Format("2006-01-02 15:04:05"),
			age.Hours(), maxDays, older)
	}

	return older
}

// isWhitelisted 检查文件前缀是否在白名单中
func isWhitelisted(prefix string, whitelist []string) bool {
	for _, allowedPrefix := range whitelist {
		if prefix == allowedPrefix {
			return true
		}
	}
	return false
}

// listCOSFiles 获取COS目录中的文件列表
func listCOSFiles(client *cos.Client, dirPath string) ([]string, error) {
	var fileNames []string

	opt := &cos.BucketGetOptions{
		Prefix:  strings.Trim(dirPath, "/") + "/",
		MaxKeys: 1000,
	}

	v, _, err := client.Bucket.Get(context.Background(), opt)
	if err != nil {
		return nil, fmt.Errorf("获取COS文件列表失败: %v", err)
	}

	for _, content := range v.Contents {
		// 跳过目录标记（以/结尾的）
		if !strings.HasSuffix(content.Key, "/") {
			// 移除目录前缀，只保留文件名
			fileName := strings.TrimPrefix(content.Key, strings.Trim(dirPath, "/")+"/")
			fileNames = append(fileNames, fileName)
		}
	}

	return fileNames, nil
}

// deleteCOSFile 删除COS中的文件
func deleteCOSFile(client *cos.Client, dirPath, fileName string) error {
	cosPath := ""
	if dirPath == "" {
		cosPath = fileName
	} else {
		cosPath = fmt.Sprintf("%s/%s", strings.Trim(dirPath, "/"), fileName)
	}

	_, err := client.Object.Delete(context.Background(), cosPath)
	if err != nil {
		return fmt.Errorf("删除COS文件失败: %s, 错误: %v", cosPath, err)
	}

	fmt.Printf("已删除文件: %s\n", cosPath)
	return nil
}

// performBackup 执行备份操作
func performBackup(client *cos.Client, targetDir string) {
	fmt.Printf("\n=== 开始执行备份 ===\n")

	// 本地文件/文件夹路径配置
	sourceFolders := os.Getenv("SOURCEFOLDER")
	if sourceFolders == "" {
		fmt.Printf("警告: SOURCEFOLDER未配置")
		return
	}

	// 解析多个路径
	sourcePaths := parseSourcePaths(sourceFolders)
	fmt.Printf("发现 %d 个路径需要处理:\n", len(sourcePaths))
	for i, path := range sourcePaths {
		fmt.Printf("  %d. %s\n", i+1, path)
	}

	// 处理每个路径
	var tempFiles []string // 存储临时文件路径，用于最后清理
	successCount := 0

	for _, sourcePath := range sourcePaths {
		fmt.Printf("\n--- 处理: %s ---\n", sourcePath)

		// 检查路径是否存在
		if _, err := os.Stat(sourcePath); os.IsNotExist(err) {
			fmt.Printf("错误: 路径不存在: %s\n", sourcePath)
			continue
		}

		// 检查是文件还是目录
		isDir, err := isDirectory(sourcePath)
		if err != nil {
			fmt.Printf("错误: 检查路径类型失败: %v\n", err)
			continue
		}

		var localFilePath string
		var cosFileName string

		if isDir {
			// 文件夹：压缩为ZIP
			cosFileName = generateFileName(sourcePath, true)
			localFilePath = filepath.Join(os.TempDir(), cosFileName)

			fmt.Printf("开始压缩文件夹: %s -> %s\n", sourcePath, localFilePath)
			err = zipFolder(sourcePath, localFilePath)
			if err != nil {
				fmt.Printf("错误: 压缩文件夹失败: %v\n", err)
				continue
			}
			fmt.Printf("文件夹压缩成功: %s\n", localFilePath)
			tempFiles = append(tempFiles, localFilePath) // 添加到临时文件列表
		} else {
			// 文件：直接上传
			cosFileName = generateFileName(sourcePath, false)
			localFilePath = sourcePath
			fmt.Printf("直接上传文件: %s\n", sourcePath)
		}

		// 构造COS路径
		var cosPath string
		if targetDir == "" {
			cosPath = cosFileName
		} else {
			cleanDir := strings.TrimRight(targetDir, "/")
			cleanFileName := strings.TrimLeft(cosFileName, "/")
			cosPath = fmt.Sprintf("%s/%s", cleanDir, cleanFileName)
		}

		// 上传文件
		fmt.Printf("开始上传文件: %s -> %s\n", localFilePath, cosPath)
		_, err = client.Object.PutFromFile(context.Background(), cosPath, localFilePath, nil)
		if err != nil {
			fmt.Printf("错误: 上传文件失败: %v\n", err)
			continue
		}

		// 验证上传
		fmt.Printf("文件上传成功: %s\n", cosPath)
		resp, err := client.Object.Head(context.Background(), cosPath, nil)
		if err != nil {
			fmt.Printf("警告: 验证上传文件失败: %v\n", err)
		} else {
			fmt.Printf("文件验证成功，大小: %d bytes\n", resp.ContentLength)
		}

		successCount++
	}

	// 清理临时文件
	for _, tempFile := range tempFiles {
		if err := os.Remove(tempFile); err != nil {
			fmt.Printf("警告: 删除临时文件失败: %s, 错误: %v\n", tempFile, err)
		}
	}

	// 输出备份汇总信息
	fmt.Printf("\n=== 备份完成 ===\n")
	fmt.Printf("总路径数: %d\n", len(sourcePaths))
	fmt.Printf("成功上传: %d\n", successCount)
	fmt.Printf("失败数量: %d\n", len(sourcePaths)-successCount)
}

// performCleanup 执行清理操作
func performCleanup(client *cos.Client, targetDir string) {
	// 检查是否启用清理
	cleanupEnabled := os.Getenv("CLEANUP_ENABLED")
	if cleanupEnabled != "true" {
		return
	}

	fmt.Printf("\n=== 开始执行定时清理 ===\n")

	// 获取配置
	cleanupDaysStr := os.Getenv("CLEANUP_DAYS")
	cleanupDays := 7 // 默认7天
	if cleanupDaysStr != "" {
		if days, err := strconv.Atoi(cleanupDaysStr); err == nil {
			cleanupDays = days
		}
	}

	whitelist := getWhiteList()
	fmt.Printf("清理配置: 保留天数=%d, 白名单=%v\n", cleanupDays, whitelist)

	// 获取文件列表
	fileNames, err := listCOSFiles(client, targetDir)
	if err != nil {
		fmt.Printf("错误: %v\n", err)
		return
	}

	fmt.Printf("发现 %d 个文件需要检查\n", len(fileNames))

	deletedCount := 0
	for _, fileName := range fileNames {
		prefix, timeStamp, isOurFormat := parseFileName(fileName)

		// 检查是否是我们上传的文件格式
		if !isOurFormat {
			fmt.Printf("跳过非程序上传文件: %s\n", fileName)
			continue
		}

		// 检查文件是否超过保留天数
		if !isFileOlderThanDays(timeStamp, cleanupDays) {
			fmt.Printf("文件未超过保留天数: %s\n", fileName)
			continue
		}

		// 检查文件前缀是否在白名单中
		if isWhitelisted(prefix, whitelist) {
			fmt.Printf("文件在白名单中，跳过删除: %s\n", fileName)
			continue
		}

		// 删除文件
		fmt.Printf("删除过期文件: %s (前缀: %s, 时间: %s)\n", fileName, prefix, timeStamp)
		err := deleteCOSFile(client, targetDir, fileName)
		if err != nil {
			fmt.Printf("删除失败: %v\n", err)
		} else {
			deletedCount++
		}
	}

	fmt.Printf("=== 清理完成，删除了 %d 个文件 ===\n", deletedCount)
}

func main() {
	// 加载.env文件
	err := godotenv.Load()
	if err != nil {
		fmt.Printf("警告: 无法加载.env文件: %v\n", err)
		fmt.Println("将使用环境变量中的配置")
	}

	// COS上的目标目录
	targetDir := os.Getenv("COS_TARGET_DIR")

	// 初始化COS客户端
	client, err := initCOSClient()
	if err != nil {
		fmt.Printf("错误: 初始化COS客户端失败: %v\n", err)
		return
	}

	// 确保目标目录存在
	err = ensureCOSDirectory(client, targetDir)
	if err != nil {
		fmt.Printf("错误: 确保目录存在失败: %v\n", err)
		return
	}
	fmt.Printf("程序启动，将持续运行并定时执行备份和清理任务\n")
	fmt.Printf("存储桶: %s, 地域: %s, 目标目录: %s\n",
		os.Getenv("COS_BUCKET_NAME"), os.Getenv("COS_REGION"), targetDir)

	// 主循环
	for {
		// 获取下次清理时间
		nextCleanupTime, err := getNextCleanupTime()
		if err != nil {
			fmt.Printf("错误: 获取清理时间失败: %v\n", err)
			// 如果配置错误，设置为24小时后重试
			nextCleanupTime = time.Now().Add(24 * time.Hour)
		}

		now := time.Now()
		waitDuration := nextCleanupTime.Sub(now)

		fmt.Printf("\n当前时间: %s\n", now.Format("2006-01-02 15:04:05"))
		fmt.Printf("下次清理时间: %s\n", nextCleanupTime.Format("2006-01-02 15:04:05"))
		fmt.Printf("等待时间: %v\n", waitDuration)

		// 等待到清理时间
		if waitDuration > 0 {
			fmt.Printf("等待中...\n")
			time.Sleep(waitDuration)
		}

		// 执行备份
		performBackup(client, targetDir)

		// 执行清理
		performCleanup(client, targetDir)

		// 等待1分钟后重新计算清理时间
		fmt.Printf("\n等待1分钟后重新计算清理时间...\n")
		time.Sleep(1 * time.Minute)
	}
}

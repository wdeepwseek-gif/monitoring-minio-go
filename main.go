package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/notification"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"gopkg.in/yaml.v3"
)

// Config структура для конфигурации
type Config struct {
	Minio struct {
		Endpoint       string `yaml:"endpoint"`
		AccessKey      string `yaml:"accessKey"`
		SecretKey      string `yaml:"secretKey"`
		UseSSL         bool   `yaml:"useSSL"`
		BucketToWatch  string `yaml:"bucketToWatch"`
	} `yaml:"minio"`
	Telegram struct {
		BotToken string `yaml:"botToken"`
	} `yaml:"telegram"`
	Chats []struct {
		ID      int64  `yaml:"id"`
		TopicID int64  `yaml:"topic_id"`
		Label   string `yaml:"label"`
	} `yaml:"chats"`
	Topics map[string]int64 `yaml:"topics,omitempty"`
	Notifications struct {
		EnabledEvents      []string `yaml:"enabledEvents"`
		ExcludeExtensions  []string `yaml:"excludeExtensions"`
		MinSizeBytes       int64    `yaml:"minSizeBytes"`
		MaxSizeBytes       int64    `yaml:"maxSizeBytes"`
	} `yaml:"notifications"`
}

const (
	logDir           = "logs"
	logRetentionDays = 30
	queueBufferSize  = 100
	maxSendAttempts  = 5
	retryDelay       = time.Minute
	queueInterval    = 9 * time.Second
)

var (
	config          Config
	bot             *tgbotapi.BotAPI
	logger          *log.Logger
	logFile         *os.File
	logMutex        sync.Mutex
	currentLogDate  string
	messageQueue    chan *queuedMessage
	logDirectory    string
)

type queuedMessage struct {
	ChatID      int64
	TopicID     int64
	ChatLabel   string
	ObjectTopic string
	Text        string
	Attempts    int
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := initLogger(ctx); err != nil {
		log.Fatalf("❌ Не удалось инициализировать логирование: %v", err)
	}
	defer closeLogFile()

	messageQueue = make(chan *queuedMessage, queueBufferSize)
	go startMessageDispatcher(ctx)

	logger.Println("🚀 Запуск MinIO Telegram Monitor...")

	logger.Println("📋 Загрузка конфигурации...")
	loadConfig()
	logger.Println("✅ Конфигурация загружена успешно")

	logger.Println("🤖 Инициализация Telegram бота...")
	initTelegramBot()
	logger.Println("✅ Telegram бот инициализирован")

	logger.Println("🪣 Подключение к MinIO...")
	minioClient := initMinioClient()
	logger.Println("✅ MinIO клиент инициализирован")

	logger.Printf("📨 Очередь уведомлений активна (буфер %d, интервал %s)", queueBufferSize, queueInterval)
	logger.Println("👀 Запуск мониторинга MinIO...")
	startMinioMonitoring(ctx, minioClient)
}

func initLogger(ctx context.Context) error {
	resolvedDir, err := resolveLogDirectory()
	if err != nil {
		return fmt.Errorf("не удалось определить каталог логов: %w", err)
	}
	logDirectory = resolvedDir

	if err := os.MkdirAll(logDirectory, 0o755); err != nil {
		return fmt.Errorf("не удалось создать каталог логов %s: %w", logDirectory, err)
	}

	filePath, err := rotateLogFile()
	if err != nil {
		return fmt.Errorf("не удалось открыть лог-файл: %w", err)
	}

	if logger == nil {
		logWriter := io.MultiWriter(os.Stdout, logFile)
		logger = log.New(logWriter, "", log.LstdFlags|log.Lmicroseconds)
	} else {
		logger.SetOutput(io.MultiWriter(os.Stdout, logFile))
		logger.SetFlags(log.LstdFlags | log.Lmicroseconds)
	}

	if filePath != "" {
		logger.Printf("📝 Логирование инициализировано: %s", filePath)
	}

	logger.Printf("📁 Каталог логов: %s", logDirectory)
	if workingDir, err := os.Getwd(); err == nil {
		logger.Printf("📁 Рабочая директория: %s", workingDir)
	}

	if err := cleanupOldLogs(); err != nil {
		logger.Printf("⚠️ Ошибка очистки старых логов: %v", err)
	}

	go monitorLogRotation(ctx)

	return nil
}

func rotateLogFile() (string, error) {
	logMutex.Lock()
	defer logMutex.Unlock()

	today := time.Now().Format("2006-01-02")
	if currentLogDate == today && logFile != nil {
		return "", nil
	}

	fileName := fmt.Sprintf("%s.log", today)
	filePath := filepath.Join(logDirectory, fileName)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return "", err
	}

	if logFile != nil {
		_ = logFile.Close()
	}

	logFile = file
	currentLogDate = today

	output := io.MultiWriter(os.Stdout, logFile)
	if logger == nil {
		logger = log.New(output, "", log.LstdFlags|log.Lmicroseconds)
	} else {
		logger.SetOutput(output)
		logger.SetFlags(log.LstdFlags | log.Lmicroseconds)
	}

	return filePath, nil
}

func closeLogFile() {
	logMutex.Lock()
	defer logMutex.Unlock()

	if logFile != nil {
		_ = logFile.Close()
		logFile = nil
	}
}

func monitorLogRotation(ctx context.Context) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if logger != nil {
				logger.Println("🛑 Остановка мониторинга логов")
			}
			return
		case <-ticker.C:
			filePath, err := rotateLogFile()
			if err != nil {
				log.Printf("❌ Ошибка ротации логов: %v", err)
				continue
			}
			if filePath != "" && logger != nil {
				logger.Printf("🔄 Обновлен лог-файл: %s", filePath)
			}
			if err := cleanupOldLogs(); err != nil {
				if logger != nil {
					logger.Printf("⚠️ Ошибка очистки старых логов: %v", err)
				}
			}
		}
	}
}

func cleanupOldLogs() error {
	if logDirectory == "" {
		return nil
	}

	entries, err := os.ReadDir(logDirectory)
	if err != nil {
		return err
	}

	cutoff := time.Now().Add(-time.Hour * 24 * time.Duration(logRetentionDays))

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if filepath.Ext(entry.Name()) != ".log" {
			continue
		}

		nameWithoutExt := strings.TrimSuffix(entry.Name(), filepath.Ext(entry.Name()))
		fileDate, err := time.Parse("2006-01-02", nameWithoutExt)
		if err != nil {
			continue
		}

		if fileDate.Before(cutoff) {
			path := filepath.Join(logDirectory, entry.Name())
			if err := os.Remove(path); err != nil {
				if logger != nil {
					logger.Printf("⚠️ Не удалось удалить лог-файл %s: %v", path, err)
				}
				continue
			}
			if logger != nil {
				logger.Printf("🧹 Удален устаревший лог-файл: %s", path)
			}
		}
	}

	return nil
}

func resolveLogDirectory() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}

	return filepath.Join(cwd, logDir), nil
}

func loadConfig() {
	// Получаем путь к директории, где находится исполняемый файл
	exePath, err := os.Executable()
	if err != nil {
		logger.Fatalf("❌ Ошибка получения пути к исполняемому файлу: %v", err)
	}
	exeDir := filepath.Dir(exePath)

	// Пробуем несколько возможных путей к конфигурации
	possiblePaths := []string{
		filepath.Join(exeDir, "config.yaml"),    // рядом с исполняемым файлом
		"./config.yaml",                         // в текущей рабочей директории
		"/app/config.yaml",                      // стандартный путь в Docker контейнере
		"/config.yaml",                          // абсолютный путь
	}

	var configFile []byte
	var configPath string

	for _, path := range possiblePaths {
		logger.Printf("🔍 Поиск конфига по пути: %s", path)
		configFile, err = os.ReadFile(path)
		if err == nil {
			configPath = path
			logger.Printf("✅ Конфиг найден: %s", path)
			break
		}
		logger.Printf("❌ Конфиг не найден по пути: %s", path)
	}

	if configFile == nil {
		// Выводим список файлов в директории исполняемого файла для отладки
		files, _ := os.ReadDir(exeDir)
		logger.Printf("📂 Содержимое директории исполняемого файла (%s):", exeDir)
		for _, file := range files {
			logger.Printf("   - %s", file.Name())
		}
		
		// Также выводим список файлов в рабочей директории
		workingDir, _ := os.Getwd()
		files, _ = os.ReadDir(workingDir)
		logger.Printf("📂 Содержимое рабочей директории (%s):", workingDir)
		for _, file := range files {
			logger.Printf("   - %s", file.Name())
		}
		
		logger.Fatalf("❌ Конфигурационный файл config.yaml не найден. Проверенные пути: %v", possiblePaths)
	}

	err = yaml.Unmarshal(configFile, &config)
	if err != nil {
		logger.Fatalf("❌ Ошибка парсинга config.yaml: %v", err)
	}
	
	// Переопределение из переменных окружения (более безопасно для продакшена)
	overrideFromEnv()
	
	// Валидация конфигурации
	if config.Minio.Endpoint == "" {
		logger.Fatal("❌ Не указан MinIO endpoint в конфигурации")
	}
	if config.Telegram.BotToken == "" {
		logger.Fatal("❌ Не указан Telegram Bot Token в конфигурации")
	}
	if len(config.Chats) == 0 {
		logger.Fatal("❌ Не указаны чаты для отправки уведомлений")
	}
	
	// Установка значений по умолчанию для размеров файлов
	if config.Notifications.MinSizeBytes == 0 {
		config.Notifications.MinSizeBytes = 0
	}
	if config.Notifications.MaxSizeBytes == 0 {
		config.Notifications.MaxSizeBytes = 1024 * 1024 * 1024 // 1GB по умолчанию
	}
	
	logger.Printf("✅ Конфигурация загружена из %s", configPath)
}

// overrideFromEnv переопределяет чувствительные данные из переменных окружения
// Это более безопасный способ хранения паролей и токенов
func overrideFromEnv() {
	// MinIO credentials
	if accessKey := os.Getenv("MINIO_ACCESS_KEY"); accessKey != "" {
		config.Minio.AccessKey = accessKey
		if logger != nil {
			logger.Println("🔒 MinIO AccessKey загружен из переменной окружения")
		}
	}
	if secretKey := os.Getenv("MINIO_SECRET_KEY"); secretKey != "" {
		config.Minio.SecretKey = secretKey
		if logger != nil {
			logger.Println("🔒 MinIO SecretKey загружен из переменной окружения")
		}
	}
	if endpoint := os.Getenv("MINIO_ENDPOINT"); endpoint != "" {
		config.Minio.Endpoint = endpoint
		if logger != nil {
			logger.Println("🔒 MinIO Endpoint загружен из переменной окружения")
		}
	}
	if bucket := os.Getenv("MINIO_BUCKET"); bucket != "" {
		config.Minio.BucketToWatch = bucket
		if logger != nil {
			logger.Println("🔒 MinIO Bucket загружен из переменной окружения")
		}
	}
	
	// Telegram credentials
	if botToken := os.Getenv("TELEGRAM_BOT_TOKEN"); botToken != "" {
		config.Telegram.BotToken = botToken
		if logger != nil {
			logger.Println("🔒 Telegram Bot Token загружен из переменной окружения")
		}
	}
}

func initTelegramBot() {
	var err error
	bot, err = tgbotapi.NewBotAPI(config.Telegram.BotToken)
	if err != nil {
		logger.Fatalf("❌ Ошибка инициализации Telegram бота: %v", err)
	}

	bot.Debug = false // Отключаем debug в продакшене
	logger.Printf("✅ Telegram бот авторизован как @%s", bot.Self.UserName)
	
	// Проверяем доступность бота
	_, err = bot.GetMe()
	if err != nil {
		logger.Fatalf("❌ Не удалось получить информацию о боте: %v", err)
	}
}

func initMinioClient() *minio.Client {
	minioClient, err := minio.New(config.Minio.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.Minio.AccessKey, config.Minio.SecretKey, ""),
		Secure: config.Minio.UseSSL,
	})
	if err != nil {
		logger.Fatalf("❌ Ошибка инициализации MinIO клиента: %v", err)
	}

	// Проверяем подключение к MinIO
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	_, err = minioClient.ListBuckets(ctx)
	if err != nil {
		logger.Fatalf("❌ Не удалось подключиться к MinIO %s: %v", config.Minio.Endpoint, err)
	}

	logger.Printf("✅ Подключено к MinIO: %s", config.Minio.Endpoint)
	return minioClient
}

func startMinioMonitoring(ctx context.Context, minioClient *minio.Client) {
	if ctx == nil {
		ctx = context.Background()
	}

	// Проверяем существует ли бакет
	exists, err := minioClient.BucketExists(ctx, config.Minio.BucketToWatch)
	if err != nil {
		logger.Fatalf("❌ Ошибка проверки бакета: %v", err)
	}
	if !exists {
		logger.Fatalf("❌ Бакет '%s' не существует", config.Minio.BucketToWatch)
	}

	logger.Printf("✅ Начинаем мониторинг бакета: %s", config.Minio.BucketToWatch)
	logger.Printf("🔔 Отслеживаемые события: %v", config.Notifications.EnabledEvents)

	// Создаем канал для событий
	events := minioClient.ListenBucketNotification(ctx, config.Minio.BucketToWatch, "", "", config.Notifications.EnabledEvents)

	// Таймер для проверки соединения каждые 10 минут
	connectionCheckTicker := time.NewTicker(10 * time.Minute)
	defer connectionCheckTicker.Stop()

	// Обрабатываем события
	for {
		select {
		case <-ctx.Done():
			logger.Println("🛑 Мониторинг MinIO остановлен")
			return
		case notificationInfo, ok := <-events:
			if !ok {
				logger.Println("⚠️ Канал уведомлений MinIO закрыт. Пытаемся переподключиться через 5 секунд...")
				time.Sleep(5 * time.Second)
				events = minioClient.ListenBucketNotification(ctx, config.Minio.BucketToWatch, "", "", config.Notifications.EnabledEvents)
				continue
			}
			if notificationInfo.Err != nil {
				logger.Printf("⚠️ Ошибка получения события: %v", notificationInfo.Err)
				continue
			}

			for _, record := range notificationInfo.Records {
				processMinioEvent(record)
			}
		case <-connectionCheckTicker.C:
			// Проверка соединения каждые 10 минут
			logger.Println("💓 Проверка соединения с MinIO...")
			checkMinioConnection(minioClient)
		}
	}
}

func checkMinioConnection(minioClient *minio.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	_, err := minioClient.ListBuckets(ctx)
	if err != nil {
		logger.Printf("❌ Потеряно соединение с MinIO: %v", err)
	} else {
		logger.Println("✅ Соединение с MinIO активно")
	}
}

func processMinioEvent(record notification.Event) {
	// Проверяем, включено ли это событие
	if !isEventEnabled(record.EventName) {
		return
	}

	// Определяем тип события
	eventParts := strings.Split(record.EventName, ":")
	if len(eventParts) < 2 {
		logger.Printf("Неизвестный формат события: %s", record.EventName)
		return
	}

	eventType := eventParts[1]
	objectName := record.S3.Object.Key
	bucketName := record.S3.Bucket.Name
	objectSize := record.S3.Object.Size
	
	// Используем московское время (UTC+3)
	moscowTime := time.Now().UTC().Add(3 * time.Hour)
	eventTime := moscowTime.Format("2006-01-02 15:04:05")

	// Проверяем фильтры
	if !shouldNotify(objectName, objectSize) {
		return
	}

	// Определяем топик (папку)
	topic := determineTopic(objectName)

	// Формируем красивое сообщение
	message := createBeautifulMessage(bucketName, objectName, objectSize, eventTime, eventType, topic)

	// Отправляем сообщение во все чаты
	for _, chat := range config.Chats {
		sendToChat(chat.ID, chat.TopicID, topic, chat.Label, message)
	}
}

func determineTopic(objectName string) string {
	parts := strings.Split(objectName, "/")
	if len(parts) > 1 {
		// Берем первую часть пути как топик (папку)
		return parts[0]
	}
	return "root" // Если файл в корне бакета
}

func sendToChat(chatID, defaultTopicID int64, objectTopic, chatLabel, message string) {
	// Определяем ID топика для отправки
	topicID := defaultTopicID
	
	// Если есть соответствие топика в конфиге, используем его
	if config.Topics != nil {
		if topicFromConfig, ok := config.Topics[objectTopic]; ok {
			topicID = topicFromConfig
		}
	}

	enqueued := &queuedMessage{
		ChatID:      chatID,
		TopicID:     topicID,
		ChatLabel:   chatLabel,
		ObjectTopic: objectTopic,
		Text:        message,
		Attempts:    0,
	}

	enqueueMessage(enqueued)
}

func enqueueMessage(msg *queuedMessage) {
	if msg == nil {
		return
	}

	if messageQueue == nil {
		log.Println("⚠️ Очередь сообщений еще не инициализирована")
		return
	}

	messageQueue <- msg
	if logger != nil {
		logger.Printf("📨 Сообщение добавлено в очередь (чат %d, попытка %d/%d, длина очереди %d)", msg.ChatID, msg.Attempts+1, maxSendAttempts, len(messageQueue))
	}
}

func startMessageDispatcher(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			if logger != nil {
				logger.Println("🛑 Очередь уведомлений остановлена")
			}
			return
		case msg, ok := <-messageQueue:
			if !ok {
				if logger != nil {
					logger.Println("🛑 Канал очереди уведомлений закрыт")
				}
				return
			}
			if msg == nil {
				continue
			}

			attemptNumber := msg.Attempts + 1
			if logger != nil {
				logger.Printf("📤 Отправка сообщения в чат %d (%s), попытка %d/%d", msg.ChatID, msg.ChatLabel, attemptNumber, maxSendAttempts)
			}

			err := sendTelegramMessage(msg)
			if err != nil {
				if logger != nil {
					logger.Printf("⚠️ Ошибка отправки сообщения в чат %d: %v", msg.ChatID, err)
				}
				msg.Attempts = attemptNumber
				if msg.Attempts < maxSendAttempts {
					go scheduleRetry(ctx, msg)
				} else if logger != nil {
					logger.Printf("❌ Исчерпаны попытки отправки сообщения в чат %d (%s)", msg.ChatID, msg.ChatLabel)
				}
			} else if logger != nil {
				logger.Printf("✅ Сообщение отправлено в чат %d (%s) с попытки %d", msg.ChatID, msg.ChatLabel, attemptNumber)
			}

			select {
			case <-ctx.Done():
				if logger != nil {
					logger.Println("🛑 Очередь уведомлений остановлена")
				}
				return
			case <-time.After(queueInterval):
			}
		}
	}
}

func sendTelegramMessage(msg *queuedMessage) error {
	telegramMessage := tgbotapi.NewMessage(msg.ChatID, msg.Text)
	telegramMessage.ParseMode = "Markdown"
	telegramMessage.DisableWebPagePreview = true

	if msg.TopicID != 0 {
		telegramMessage.ReplyToMessageID = int(msg.TopicID)
	}

	_, err := bot.Send(telegramMessage)
	return err
}

func scheduleRetry(ctx context.Context, msg *queuedMessage) {
	if logger != nil {
		logger.Printf("⏳ Повторная попытка отправки сообщения в чат %d через %s (попытка %d/%d)", msg.ChatID, retryDelay, msg.Attempts+1, maxSendAttempts)
	}

	select {
	case <-ctx.Done():
		if logger != nil {
			logger.Printf("🛑 Отмена повторной отправки сообщения в чат %d", msg.ChatID)
		}
		return
	case <-time.After(retryDelay):
	}

	enqueueMessage(msg)
}

// isEventEnabled проверяет, включено ли событие в конфигурации
func isEventEnabled(eventName string) bool {
	for _, enabledEvent := range config.Notifications.EnabledEvents {
		if enabledEvent == eventName || strings.HasSuffix(enabledEvent, "*") && strings.HasPrefix(eventName, strings.TrimSuffix(enabledEvent, "*")) {
			return true
		}
	}
	return false
}

// shouldNotify проверяет, нужно ли отправлять уведомление на основе фильтров
func shouldNotify(objectName string, objectSize int64) bool {
	// Проверяем расширения файлов
	for _, ext := range config.Notifications.ExcludeExtensions {
		if strings.HasSuffix(strings.ToLower(objectName), strings.ToLower(ext)) {
			return false
		}
	}
	
	// Проверяем размер файла
	if objectSize < config.Notifications.MinSizeBytes {
		return false
	}
	if config.Notifications.MaxSizeBytes > 0 && objectSize > config.Notifications.MaxSizeBytes {
		return false
	}
	
	return true
}

// createBeautifulMessage создает красиво отформатированное сообщение
func createBeautifulMessage(bucketName, objectName string, objectSize int64, eventTime, eventType, topic string) string {
	// Определяем текст и эмодзи для типа события
	var eventText string
	var eventEmoji string

	switch eventType {
	case "ObjectCreated":
		eventEmoji = "✅"
		eventText = "*Файл добавлен*"
	case "ObjectRemoved":
		eventEmoji = "❌"
		eventText = "*Файл удален*"
	default:
		eventEmoji = "🔔"
		eventText = fmt.Sprintf("*%s*", escapeMarkdown(eventType))
	}
	
	// Форматируем размер файла
	var sizeText string
	if objectSize == 0 {
		sizeText = "0 байт"
	} else if objectSize < 1024 {
		sizeText = fmt.Sprintf("%d байт", objectSize)
	} else if objectSize < 1024*1024 {
		sizeText = fmt.Sprintf("%.1f КБ", float64(objectSize)/1024)
	} else if objectSize < 1024*1024*1024 {
		sizeText = fmt.Sprintf("%.1f МБ", float64(objectSize)/(1024*1024))
	} else {
		sizeText = fmt.Sprintf("%.1f ГБ", float64(objectSize)/(1024*1024*1024))
	}
	
	// Формируем красивое сообщение с топиком
	message := fmt.Sprintf(`%s %s

🏷️ *Топик:* #%s
📦 *Bucket:* %s
📄 *Object:* %s
📏 *Size:* %s
🕒 *Time:* %s
🔔 *Type:* %s`,
		eventEmoji,
		eventText,
		escapeMarkdown(topic),
		escapeMarkdown(bucketName),
		escapeMarkdown(objectName),
		sizeText,
		eventTime,
		escapeMarkdown(eventType))
	
	return message
}

// escapeMarkdown экранирует специальные символы Markdown для Telegram
func escapeMarkdown(text string) string {
	// Список символов, которые нужно экранировать в Telegram Markdown
	charsToEscape := []string{"_", "*", "[", "]", "(", ")", "~", "`", ">", "#", "+", "-", "=", "|", "{", "}", ".", "!"}
	
	for _, char := range charsToEscape {
		text = strings.ReplaceAll(text, char, "\\"+char)
	}
	
	return text
}
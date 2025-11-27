# Этап сборки приложения
FROM golang:1.23-alpine AS builder

# Устанавливаем зависимости для сборки
RUN apk add --no-cache git ca-certificates tzdata

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем файлы зависимостей (для кэширования)
COPY go.mod go.sum ./

# Скачиваем зависимости
RUN go mod download

# Копируем все исходные файлы
COPY . .

# Собираем бинарник (статически линкованный с оптимизацией)
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -a -installsuffix cgo \
    -o minio-telegram-monitor .

# Этап запуска (минимальный образ)
FROM scratch

# Копируем сертификаты и временные зоны из builder
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

# Копируем статически скомпилированный бинарник
COPY --from=builder /app/minio-telegram-monitor /minio-telegram-monitor

# Создаем рабочую директорию
WORKDIR /app

# Копируем конфигурационный файл
COPY config.yaml /app/config.yaml

# Устанавливаем переменные окружения
ENV TZ=UTC

# Открываем порт (если понадобится в будущем)
EXPOSE 8080

# Команда запуска
ENTRYPOINT ["/minio-telegram-monitor"]

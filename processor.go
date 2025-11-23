package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

// Структура для одной записи лога
type LogEntry struct {
	Timestamp    string // время в формате "2024-01-15 10:30:00"
	IP           string // IP адрес клиента
	Method       string // HTTP метод (GET, POST и т.д.)
	URL          string // путь запроса
	StatusCode   int    // HTTP статус код
	ResponseTime int    // время ответа в миллисекундах
}

// Структура для сбора статистики
type Statistics struct {
	TotalRequests   int            // общее количество запросов
	ErrorCount      int            // количество ошибок (статус >= 400)
	RequestsByIP    map[string]int // количество запросов с каждого IP
	AverageRespTime float64        // среднее время ответа
}

// Парсим строку CSV в структуру LogEntry
func parseLogLine(line string, lineNumber int) (LogEntry, error) {
	fields := strings.Split(line, ",")
	// если кол-во полей не равно 6, передаем ошибку
	if len(fields) != 6 {
		return LogEntry{}, fmt.Errorf("неверный формат логов в строке %d: ", lineNumber+1)
	}

	// проверка корректности содержимого поля statusCode
	statusCode, err := strconv.Atoi(fields[4])
	if err != nil {
		return LogEntry{}, fmt.Errorf("неверный код ответа в строке %d: %v", lineNumber+1, err)
	}

	// проверка корректности содержимого поля responseTime
	responseTime, err := strconv.Atoi(fields[5])
	if err != nil {
		return LogEntry{}, fmt.Errorf("неверное время ответа в строке %d: %v", lineNumber+1, err)
	}

	return LogEntry{
		Timestamp:    fields[0],
		IP:           fields[1],
		Method:       fields[2],
		URL:          fields[3],
		StatusCode:   statusCode,
		ResponseTime: responseTime,
	}, nil
}

// Функция readLogs читает файл с логами, построчно парсит строки и отправляет
// полученные записи (LogEntry) в канал для дальнейшей обработки.
// Функция запускает внутреннюю горутину, которая закрывает канал после завершения.
func readLogs(ctx context.Context, filename string) (<-chan LogEntry, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	// проверяем открылся ли файл
	info, err := file.Stat()
	if err != nil {
		fmt.Println("Ошибка получения информации о файле:", err)
	}
	fmt.Println("Имя файла:", info.Name())

	// Создаем выходной канал для передачи обработанных записей лога
	out := make(chan LogEntry)

	// Запускаем горутину, которая будет читать и парсить файл
	go func() {
		defer close(out)   // закрываем канал когда горутина завершится
		defer file.Close() // закрываем файл когда горутина завершится

		// Создаем сканер для построчного чтения файла
		scanner := bufio.NewScanner(file)

		// Счетчик номера текущей строки в файле (для диагностики ошибок)
		lineNumber := 0

		// Считываем первую строку - заголовок CSV - пропускаем ее
		if !scanner.Scan() {
			log.Printf("Не удалось считать заголовок или файл пуст")
			if err := scanner.Err(); err != nil {
				log.Fatalf("Ошибка сканера: %v", err)
			}
			return
		}

		// Цикл по остальным строкам файла
		for scanner.Scan() {
			// Увеличиваем номер строки
			lineNumber++
			// Проверяем, не отменен ли контекст — если да, завершаем работу
			select {
			case <-ctx.Done():
				fmt.Printf("Контекст отменен\n")
				return
			default:
				// Получаем текст текущей строки
				line := scanner.Text()

				// Парсим строку, передавая её номер для более информативной ошибки
				logEntry, err := parseLogLine(line, lineNumber)

				// При ошибке парсинга выводим сообщение в лог, строку пропускаем
				if err != nil {
					log.Printf("ошибка при парсинге логов строка %d: %v", lineNumber+1, err)
					continue // при ошибке парсинга пропускаем строку
				}

				// Отправляем успешно разобранную запись в канал для дальнейшей обработки
				out <- logEntry
			}
		}
	}()

	// Возвращаем канал, из которого можно читать лог-записи
	return out, nil
}

// Обработка логов с использованием worker pool
// параллельно обрабатываем записи из канала input, возвращаем канал с результатами
func processLogs(ctx context.Context, input <-chan LogEntry, numWorkers int) <-chan LogEntry {
	out := make(chan LogEntry)
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		for logEntry := range input {
			select {
			case <-ctx.Done():
				return
			default:
				out <- logEntry
			}
		}
	}

	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go worker()
	}

	// Закрываем канал после завершения всех воркеров
	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// Функция разветвления каналов для filtered и unfiltered данных с использованием буферизованных каналов
func tee(in <-chan LogEntry, bufferSize int) (<-chan LogEntry, <-chan LogEntry) {
	out1 := make(chan LogEntry, bufferSize)
	out2 := make(chan LogEntry, bufferSize)
	go func() {
		defer close(out1)
		defer close(out2)
		for v := range in {
			out1 <- v
			out2 <- v
		}
	}()
	return out1, out2
}

// Фильтрация логов: пропускаем только записи с statusCode >= minStatus
func filterLogs(input <-chan LogEntry, minStatus int) <-chan LogEntry {
	out := make(chan LogEntry)

	go func() {
		defer close(out)
		for logEntry := range input {
			if logEntry.StatusCode >= minStatus {
				out <- logEntry
			}
		}
	}()

	return out
}

// Подсчет статистики по логам из канала input
func calculateStats(input <-chan LogEntry) Statistics {
	stats := Statistics{
		RequestsByIP: make(map[string]int),
	}
	totalRespTime := 0

	for logEntry := range input {
		stats.TotalRequests++
		if logEntry.StatusCode >= 400 {
			stats.ErrorCount++
		}
		stats.RequestsByIP[logEntry.IP]++
		totalRespTime += logEntry.ResponseTime
	}

	if stats.TotalRequests > 0 {
		stats.AverageRespTime = float64(totalRespTime) / float64(stats.TotalRequests)
	}

	return stats
}

// Вывод топ-N IP адресов по количеству запросов
func printTopIPs(requestsByIP map[string]int, n int) {
	type ipCount struct {
		ip    string
		count int
	}

	var ipCounts []ipCount
	for ip, count := range requestsByIP {
		ipCounts = append(ipCounts, ipCount{ip, count})
	}

	// Сортируем по убыванию количества запросов
	for i := 0; i < len(ipCounts); i++ {
		for j := i + 1; j < len(ipCounts); j++ {
			if ipCounts[j].count > ipCounts[i].count {
				ipCounts[j], ipCounts[i] = ipCounts[i], ipCounts[j]
			}
		}
	}

	limit := n
	if len(ipCounts) < n {
		limit = len(ipCounts)
	}

	fmt.Printf("Топ %d IP адресов:\n", limit)
	for i := 0; i < limit; i++ {
		fmt.Printf("%s: %d запросов\n", ipCounts[i].ip, ipCounts[i].count)
	}
}

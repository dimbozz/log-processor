package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
)

// Главная функция – точка входа в программу
func main() {
	// Создаем контекст с возможностью отмены
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Проверяем аргументы командной строки: ожидаем имя файла с логами
	if len(os.Args) < 2 {
		fmt.Println("Запуск: go run main.go <logfile.csv>")
		return
	}

	// Получаем путь к файлу из аргументов
	inputFile := os.Args[1]

	// Читаем логи из файла (функция из processor.go)
	logChan, err := readLogs(ctx, inputFile)
	if err != nil {
		log.Fatalf("ошибка чтения логов: %v", err)
	}

	// Параллельно обрабатываем логи с пулом из 3 воркеров, результат — канал с обработанными логами
	processedChan := processLogs(ctx, logChan, 3)

	//Формируем filtered и unfiltered буферизованные каналы для предотвращения блокировок при параллельном чтении данных
	unfilteredChan, filteredChan := tee(processedChan, 100)

	// Создаем WaitGroup, чтобы дождаться завершения обеих горутин подсчета статистики
	var wg sync.WaitGroup
	wg.Add(2)

	// Переменные для хранения результатов статистики
	var stats Statistics
	var filteredStats Statistics

	// Подсчет статистики по всем логам запускается в отдельной горутине
	go func() {
		defer wg.Done()
		stats = calculateStats(unfilteredChan)
	}()

	// Фильтруем логи — выбираем только с кодом >= 400 (ошибки)
	// Подсчитываем статистику по отфильтрованным логам в другой горутине
	go func() {
		defer wg.Done()
		filteredStats = calculateStats(filterLogs(filteredChan, 400)) // Фильтруем и считаем ошибки
	}()

	// Ждем, пока обе горутины завершатся
	wg.Wait()

	// Выводим результаты подсчёта
	fmt.Printf("Всего запросов: %d\n", stats.TotalRequests)
	fmt.Printf("Всего ошибок (4xx and 5xx): %d\n", filteredStats.ErrorCount)
	fmt.Printf("Среднее время ответа: %.2f ms\n", stats.AverageRespTime)

	// Выводим топ IP адресов по количеству запросов
	// В данном случае Топ 5
	printTopIPs(stats.RequestsByIP, 5)
}

package main

import (
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	"reniec/codigo"

	_ "github.com/lib/pq"
)

const NumWorkers = 15

type Resultado struct {
	DNI    string
	Codigo string
	Error  error
}

func main() {
	dbConfig := codigo.DBConfig{
		Host:     "localhost",
		Port:     5433,
		User:     "postgres",
		Password: "admin123",
		DBName:   "personas",
	}

	db, err := codigo.ConectarDB(dbConfig)
	if err != nil {
		log.Fatalf("Error conectando a la base de datos: %v", err)
	}
	defer db.Close()

	fmt.Printf("🚀 Iniciando procesamiento con %d workers...\n", NumWorkers)
	startTime := time.Now()

	err = procesarDNIs(db)
	if err != nil {
		log.Fatalf("Error procesando DNIs: %v", err)
	}

	fmt.Printf("🎉 Proceso completado en %v\n", time.Since(startTime))
}

func procesarDNIs(db *sql.DB) error {
	dnis, err := codigo.ObtenerDNIsPendientes(db)
	if err != nil {
		return err
	}

	total := len(dnis)
	if total == 0 {
		fmt.Println("No hay DNIs pendientes")
		return nil
	}

	fmt.Printf("📋 Procesando %d DNIs\n", total)

	dniChan := make(chan string, total)
	resultadoChan := make(chan Resultado, total)

	var wg sync.WaitGroup
	var exitosos, errores int
	var mu sync.Mutex

	// Llenar canal
	for _, dni := range dnis {
		dniChan <- dni
	}
	close(dniChan)

	// Iniciar workers
	for i := 0; i < NumWorkers; i++ {
		wg.Add(1)
		go worker(dniChan, resultadoChan, &wg)
	}

	// Procesar resultados
	go func() {
		for resultado := range resultadoChan {
			mu.Lock()
			if resultado.Error != nil {
				errores++
				fmt.Printf("❌ Error DNI %s: %v\n", resultado.DNI, resultado.Error)
			} else {
				err := codigo.ActualizarCodigoVerificacion(db, resultado.DNI, resultado.Codigo)
				if err != nil {
					errores++
					fmt.Printf("❌ Error BD DNI %s: %v\n", resultado.DNI, err)
				} else {
					exitosos++
					fmt.Printf("✅ DNI %s: %s\n", resultado.DNI, resultado.Codigo)
				}
			}
			mu.Unlock()
		}
	}()

	wg.Wait()
	close(resultadoChan)

	// Esperar resultados
	time.Sleep(2 * time.Second)

	fmt.Printf("\n📊 Resultados: %d exitosos, %d errores de %d total\n", exitosos, errores, total)
	return nil
}

func worker(dniChan <-chan string, resultadoChan chan<- Resultado, wg *sync.WaitGroup) {
	defer wg.Done()

	for dni := range dniChan {
		codigo, err := procesarDNI(dni)
		resultadoChan <- Resultado{DNI: dni, Codigo: codigo, Error: err}
		time.Sleep(1 * time.Second)
	}
}

func procesarDNI(dni string) (string, error) {
	maxReintentos := 2
	var lastError error

	for intento := 1; intento <= maxReintentos; intento++ {
		codigo, err := codigo.ObtenerCodigoVerificacion(dni)
		if err == nil {
			return codigo, nil
		}
		lastError = err
		if intento < maxReintentos {
			time.Sleep(time.Duration(intento*2) * time.Second)
		}
	}

	return "", lastError
}

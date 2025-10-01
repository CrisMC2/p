package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"net/http/cookiejar"
	"sync"
	"time"

	"reniec/codigo"

	_ "github.com/lib/pq"
)

const NumWorkers = 2

type Resultado struct {
	DNI   string
	Datos *codigo.DatosPersona
	Error error
}

// Estructura para mantener el estado del worker
type WorkerState struct {
	ID           int
	Client       *http.Client
	Token        string
	ConsultCount int
	TargetURL    string
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

	fmt.Printf("🚀 Iniciando procesamiento con %d workers con delays escalonados...\n", NumWorkers)
	startTime := time.Now()

	err = procesarDatosIncompletos(db)
	if err != nil {
		log.Fatalf("Error procesando datos: %v", err)
	}

	fmt.Printf("🎉 Proceso completado en %v\n", time.Since(startTime))
}

func procesarDatosIncompletos(db *sql.DB) error {
	dnis, err := codigo.ObtenerDNIsIncompletos(db)
	if err != nil {
		return err
	}

	total := len(dnis)
	if total == 0 {
		fmt.Println("✨ No hay DNIs con datos incompletos")
		return nil
	}

	fmt.Printf("📋 Procesando %d DNIs con datos incompletos\n", total)

	dniChan := make(chan string, total)
	resultadoChan := make(chan Resultado, total)
	var wg sync.WaitGroup
	var exitosos, errores int
	var mu sync.Mutex

	// Llenar canal con DNIs pendientes
	for _, dni := range dnis {
		dniChan <- dni
	}
	close(dniChan)

	// Iniciar workers con tokens individuales
	for i := 0; i < NumWorkers; i++ {
		wg.Add(1)
		go workerConToken(i+1, dniChan, resultadoChan, &wg)
	}

	// Procesar resultados en goroutine separada
	var processingWg sync.WaitGroup
	processingWg.Add(1)
	go func() {
		defer processingWg.Done()
		for resultado := range resultadoChan {
			mu.Lock()
			if resultado.Error != nil {
				errores++
				fmt.Printf("❌ Error DNI %s: %v\n", resultado.DNI, resultado.Error)
			} else {
				err := codigo.ActualizarDatosPersona(db, resultado.Datos)
				if err != nil {
					errores++
					fmt.Printf("❌ Error BD DNI %s: %v\n", resultado.DNI, err)
				} else {
					exitosos++
					fmt.Printf("✅ Worker DNI %s: %s %s %s\n",
						resultado.DNI,
						resultado.Datos.Nombres,
						resultado.Datos.ApellidoPaterno,
						resultado.Datos.ApellidoMaterno)
				}
			}
			mu.Unlock()
		}
	}()

	// Esperar a que terminen todos los workers
	wg.Wait()
	close(resultadoChan)

	// Esperar a que termine el procesamiento de resultados
	processingWg.Wait()

	fmt.Printf("\n📊 Resultados finales: %d exitosos, %d errores de %d total\n", exitosos, errores, total)
	return nil
}

func workerConToken(workerID int, dniChan <-chan string, resultadoChan chan<- Resultado, wg *sync.WaitGroup) {
	defer wg.Done()

	// Crear estado único para este worker
	state := &WorkerState{
		ID:           workerID,
		ConsultCount: 0,
		TargetURL:    "https://eldni.com/pe/buscar-datos-por-dni",
	}

	// Crear cliente HTTP único con jar de cookies propio
	jar, _ := cookiejar.New(nil)
	state.Client = &http.Client{
		Timeout: 30 * time.Second,
		Jar:     jar,
	}

	// Obtener token inicial
	fmt.Printf("🔑 Worker %d obteniendo token inicial...\n", workerID)
	if err := renovarToken(state); err != nil {
		fmt.Printf("❌ Worker %d falló al obtener token inicial: %v\n", workerID, err)
		return
	}

	for dni := range dniChan {
		// Renovar token cada 3 consultas
		if state.ConsultCount%3 == 0 && state.ConsultCount > 0 {
			fmt.Printf("🔄 Worker %d renovando token (consulta #%d)...\n", workerID, state.ConsultCount)
			if err := renovarToken(state); err != nil {
				fmt.Printf("⚠️ Worker %d falló al renovar token: %v\n", workerID, err)
				// Continuar con el token actual si falla la renovación
			}
		}

		datos, err := procesarDNIConToken(dni, state)
		resultadoChan <- Resultado{DNI: dni, Datos: datos, Error: err}

		state.ConsultCount++

		// Rate limiting más agresivo para evitar bloqueo IP
		sleepTime := time.Duration(5+workerID*2) * time.Second
		time.Sleep(sleepTime)
	}

	fmt.Printf("✅ Worker %d completado - %d consultas realizadas\n", workerID, state.ConsultCount)
}

func renovarToken(state *WorkerState) error {
	req, err := http.NewRequest("GET", state.TargetURL, nil)
	if err != nil {
		return err
	}
	codigo.SetHeaders(req)

	resp, err := state.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("error del servidor: %d", resp.StatusCode)
	}

	body, err := codigo.LeerRespuesta(resp)
	if err != nil {
		return err
	}

	token, err := codigo.ExtraerToken(string(body))
	if err != nil {
		return err
	}

	state.Token = token
	fmt.Printf("🔑 Worker %d token renovado exitosamente\n", state.ID)
	return nil
}

func procesarDNIConToken(dni string, state *WorkerState) (*codigo.DatosPersona, error) {
	maxReintentos := 2 // Reducir reintentos
	var lastError error

	for intento := 1; intento <= maxReintentos; intento++ {
		datos, err := consultarDNIConToken(dni, state)
		if err == nil {
			return datos, nil
		}

		lastError = err

		// Si es error 429, esperar más tiempo antes del siguiente intento
		if isRateLimitError(err) {
			waitTime := time.Duration(30+intento*30) * time.Second
			fmt.Printf("⏳ Worker %d esperando %v por rate limit...\n", state.ID, waitTime)
			time.Sleep(waitTime)

			// Renovar token después de esperar
			if renewErr := renovarToken(state); renewErr != nil {
				fmt.Printf("⚠️ Worker %d falló al renovar token: %v\n", state.ID, renewErr)
			}
		}

		fmt.Printf("⚠️ Worker %d - Reintento %d/%d para DNI %s: %v\n", state.ID, intento, maxReintentos, dni, err)

		if intento < maxReintentos {
			// Backoff exponencial más largo: 10s, 20s
			backoff := time.Duration(intento*10) * time.Second
			time.Sleep(backoff)
		}
	}

	return nil, fmt.Errorf("worker %d agotó %d reintentos: %v", state.ID, maxReintentos, lastError)
}

func consultarDNIConToken(dni string, state *WorkerState) (*codigo.DatosPersona, error) {
	if state.Token == "" {
		return nil, fmt.Errorf("token no disponible")
	}

	return codigo.EnviarFormularioConToken(state.Client, state.TargetURL, dni, state.Token)
}

func isRateLimitError(err error) bool {
	if err == nil {
		return false
	}
	return contains(err.Error(), "429")
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s == substr || len(s) > len(substr) &&
			(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
				indexSubstring(s, substr) != -1))
}

func indexSubstring(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

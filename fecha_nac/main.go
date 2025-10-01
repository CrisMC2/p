package main

import (
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	_ "github.com/lib/pq"
)

type DBConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
}

type DNIData struct {
	DNI             string `json:"dni"`
	Nombres         string `json:"nombres"`
	FechaNacimiento string `json:"fecha_nacimiento"`
}

type DNIScraper struct {
	client           *http.Client
	nonce            string
	lastRequest      time.Time
	requestCount     int
	userAgents       []string
	db               *sql.DB
	minuteStart      time.Time
	requestsInMinute int
}

func NewDNIScraper(dbConfig DBConfig) (*DNIScraper, error) {
	jar, _ := cookiejar.New(nil)

	userAgents := []string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	}

	// Conectar a la base de datos
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		dbConfig.Host, dbConfig.Port, dbConfig.User, dbConfig.Password, dbConfig.DBName)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("error conectando a la BD: %v", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("error verificando conexión BD: %v", err)
	}

	fmt.Println("✅ Conexión a base de datos exitosa")

	return &DNIScraper{
		client: &http.Client{
			Jar:     jar,
			Timeout: 45 * time.Second,
		},
		userAgents:       userAgents,
		db:               db,
		minuteStart:      time.Now(),
		requestsInMinute: 0,
	}, nil
}

func (ds *DNIScraper) Close() {
	if ds.db != nil {
		ds.db.Close()
	}
}

// Convertir fecha de dd/mm/aaaa a aaaa-mm-dd
func convertirFecha(fechaDDMMAAAA string) (string, error) {
	parts := strings.Split(fechaDDMMAAAA, "/")
	if len(parts) != 3 {
		return "", fmt.Errorf("formato de fecha inválido")
	}
	return fmt.Sprintf("%s-%s-%s", parts[2], parts[1], parts[0]), nil
}

// Actualizar fecha de nacimiento en la BD
func (ds *DNIScraper) ActualizarFechaBD(dni, fechaDDMMAAAA string) error {
	fechaSQL, err := convertirFecha(fechaDDMMAAAA)
	if err != nil {
		return err
	}

	query := `UPDATE personas SET fecha_nacimiento = $1 WHERE dni = $2 AND fecha_nacimiento IS NULL`
	result, err := ds.db.Exec(query, fechaSQL, dni)
	if err != nil {
		return err
	}

	rows, _ := result.RowsAffected()
	if rows > 0 {
		fmt.Printf("✅ BD actualizada para DNI %s: %s\n", dni, fechaSQL)
	} else {
		fmt.Printf("ℹ️  DNI %s ya tiene fecha o no existe\n", dni)
	}

	return nil
}

func (ds *DNIScraper) getRandomUserAgent() string {
	n, _ := rand.Int(rand.Reader, big.NewInt(int64(len(ds.userAgents))))
	return ds.userAgents[n.Int64()]
}

func (ds *DNIScraper) resetSession() error {
	fmt.Println("🔄 Reseteando sesión...")

	jar, _ := cookiejar.New(nil)
	ds.client.Jar = jar
	ds.nonce = ""
	ds.requestCount = 0

	time.Sleep(3 * time.Second)

	return ds.getNonce()
}

func (ds *DNIScraper) getNonce() error {
	req, _ := http.NewRequest("GET", "https://dniperu.com/fecha-de-nacimiento-con-dni/", nil)
	req.Header.Set("User-Agent", ds.getRandomUserAgent())
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Language", "es-ES,es;q=0.9,en;q=0.8")
	req.Header.Set("Cache-Control", "no-cache")

	resp, err := ds.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	doc, _ := goquery.NewDocumentFromReader(resp.Body)

	doc.Find("script").Each(func(i int, s *goquery.Selection) {
		if strings.Contains(s.Text(), "fecha_vars") && ds.nonce == "" {
			re := regexp.MustCompile(`fecha_vars\s*=\s*\{[^}]*nonce['"]?\s*:\s*['"]([^'"]+)['"]`)
			if matches := re.FindStringSubmatch(s.Text()); len(matches) > 1 {
				ds.nonce = matches[1]
			}
		}
	})

	if ds.nonce == "" {
		return fmt.Errorf("no se pudo obtener el nonce")
	}

	fmt.Printf("✅ Nuevo nonce obtenido: %s\n", ds.nonce)
	return nil
}

func (ds *DNIScraper) ConsultarDNI(dni string) (*DNIData, error) {
	if len(dni) != 8 || !regexp.MustCompile(`^\d{8}$`).MatchString(dni) {
		return nil, fmt.Errorf("DNI debe tener 8 dígitos")
	}

	// Control de 5 requests por minuto
	now := time.Now()
	if now.Sub(ds.minuteStart) >= 60*time.Second {
		// Nuevo minuto
		ds.minuteStart = now
		ds.requestsInMinute = 0
		fmt.Println("🔄 Nuevo ciclo de minuto iniciado")
	}

	// Si ya se hicieron 5 requests en este minuto, esperar al siguiente
	if ds.requestsInMinute >= 5 {
		waitTime := 61*time.Second - now.Sub(ds.minuteStart) + time.Second
		fmt.Printf("⏳ Esperando %v para el siguiente ciclo...\n", waitTime)
		time.Sleep(waitTime)
		ds.minuteStart = time.Now()
		ds.requestsInMinute = 0
	}

	// Delay de 12 segundos entre requests (excepto el primero)
	if ds.requestsInMinute > 0 {
		fmt.Printf("⏳ Esperando 12 segundos antes de la siguiente consulta...\n")
		time.Sleep(12 * time.Second)
	}

	ds.requestCount++
	if ds.requestCount > 4 {
		if err := ds.resetSession(); err != nil {
			return nil, err
		}
	}

	if ds.nonce == "" {
		if err := ds.getNonce(); err != nil {
			return nil, err
		}
	}

	data := url.Values{}
	data.Set("dni", dni)
	data.Set("action", "buscar_fecha")
	data.Set("security", ds.nonce)
	data.Set("company", "")

	req, _ := http.NewRequest("POST", "https://dniperu.com/wp-admin/admin-ajax.php", strings.NewReader(data.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	req.Header.Set("User-Agent", ds.getRandomUserAgent())
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("Referer", "https://dniperu.com/fecha-de-nacimiento-con-dni/")
	req.Header.Set("Accept", "application/json, text/javascript, */*; q=0.01")
	req.Header.Set("Accept-Language", "es-ES,es;q=0.9,en;q=0.8")

	fmt.Printf("🔍 Consultando DNI: %s (Request %d/5 del minuto)\n", dni, ds.requestsInMinute+1)
	ds.lastRequest = time.Now()
	ds.requestsInMinute++

	resp, err := ds.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	respStr := strings.TrimSpace(string(body))

	switch respStr {
	case "-1":
		return nil, fmt.Errorf("acceso denegado")
	case "0":
		return nil, fmt.Errorf("DNI no encontrado")
	}

	var response struct {
		Success bool `json:"success"`
		Data    struct {
			DNI             string `json:"dni"`
			Nombres         string `json:"nombres"`
			FechaNacimiento string `json:"fechaNacimiento"`
			Message         string `json:"message"`
		} `json:"data"`
	}

	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("error parseando respuesta")
	}

	if !response.Success {
		if strings.Contains(response.Data.Message, "demasiadas solicitudes") {
			fmt.Println("⚠️  Rate limit detectado, esperando 10 segundos...")
			time.Sleep(10 * time.Second)
			ds.minuteStart = time.Now()
			ds.requestsInMinute = 0

			if err := ds.resetSession(); err != nil {
				return nil, err
			}

			fmt.Println("🔄 Reintentando consulta...")
			return ds.ConsultarDNI(dni)
		}
		return nil, fmt.Errorf("consulta no exitosa: %s", response.Data.Message)
	}

	dniData := &DNIData{
		DNI:             dni,
		Nombres:         response.Data.Nombres,
		FechaNacimiento: response.Data.FechaNacimiento,
	}

	// Actualizar BD automáticamente
	if err := ds.ActualizarFechaBD(dni, dniData.FechaNacimiento); err != nil {
		fmt.Printf("⚠️  Error actualizando BD: %v\n", err)
	}

	return dniData, nil
}

func (ds *DNIScraper) ConsultarMultiplesDNIs(dnis []string) {
	fmt.Printf("📋 Iniciando consulta de %d DNIs...\n\n", len(dnis))

	for i, dni := range dnis {
		fmt.Printf("=== Consulta %d/%d ===\n", i+1, len(dnis))

		data, err := ds.ConsultarDNI(dni)
		if err != nil {
			fmt.Printf("❌ Error con DNI %s: %v\n\n", dni, err)
			continue
		}

		fmt.Printf("✅ DNI: %s\n   Nombres: %s\n   Fecha: %s\n\n",
			data.DNI, data.Nombres, data.FechaNacimiento)
	}
}

// Obtener DNIs sin fecha de nacimiento desde la BD
func (ds *DNIScraper) ObtenerDNIsSinFecha() ([]string, error) {
	query := `SELECT dni FROM personas WHERE fecha_nacimiento IS NULL ORDER BY id`
	rows, err := ds.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dnis []string
	for rows.Next() {
		var dni string
		if err := rows.Scan(&dni); err != nil {
			continue
		}
		dnis = append(dnis, dni)
	}

	return dnis, nil
}

func main() {
	dbConfig := DBConfig{
		Host:     "localhost",
		Port:     5433,
		User:     "postgres",
		Password: "admin123",
		DBName:   "personas",
	}

	scraper, err := NewDNIScraper(dbConfig)
	if err != nil {
		log.Fatalf("Error inicializando scraper: %v", err)
	}
	defer scraper.Close()

	// Obtener DNIs sin fecha de nacimiento de la BD
	dnisSinFecha, err := scraper.ObtenerDNIsSinFecha()
	if err != nil {
		log.Fatalf("Error obteniendo DNIs: %v", err)
	}

	fmt.Printf("📊 Se encontraron %d DNIs sin fecha de nacimiento\n\n", len(dnisSinFecha))

	if len(dnisSinFecha) == 0 {
		fmt.Println("✅ Todos los DNIs ya tienen fecha de nacimiento")
		return
	}

	// Procesar todos los DNIs sin fecha
	scraper.ConsultarMultiplesDNIs(dnisSinFecha)

	fmt.Println("\n🎉 Proceso completado!")
}

package codigo

import (
	"compress/gzip"
	"database/sql"
	"fmt"
	"io"
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

type DatosPersona struct {
	DNI             string
	Nombres         string
	ApellidoPaterno string
	ApellidoMaterno string
	CodigoVerif     string
}

func ConectarDB(config DBConfig) (*sql.DB, error) {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		config.Host, config.Port, config.User, config.Password, config.DBName)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, err
	}

	fmt.Println("✅ Conectado a PostgreSQL")
	return db, nil
}

func ObtenerDNIsPendientes(db *sql.DB) ([]string, error) {
	query := `
		SELECT dni FROM personas 
		WHERE codigo_verificador IS NULL
		ORDER BY id`

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dnis []string
	for rows.Next() {
		var dni string
		if err := rows.Scan(&dni); err != nil {
			return nil, err
		}
		dnis = append(dnis, dni)
	}
	return dnis, nil
}

func ObtenerDNIsIncompletos(db *sql.DB) ([]string, error) {
	query := `
		SELECT dni FROM personas 
		WHERE nombres IS NULL OR nombres = '' 
		   OR apellido_paterno IS NULL OR apellido_paterno = ''
		   OR apellido_materno IS NULL OR apellido_materno = ''
		ORDER BY id`

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dnis []string
	for rows.Next() {
		var dni string
		if err := rows.Scan(&dni); err != nil {
			return nil, err
		}
		dnis = append(dnis, dni)
	}
	return dnis, nil
}

func ActualizarCodigoVerificacion(db *sql.DB, dni, codigo string) error {
	_, err := db.Exec("UPDATE personas SET codigo_verificador = $1 WHERE dni = $2", codigo, dni)
	return err
}

func ActualizarDatosPersona(db *sql.DB, datos *DatosPersona) error {
	query := `
		UPDATE personas 
		SET nombres = $1, apellido_paterno = $2, apellido_materno = $3
		WHERE dni = $4`

	_, err := db.Exec(query, datos.Nombres, datos.ApellidoPaterno, datos.ApellidoMaterno, datos.DNI)
	return err
}

// FUNCIÓN PRINCIPAL MEJORADA - Usando el método robusto de scraping
func ObtenerDatosPersona(dni string) (*DatosPersona, error) {
	if !regexp.MustCompile(`^[0-9]{8}$`).MatchString(dni) {
		return nil, fmt.Errorf("DNI inválido: debe tener 8 dígitos")
	}

	jar, _ := cookiejar.New(nil)
	client := &http.Client{Timeout: 30 * time.Second, Jar: jar}
	targetURL := "https://eldni.com/pe/buscar-datos-por-dni"

	// Paso 1: Obtener el token CSRF
	token, err := obtenerToken(client, targetURL)
	if err != nil {
		return nil, fmt.Errorf("error obteniendo token: %v", err)
	}

	// Paso 2: Enviar formulario y obtener datos
	return enviarFormularioDatos(client, targetURL, dni, token)
}

func obtenerToken(client *http.Client, targetURL string) (string, error) {
	req, err := http.NewRequest("GET", targetURL, nil)
	if err != nil {
		return "", err
	}
	setHeaders(req)

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("error del servidor: %d", resp.StatusCode)
	}

	body, err := leerRespuesta(resp)
	if err != nil {
		return "", err
	}

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(string(body)))
	if err != nil {
		return "", err
	}

	token, exists := doc.Find("input[name='_token']").Attr("value")
	if !exists {
		return "", fmt.Errorf("token CSRF no encontrado")
	}

	return token, nil
}

func enviarFormularioDatos(client *http.Client, targetURL, dni, token string) (*DatosPersona, error) {
	data := url.Values{
		"_token": {token},
		"dni":    {dni},
	}

	req, err := http.NewRequest("POST", targetURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Referer", targetURL)
	setHeaders(req)

	time.Sleep(1 * time.Second) // Rate limiting

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("error del servidor: %d", resp.StatusCode)
	}

	body, err := leerRespuesta(resp)
	if err != nil {
		return nil, err
	}

	return extraerDatosPersona(string(body), dni)
}

// FUNCIÓN MEJORADA - Extracción robusta con múltiples métodos
func extraerDatosPersona(html, dni string) (*DatosPersona, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return nil, err
	}

	datos := &DatosPersona{DNI: dni}

	// Método principal: extraer de los inputs de copia
	if nombres := strings.TrimSpace(doc.Find("#nombres").AttrOr("value", "")); nombres != "" {
		datos.Nombres = nombres
	}
	if apellidoP := strings.TrimSpace(doc.Find("#apellidop").AttrOr("value", "")); apellidoP != "" {
		datos.ApellidoPaterno = apellidoP
	}
	if apellidoM := strings.TrimSpace(doc.Find("#apellidom").AttrOr("value", "")); apellidoM != "" {
		datos.ApellidoMaterno = apellidoM
	}

	// Método alternativo: extraer de la tabla
	if datos.Nombres == "" || datos.ApellidoPaterno == "" || datos.ApellidoMaterno == "" {
		doc.Find("table tbody tr td").Each(func(i int, s *goquery.Selection) {
			text := strings.TrimSpace(s.Text())
			switch i {
			case 1: // Segunda columna: Nombres
				if datos.Nombres == "" {
					datos.Nombres = text
				}
			case 2: // Tercera columna: Apellido Paterno
				if datos.ApellidoPaterno == "" {
					datos.ApellidoPaterno = text
				}
			case 3: // Cuarta columna: Apellido Materno
				if datos.ApellidoMaterno == "" {
					datos.ApellidoMaterno = text
				}
			}
		})
	}

	// Método adicional: Buscar por clases CSS comunes
	if datos.Nombres == "" {
		nombres := strings.TrimSpace(doc.Find(".nombres, .nombre, [data-nombres]").First().Text())
		if nombres != "" {
			datos.Nombres = nombres
		}
	}

	if datos.ApellidoPaterno == "" {
		apellidoP := strings.TrimSpace(doc.Find(".apellido-paterno, .paterno, [data-paterno]").First().Text())
		if apellidoP != "" {
			datos.ApellidoPaterno = apellidoP
		}
	}

	if datos.ApellidoMaterno == "" {
		apellidoM := strings.TrimSpace(doc.Find(".apellido-materno, .materno, [data-materno]").First().Text())
		if apellidoM != "" {
			datos.ApellidoMaterno = apellidoM
		}
	}

	// Método regex como último recurso
	if datos.Nombres == "" || datos.ApellidoPaterno == "" || datos.ApellidoMaterno == "" {
		extractDataWithRegex(html, datos)
	}

	// No calcular código de verificación - solo obtener datos personales
	datos.CodigoVerif = ""

	// Verificar si encontramos datos
	if datos.Nombres == "" && datos.ApellidoPaterno == "" && datos.ApellidoMaterno == "" {
		return nil, fmt.Errorf("no se encontraron datos para el DNI %s", dni)
	}

	return datos, nil
}

// Función auxiliar para extracción con regex
func extractDataWithRegex(html string, datos *DatosPersona) {
	// Patrones regex para encontrar datos en el HTML
	patterns := map[string]*string{
		`(?i)nombres?\s*:?\s*([A-ZÁÉÍÓÚÑ\s]{2,50})`:           &datos.Nombres,
		`(?i)apellido\s*paterno\s*:?\s*([A-ZÁÉÍÓÚÑ\s]{2,30})`: &datos.ApellidoPaterno,
		`(?i)apellido\s*materno\s*:?\s*([A-ZÁÉÍÓÚÑ\s]{2,30})`: &datos.ApellidoMaterno,
	}

	for pattern, field := range patterns {
		if *field == "" { // Solo buscar si no se encontró antes
			re := regexp.MustCompile(pattern)
			if matches := re.FindStringSubmatch(html); len(matches) > 1 {
				*field = strings.TrimSpace(matches[1])
			}
		}
	}
}

// HEADERS MEJORADOS - Simulando navegador real más convincentemente
func setHeaders(req *http.Request) {
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8")
	req.Header.Set("Accept-Language", "es-PE,es;q=0.9,en;q=0.8")
	req.Header.Set("Accept-Encoding", "gzip, deflate")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Upgrade-Insecure-Requests", "1")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Pragma", "no-cache")
	req.Header.Set("Sec-Fetch-Dest", "document")
	req.Header.Set("Sec-Fetch-Mode", "navigate")
	req.Header.Set("Sec-Fetch-Site", "none")
	req.Header.Set("Sec-Fetch-User", "?1")
}

func leerRespuesta(resp *http.Response) ([]byte, error) {
	var reader io.Reader = resp.Body

	if resp.Header.Get("Content-Encoding") == "gzip" {
		gzipReader, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, err
		}
		defer gzipReader.Close()
		reader = gzipReader
	}

	return io.ReadAll(reader)
}

// FUNCIONES EXPORTADAS PARA EL SISTEMA DE TOKENS ROTATIVOS

// LeerRespuesta - versión exportada
func LeerRespuesta(resp *http.Response) ([]byte, error) {
	return leerRespuesta(resp)
}

// SetHeaders - versión exportada
func SetHeaders(req *http.Request) {
	setHeaders(req)
}

// ExtraerToken - extraer token desde HTML
func ExtraerToken(html string) (string, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return "", err
	}

	token, exists := doc.Find("input[name='_token']").Attr("value")
	if !exists {
		return "", fmt.Errorf("token CSRF no encontrado")
	}

	return token, nil
}

// EnviarFormularioConToken - enviar formulario con token específico
func EnviarFormularioConToken(client *http.Client, targetURL, dni, token string) (*DatosPersona, error) {
	data := url.Values{
		"_token": {token},
		"dni":    {dni},
	}

	req, err := http.NewRequest("POST", targetURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Referer", targetURL)
	setHeaders(req)

	time.Sleep(1 * time.Second) // Rate limiting

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("error del servidor: %d", resp.StatusCode)
	}

	body, err := leerRespuesta(resp)
	if err != nil {
		return nil, err
	}

	return extraerDatosPersona(string(body), dni)
}

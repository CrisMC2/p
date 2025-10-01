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

func ActualizarCodigoVerificacion(db *sql.DB, dni, codigo string) error {
	_, err := db.Exec("UPDATE personas SET codigo_verificador = $1 WHERE dni = $2", codigo, dni)
	return err
}

func ObtenerCodigoVerificacion(dni string) (string, error) {
	if !regexp.MustCompile(`^[0-9]{8}$`).MatchString(dni) {
		return "", fmt.Errorf("DNI inválido")
	}

	jar, _ := cookiejar.New(nil)
	client := &http.Client{Timeout: 30 * time.Second, Jar: jar}
	targetURL := "https://eldni.com/pe/obtener-digito-verificador-del-dni"

	token, err := obtenerToken(client, targetURL)
	if err != nil {
		return "", err
	}

	return enviarFormulario(client, targetURL, dni, token)
}

func obtenerToken(client *http.Client, targetURL string) (string, error) {
	req, _ := http.NewRequest("GET", targetURL, nil)
	setHeaders(req)

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

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
		return "", fmt.Errorf("token no encontrado")
	}
	return token, nil
}

func enviarFormulario(client *http.Client, targetURL, dni, token string) (string, error) {
	data := url.Values{"_token": {token}, "dniveri": {dni}}

	req, _ := http.NewRequest("POST", targetURL, strings.NewReader(data.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Referer", targetURL)
	setHeaders(req)

	time.Sleep(1 * time.Second)

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("error servidor: %d", resp.StatusCode)
	}

	body, err := leerRespuesta(resp)
	if err != nil {
		return "", err
	}

	doc, _ := goquery.NewDocumentFromReader(strings.NewReader(string(body)))
	codigo := extraerCodigo(doc, string(body))
	if codigo == "" {
		return "", fmt.Errorf("código no encontrado")
	}
	return codigo, nil
}

func extraerCodigo(doc *goquery.Document, html string) string {
	// Buscar en <mark>
	if codigo := strings.TrimSpace(doc.Find("mark").Text()); esDigito(codigo) {
		return codigo
	}

	// Buscar en input
	if codigo, exists := doc.Find("#digito_verificador").Attr("value"); exists && esDigito(codigo) {
		return codigo
	}

	// Buscar con regex
	patterns := []string{
		`(\d)\s*es\s*el\s*d[íi]gito\s*verificador`,
		`d[íi]gito\s*verificador\s*(?:es\s*)?(\d)`,
		`verificador\s*:\s*(\d)`,
	}

	for _, pattern := range patterns {
		re := regexp.MustCompile(`(?i)` + pattern)
		if matches := re.FindStringSubmatch(html); len(matches) > 1 && esDigito(matches[1]) {
			return matches[1]
		}
	}

	return ""
}

func esDigito(s string) bool {
	return regexp.MustCompile(`^\d$`).MatchString(s)
}

func setHeaders(req *http.Request) {
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "es-ES,es;q=0.9")
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

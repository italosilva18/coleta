package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"io/ioutil"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Config representa a estrutura do arquivo de configuração
type Config struct {
	SQLDBConnection   string            `json:"sqlDBConnection"`
	MongoDBConnection string            `json:"mongoDBConnection"`
	Queries           map[string]string `json:"queries"`
}

func main() {
	// Carrega as configurações do arquivo JSON
	config, err := loadConfig("coleta/config.json")
	if err != nil {
		log.Fatal("Erro ao carregar as configurações:", err)
	}

	// Conecta ao banco MySQL
	sqlDB, err := connectToMySQL(config.SQLDBConnection)
	if err != nil {
		log.Fatal("Erro ao conectar ao MySQL:", err)
	}
	defer sqlDB.Close()

	// Executa consulta SQL
	result, err := executeQuerySQL(sqlDB, config.Queries["consulta1"])
	if err != nil {
		log.Fatal("Erro ao executar consulta SQL:", err)
	}

	// Conecta ao banco MongoDB
	mongoClient, err := connectToMongoDB(config.MongoDBConnection)
	if err != nil {
		log.Fatal("Erro ao conectar ao banco MongoDB:", err)
	}
	defer mongoClient.Disconnect(context.Background())

	// Envia dados para o MongoDB
	err = sendDataToMongoDB(mongoClient, result)
	if err != nil {
		log.Fatal("Erro ao enviar dados para o MongoDB:", err)
	}

	log.Println("Dados enviados para o MongoDB com sucesso")
}

func connectToMySQL(connection string) (*sql.DB, error) {
	db, err := sql.Open("mysql", connection)
	if err != nil {
		return nil, err
	}

	// Verifica a conexão
	err = db.Ping()
	if err != nil {
		return nil, err
	}

	log.Println("Conectado ao banco local (MySQL) com sucesso")

	return db, nil
}

func executeQuerySQL(db *sql.DB, query string) ([]interface{}, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var result []interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
		}

		err := rows.Scan(values...)
		if err != nil {
			return nil, err
		}

		rowData := make(map[string]interface{})
		for i, col := range columns {
			rowData[col] = *(values[i].(*interface{}))
		}

		result = append(result, rowData)
	}

	log.Printf("Consulta SQL executada com sucesso: %s", query)

	return result, nil
}

func connectToMongoDB(connection string) (*mongo.Client, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI(connection))
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = client.Connect(ctx)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func sendDataToMongoDB(client *mongo.Client, data []interface{}) error {
	collection := client.Database("Suporte").Collection("LOJAS")

	for _, d := range data {
		_, err := collection.InsertOne(context.Background(), d)
		if err != nil {
			return err
		}
	}

	return nil
}

func loadConfig(filePath string) (Config, error) {
	var config Config

	content, err := ioutil.ReadFile(filePath)
	if err != nil {
		return config, err
	}

	err = json.Unmarshal(content, &config)
	if err != nil {
		return config, err
	}

	return config, nil
}

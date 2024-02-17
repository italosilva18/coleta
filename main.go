package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
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
		log.Fatalf("Erro ao carregar as configurações: %v", err)
	}

	// Conecta ao banco MySQL
	sqlDB, err := connectToMySQL(config.SQLDBConnection)
	if err != nil {
		log.Fatalf("Erro ao conectar ao MySQL: %v", err)
	}
	defer sqlDB.Close()

	// Executa consulta SQL
	result, err := executeQuerySQL(sqlDB, config.Queries["consulta1"])
	if err != nil {
		log.Fatalf("Erro ao executar consulta SQL: %v", err)
	}

	// Conecta ao banco MongoDB
	mongoClient, err := connectToMongoDB(config.MongoDBConnection)
	if err != nil {
		log.Fatalf("Erro ao conectar ao banco MongoDB: %v", err)
	}
	defer mongoClient.Disconnect(context.Background())

	// Envia dados para o MongoDB
	err = sendDataToMongoDB(mongoClient, result)
	if err != nil {
		log.Fatalf("Erro ao enviar dados para o MongoDB: %v", err)
	}

	log.Println("Dados enviados para o MongoDB com sucesso")
}

func connectToMySQL(connection string) (*sql.DB, error) {
	db, err := sql.Open("mysql", connection)
	if err != nil {
		return nil, fmt.Errorf("Erro ao conectar ao MySQL: %v", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("Erro ao realizar ping no MySQL: %v", err)
	}

	log.Println("Conectado ao banco local (MySQL) com sucesso")

	return db, nil
}

func executeQuerySQL(db *sql.DB, query string) ([]map[string]interface{}, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("Erro ao executar consulta SQL: %v", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("Erro ao obter colunas: %v", err)
	}

	var result []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
		}

		if err := rows.Scan(values...); err != nil {
			return nil, fmt.Errorf("Erro ao fazer scan da linha: %v", err)
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
		return nil, fmt.Errorf("Erro ao criar cliente MongoDB: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		return nil, fmt.Errorf("Erro ao conectar ao MongoDB: %v", err)
	}

	return client, nil
}
func sendDataToMongoDB(client *mongo.Client, data []map[string]interface{}) error {
	collection := client.Database("Suporte").Collection("LOJAS")

	for _, d := range data {
		convertedData := make(map[string]interface{})
		for key, value := range d {
			switch key {
			case "meio_pagto", "descricao":
				if v, ok := value.([]uint8); ok {
					convertedData[key] = string(v)
				} else {
					convertedData[key] = value
				}
			case "qtd", "valor":
				if v, ok := value.([]uint8); ok {
					convertedData[key] = string(v)
				} else {
					convertedData[key] = value
				}
			}
		}

		_, err := collection.InsertOne(context.Background(), convertedData)
		if err != nil {
			return err
		}
	}

	return nil
}

func convertBytesToString(value interface{}) interface{} {
	switch v := value.(type) {
	case []byte:
		return string(v)
	case []interface{}:
		// Se for um slice de interfaces, aplica a conversão recursivamente
		var result []interface{}
		for _, item := range v {
			result = append(result, convertBytesToString(item))
		}
		return result
	default:
		return value
	}
}

func loadConfig(filePath string) (Config, error) {
	var config Config

	content, err := ioutil.ReadFile(filePath)
	if err != nil {
		return config, fmt.Errorf("Erro ao ler arquivo de configuração: %v", err)
	}

	if err := json.Unmarshal(content, &config); err != nil {
		return config, fmt.Errorf("Erro ao fazer unmarshal do JSON: %v", err)
	}

	return config, nil
}

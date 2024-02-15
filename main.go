package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/nakagami/firebirdsql"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Config struct {
	Firebird struct {
		User     string `json:"user"`
		Password string `json:"password"`
		DBName   string `json:"dbname"`
	} `json:"firebird"`
}

type Queries struct {
	GetTables         string `json:"getTables"`
	TOTAL_VENDIDO_DIA string `json:"TOTAL_VENDIDO_DIA"`
	TICKET_MEDIO_DIA  string `json:"TICKET_MEDIO_DIA"`
	// ... outras consultas ...
}

func main() {
	// Carregar configuração do JSON
	config, err := loadConfig("config.json")
	if err != nil {
		log.Fatal("Erro ao carregar a configuração:", err)
	}

	// Carregar consultas do JSON
	queries, err := loadQueries("queries.json")
	if err != nil {
		log.Fatal("Erro ao carregar as consultas:", err)
	}

	// Conexão com o Firebird
	firebirdDSN := fmt.Sprintf("user=%s password=%s dbname=%s", config.Firebird.User, config.Firebird.Password, config.Firebird.DBName)
	firebirdDB, err := sql.Open("firebirdsql", firebirdDSN)
	if err != nil {
		log.Fatal("Erro ao conectar ao Firebird:", err)
	}
	defer firebirdDB.Close()

	// Conexão com o MongoDB
	mongoURI := "mongodb+srv://suporte:Italo2013@suporte.ifkalhd.mongodb.net/?retryWrites=true&w=majority"
	mongoDBName := "Suporte"
	mongoCollectionName := "LOJAS"

	mongoClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatal("Erro ao conectar ao MongoDB:", err)
	}
	defer mongoClient.Disconnect(context.Background())

	mongoDB := mongoClient.Database(mongoDBName)

	// Executar consultas e inserir os dados no MongoDB
	insertDataIntoMongoDB(firebirdDB, queries, mongoDB, mongoCollectionName)

}

func loadConfig(filename string) (*Config, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	var config Config
	err = decoder.Decode(&config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

func loadQueries(filename string) (*Queries, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	var queries Queries
	err = decoder.Decode(&queries)
	if err != nil {
		return nil, err
	}

	return &queries, nil
}

func insertDataIntoMongoDB(firebirdDB *sql.DB, queries *Queries, mongoDB *mongo.Database, collectionName string) {
	// Exemplo de como executar uma consulta e inserir dados no MongoDB
	// Substitua este trecho pelo seu código para executar todas as consultas e processar os resultados

	// Exemplo: Consulta TOTAL_VENDIDO_DIA
	rows, err := firebirdDB.Query(queries.TOTAL_VENDIDO_DIA, time.Now(), time.Now())
	if err != nil {
		log.Fatal("Erro ao executar consulta TOTAL_VENDIDO_DIA:", err)
	}
	defer rows.Close()

	// Exemplo: Processar os resultados e inserir no MongoDB
	for rows.Next() {
		var totalVendas float64
		var totalCustos float64
		var totalMargem float64
		err := rows.Scan(&totalVendas, &totalCustos, &totalMargem)
		if err != nil {
			log.Println("Erro ao processar resultado da consulta:", err)
			continue
		}

		data := map[string]interface{}{
			"total_vendas": totalVendas,
			"total_custos": totalCustos,
			"total_margem": totalMargem,
			"timestamp":    time.Now(),
		}

		collection := mongoDB.Collection(collectionName)
		_, err = collection.InsertOne(context.Background(), data)
		if err != nil {
			log.Println("Erro ao inserir dados no MongoDB:", err)
		}
	}
}
